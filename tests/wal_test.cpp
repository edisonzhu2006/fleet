// Tests for the write-ahead log, snapshots and recovery files (m4):
// CRC-32C, record framing round trips, the torn-tail rule at every byte
// offset, corruption in the middle of the log, snapshot round trips, the
// crash points of snapshot + rotation, and group commit vs per-record sync.
// The deterministic-replay property (replay equals live) is in
// recovery_test.cpp.

#include <gtest/gtest.h>

#include <fcntl.h>
#include <fstream>
#include <set>
#include <sstream>
#include <string>
#include <unistd.h>

#include "master_core.h"
#include "wal.h"
#include "wal_test_helpers.h"

namespace fleet {
namespace {

using namespace std::chrono_literals;
using namespace fleet::testing;
namespace fs = std::filesystem;

std::string read_bytes(const fs::path& p) {
  std::ifstream in(p, std::ios::binary);
  std::ostringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

void write_bytes(const fs::path& p, const std::string& bytes) {
  std::ofstream out(p, std::ios::binary | std::ios::trunc);
  out << bytes;
}

// The records of a busy master: every record type except TaskRequeued.
std::vector<v1::WalRecord> busy_records(MasterState* final_state = nullptr) {
  MasterCore core(durable_test_options(), 1);
  run_busy_scenario(core);
  EXPECT_CORE_OK(core);
  if (final_state) *final_state = core.durable_state();
  return core.take_records();
}

std::string frame_all(const std::vector<v1::WalRecord>& records, std::vector<size_t>* ends) {
  std::string out;
  for (const v1::WalRecord& r : records) {
    append_frame(&out, r.SerializeAsString());
    if (ends) ends->push_back(out.size());
  }
  return out;
}

StorageOptions fast_storage() {
  StorageOptions o;
  o.sync = SyncMode::kNone;  // durability itself is not under test here
  o.snapshot_every = 0;
  return o;
}

// ------------------------------------------------------------- framing

TEST(Crc32c, MatchesTheStandardCheckValueAndChains) {
  // The CRC-32C check value from the iSCSI spec (RFC 3720, B.4) and every
  // published CRC catalogue: crc("123456789") = 0xE3069283.
  const std::string s = "123456789";
  EXPECT_EQ(crc32c(s.data(), s.size()), 0xE3069283u);
  EXPECT_EQ(crc32c(s.data() + 4, 5, crc32c(s.data(), 4)), 0xE3069283u);
  EXPECT_EQ(crc32c(nullptr, 0), 0u);
  // 32 zero bytes, also from RFC 3720.
  const std::string zeros(32, '\0');
  EXPECT_EQ(crc32c(zeros.data(), zeros.size()), 0x8A9136AAu);
}

TEST(WalRecord, RoundTripsThroughFramesWithEveryOpType) {
  const std::vector<v1::WalRecord> records = busy_records();
  std::set<v1::WalRecord::OpCase> ops;
  for (const auto& r : records) ops.insert(r.op_case());
  for (auto op : {v1::WalRecord::kNodeRegistered, v1::WalRecord::kNodeDied,
                  v1::WalRecord::kNodeDraining, v1::WalRecord::kTasksSubmitted,
                  v1::WalRecord::kTaskPlaced, v1::WalRecord::kTaskPreempted,
                  v1::WalRecord::kTaskMoving, v1::WalRecord::kTaskExited,
                  v1::WalRecord::kTaskBackoffOver, v1::WalRecord::kTaskKilled}) {
    EXPECT_TRUE(ops.count(op)) << "scenario made no record of op " << op;
  }
  const std::string bytes = frame_all(records, nullptr);
  const ParsedLog parsed = parse_log(bytes);
  ASSERT_TRUE(parsed.error.empty()) << parsed.error;
  EXPECT_EQ(parsed.valid_bytes, bytes.size());
  EXPECT_EQ(parsed.torn_bytes, 0u);
  ASSERT_EQ(parsed.records.size(), records.size());
  for (size_t i = 0; i < records.size(); ++i) {
    EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(parsed.records[i], records[i]))
        << "record " << i;
    EXPECT_EQ(parsed.records[i].lsn(), i + 1);
  }
}

TEST(WalRecord, FrameLayoutIsLengthThenChecksumThenPayload) {
  const std::string payload = "abc";
  std::string f;
  append_frame(&f, payload);
  ASSERT_EQ(f.size(), 8u + 3u);
  EXPECT_EQ(f.substr(0, 4), std::string("\x03\x00\x00\x00", 4));  // little-endian length
  const uint32_t crc = crc32c("abc", 3, crc32c(f.data(), 4));
  for (int i = 0; i < 4; ++i) {
    EXPECT_EQ(static_cast<unsigned char>(f[4 + i]), (crc >> (8 * i)) & 0xFF);
  }
  EXPECT_EQ(f.substr(8), payload);
}

TEST(Storage, AppendedRecordsComeBackAfterReopen) {
  const fs::path dir = scratch_dir("wal");
  MasterState live;
  const std::vector<v1::WalRecord> records = busy_records(&live);
  {
    MasterState empty;
    std::string err;
    auto st = Storage::open(dir, fast_storage(), &empty, nullptr, &err);
    ASSERT_TRUE(st) << err;
    // Three uneven batches, as three MasterServer calls would commit them.
    const size_t a = records.size() / 3, b = 2 * records.size() / 3;
    ASSERT_TRUE(st->append({records.begin(), records.begin() + a}, &err)) << err;
    ASSERT_TRUE(st->append({records.begin() + a, records.begin() + b}, &err)) << err;
    ASSERT_TRUE(st->append({records.begin() + b, records.end()}, &err)) << err;
    EXPECT_EQ(st->stats().records, records.size());
  }
  MasterState recovered;
  RecoveryInfo info;
  std::string err;
  auto st = Storage::open(dir, fast_storage(), &recovered, &info, &err);
  ASSERT_TRUE(st) << err;
  EXPECT_FALSE(info.had_snapshot);
  EXPECT_EQ(info.replayed, records.size());
  EXPECT_EQ(info.torn_bytes, 0u);
  EXPECT_EQ(recovered.lsn(), records.size());
  EXPECT_EQ(state_diff(recovered, live), "");
  EXPECT_EQ(recovered.check_invariants(), "");
}

// ----------------------------------------------------------- torn tail

TEST(TornTail, EveryCutInsideTheLastRecordLosesOnlyThatRecord) {
  const std::vector<v1::WalRecord> records = busy_records();
  std::vector<size_t> ends;
  const std::string bytes = frame_all(records, &ends);
  const size_t last_start = ends[ends.size() - 2];
  // Cut the file at every byte offset from the start of the last frame to
  // one byte short of its end: header torn, payload torn, all of them.
  for (size_t cut = last_start; cut < bytes.size(); ++cut) {
    const ParsedLog p = parse_log(std::string_view(bytes).substr(0, cut));
    ASSERT_TRUE(p.error.empty()) << "cut " << cut << ": " << p.error;
    EXPECT_EQ(p.records.size(), records.size() - 1) << "cut " << cut;
    EXPECT_EQ(p.valid_bytes, last_start);
    EXPECT_EQ(p.torn_bytes, cut - last_start);
  }
}

TEST(TornTail, RecoveryTruncatesItAndAppendsCleanlyAfter) {
  MasterState live;
  const std::vector<v1::WalRecord> records = busy_records(&live);
  std::vector<size_t> ends;
  const std::string bytes = frame_all(records, &ends);
  const size_t last_start = ends[ends.size() - 2];
  // A handful of cut points through the last frame, each a fresh directory.
  for (size_t cut : {last_start + 1, last_start + 7, last_start + 8, bytes.size() - 1}) {
    const fs::path dir = scratch_dir("torn");
    write_bytes(Storage::log_path(dir), bytes.substr(0, cut));
    MasterState s;
    RecoveryInfo info;
    std::string err;
    auto st = Storage::open(dir, fast_storage(), &s, &info, &err);
    ASSERT_TRUE(st) << err;
    EXPECT_EQ(info.replayed, records.size() - 1);
    EXPECT_EQ(info.torn_bytes, cut - last_start);
    EXPECT_EQ(fs::file_size(Storage::log_path(dir)), last_start) << "torn tail not truncated";
    // The lost record was never synced, so no one acted on it; the master
    // decides again and logs it again, right after the good prefix.
    ASSERT_TRUE(st->append({records.back()}, &err)) << err;
    st.reset();
    MasterState again;
    ASSERT_TRUE(Storage::open(dir, fast_storage(), &again, &info, &err)) << err;
    EXPECT_EQ(info.replayed, records.size());
    EXPECT_EQ(state_diff(again, live), "");
  }
}

TEST(TornTail, ABadChecksumAtTheEndOrZeroFillIsTorn) {
  const std::vector<v1::WalRecord> records = busy_records();
  std::vector<size_t> ends;
  std::string bytes = frame_all(records, &ends);
  const size_t last_start = ends[ends.size() - 2];
  std::string flipped = bytes;
  flipped[bytes.size() - 1] ^= 0x40;  // last payload byte of the last record
  ParsedLog p = parse_log(flipped);
  EXPECT_TRUE(p.error.empty()) << p.error;
  EXPECT_EQ(p.records.size(), records.size() - 1);
  // A crash after the file grew but before the data landed can leave
  // zeros (some file systems allocate first): torn, not corrupt.
  p = parse_log(bytes + std::string(4096, '\0'));
  EXPECT_TRUE(p.error.empty()) << p.error;
  EXPECT_EQ(p.records.size(), records.size());
  EXPECT_EQ(p.torn_bytes, 4096u);
  std::string zeroed = bytes.substr(0, last_start) + std::string(bytes.size() - last_start, '\0');
  p = parse_log(zeroed);
  EXPECT_TRUE(p.error.empty()) << p.error;
  EXPECT_EQ(p.records.size(), records.size() - 1);
}

TEST(Corruption, DamageInTheMiddleIsRefusedNotSkipped) {
  const std::vector<v1::WalRecord> records = busy_records();
  std::vector<size_t> ends;
  const std::string bytes = frame_all(records, &ends);
  std::string bad = bytes;
  bad[ends[0] + 9] ^= 0x01;  // a payload byte of the second record
  const ParsedLog p = parse_log(bad);
  EXPECT_NE(p.error.find("checksum mismatch"), std::string::npos) << p.error;
  EXPECT_EQ(p.records.size(), 1u);

  // Storage refuses to start and leaves the file as it was, for a human.
  const fs::path dir = scratch_dir("corrupt");
  write_bytes(Storage::log_path(dir), bad);
  MasterState s;
  std::string err;
  EXPECT_FALSE(Storage::open(dir, fast_storage(), &s, nullptr, &err));
  EXPECT_NE(err.find("corrupt record at offset " + std::to_string(ends[0])), std::string::npos)
      << err;
  EXPECT_EQ(read_bytes(Storage::log_path(dir)), bad);
}

TEST(Corruption, AnImpossibleLengthIsDamageEvenAtTheEnd) {
  // A torn write leaves a correct prefix, so a complete header whose
  // length the writer could never have produced is not a tear.
  const std::vector<v1::WalRecord> records = busy_records();
  std::string bytes = frame_all(records, nullptr);
  bytes += std::string("\xff\xff\xff\xff\x01\x02\x03\x04", 8);
  const ParsedLog p = parse_log(bytes);
  EXPECT_NE(p.error.find("impossible length"), std::string::npos) << p.error;
}

TEST(Corruption, AValidChecksumOverUnparsableBytesIsAnErrorNotATornTail) {
  std::string bytes;
  append_frame(&bytes, std::string("\xff\xff\xff", 3));  // not a WalRecord
  const ParsedLog p = parse_log(bytes);
  EXPECT_NE(p.error.find("does not parse"), std::string::npos) << p.error;
}

TEST(Corruption, ARecordThatDoesNotFollowTheLsnIsRefused) {
  std::vector<v1::WalRecord> records = busy_records();
  records.erase(records.begin() + 3);  // a hole in the log
  const fs::path dir = scratch_dir("gap");
  write_bytes(Storage::log_path(dir), frame_all(records, nullptr));
  MasterState s;
  std::string err;
  EXPECT_FALSE(Storage::open(dir, fast_storage(), &s, nullptr, &err));
  EXPECT_NE(err.find("does not follow 3"), std::string::npos) << err;
}

// ------------------------------------------------------------ snapshots

TEST(Snapshot, RoundTripsEveryTaskAndNodeState) {
  MasterCore core(durable_test_options(), 1);
  run_busy_scenario(core);
  const MasterState& live = core.durable_state();
  std::set<TaskState> states;
  for (const Task& t : live.cluster().tasks()) states.insert(t.state);
  for (TaskState want : {TaskState::kPending, TaskState::kRunning, TaskState::kBackoff,
                         TaskState::kSucceeded, TaskState::kFailed, TaskState::kKilled}) {
    EXPECT_TRUE(states.count(want)) << "scenario left no task " << to_string(want);
  }
  bool moving = false;
  for (TaskId t = 0; t < live.cluster().tasks().size(); ++t) moving = moving || live.task(t).moving;
  EXPECT_TRUE(moving);
  std::set<NodeState> node_states;
  for (const Node& n : live.cluster().nodes()) node_states.insert(n.state);
  EXPECT_EQ(node_states.size(), 3u);  // ready, draining, dead

  v1::Snapshot snap;
  live.to_snapshot(&snap);
  std::string err;
  auto back = MasterState::from_snapshot(snap, &err);
  ASSERT_TRUE(back) << err;
  EXPECT_EQ(state_diff(*back, live), "");
  EXPECT_EQ(back->check_invariants(), "");
  // Derived structures were rebuilt, not copied: compare a few directly.
  for (const Node& n : live.cluster().nodes()) {
    EXPECT_EQ(back->cluster().node(n.id).allocated, n.allocated);
    EXPECT_EQ(back->cluster().tasks_on(n.id), live.cluster().tasks_on(n.id));
  }
  EXPECT_EQ(back->cluster().pending(), live.cluster().pending());
  EXPECT_EQ(back->backoff(), live.backoff());
  EXPECT_EQ(back->next_epoch(), live.next_epoch());
  // And it keeps working: the same next record applies to both.
  MasterCore resumed(durable_test_options(), *back, kT0 + 10s, 1);
  EXPECT_CORE_OK(resumed);
}

TEST(Snapshot, InconsistentSnapshotsAreRefused) {
  MasterCore core(durable_test_options(), 1);
  run_busy_scenario(core);
  v1::Snapshot snap;
  core.durable_state().to_snapshot(&snap);
  std::string err;
  v1::Snapshot bad = snap;
  // A running task on a dead node.
  for (int i = 0; i < bad.tasks_size(); ++i) {
    if (bad.tasks(i).state() == v1::TASK_STATE_RUNNING) {
      bad.mutable_tasks(i)->set_node(0);  // node "a" is dead in the scenario
      break;
    }
  }
  EXPECT_FALSE(MasterState::from_snapshot(bad, &err));
  EXPECT_NE(err.find("does not fit"), std::string::npos) << err;
  bad = snap;
  bad.mutable_nodes(1)->set_epoch(0);  // a live node with a revoked epoch
  EXPECT_FALSE(MasterState::from_snapshot(bad, &err));
  bad = snap;
  bad.mutable_nodes(2)->set_name("b");  // duplicate name
  EXPECT_FALSE(MasterState::from_snapshot(bad, &err));
}

TEST(Snapshot, WriteRotateThenRecoverFromSnapshotPlusLog) {
  const fs::path dir = scratch_dir("snap");
  MasterState live;
  const std::vector<v1::WalRecord> records = busy_records(&live);
  const size_t half = records.size() / 2;
  {
    MasterState s;
    std::string err;
    auto st = Storage::open(dir, fast_storage(), &s, nullptr, &err);
    ASSERT_TRUE(st) << err;
    ASSERT_TRUE(st->append({records.begin(), records.begin() + half}, &err));
    ASSERT_TRUE(apply_all(s, {records.begin(), records.begin() + half}).empty());
    ASSERT_TRUE(st->write_snapshot(s, &err)) << err;
    EXPECT_EQ(fs::file_size(Storage::log_path(dir)), 0u) << "log not rotated";
    EXPECT_FALSE(fs::exists(dir / "snapshot.tmp"));
    ASSERT_TRUE(st->append({records.begin() + half, records.end()}, &err));
  }
  MasterState recovered;
  RecoveryInfo info;
  std::string err;
  ASSERT_TRUE(Storage::open(dir, fast_storage(), &recovered, &info, &err)) << err;
  EXPECT_TRUE(info.had_snapshot);
  EXPECT_EQ(info.snapshot_lsn, half);
  EXPECT_EQ(info.replayed, records.size() - half);
  EXPECT_EQ(info.skipped, 0u);
  EXPECT_EQ(state_diff(recovered, live), "");
}

TEST(Snapshot, CrashBetweenSnapshotAndRotationSkipsRecordsAlreadyIn) {
  // The snapshot's rename is durable but the old log was never rotated:
  // recovery must skip the log's first `half` records, not apply them twice.
  const fs::path dir = scratch_dir("snapcrash");
  MasterState live;
  const std::vector<v1::WalRecord> records = busy_records(&live);
  const size_t half = records.size() / 2;
  MasterState at_half;
  ASSERT_TRUE(apply_all(at_half, {records.begin(), records.begin() + half}).empty());
  v1::Snapshot snap;
  at_half.to_snapshot(&snap);
  std::string frame;
  append_frame(&frame, snap.SerializeAsString());
  write_bytes(Storage::snapshot_path(dir), frame);
  write_bytes(Storage::log_path(dir), frame_all(records, nullptr));
  // A leftover temp file from the crash is ignored and removed.
  write_bytes(dir / "snapshot.tmp", "half-written");
  MasterState recovered;
  RecoveryInfo info;
  std::string err;
  ASSERT_TRUE(Storage::open(dir, fast_storage(), &recovered, &info, &err)) << err;
  EXPECT_EQ(info.skipped, half);
  EXPECT_EQ(info.replayed, records.size() - half);
  EXPECT_EQ(state_diff(recovered, live), "");
  EXPECT_FALSE(fs::exists(dir / "snapshot.tmp"));
}

TEST(Snapshot, ACorruptSnapshotIsRefused) {
  const fs::path dir = scratch_dir("snapbad");
  MasterState live;
  busy_records(&live);
  v1::Snapshot snap;
  live.to_snapshot(&snap);
  std::string frame;
  append_frame(&frame, snap.SerializeAsString());
  frame[frame.size() / 2] ^= 0x10;
  write_bytes(Storage::snapshot_path(dir), frame);
  MasterState s;
  std::string err;
  EXPECT_FALSE(Storage::open(dir, fast_storage(), &s, nullptr, &err));
  EXPECT_NE(err.find("snapshot"), std::string::npos) << err;
}

TEST(Snapshot, TakenAutomaticallyEveryNRecords) {
  const fs::path dir = scratch_dir("snapauto");
  const std::vector<v1::WalRecord> records = busy_records();
  StorageOptions o = fast_storage();
  o.snapshot_every = 10;
  MasterState s;
  std::string err;
  auto st = Storage::open(dir, o, &s, nullptr, &err);
  ASSERT_TRUE(st) << err;
  // Storage only says a snapshot is due; MasterServer takes it after the
  // commit that crossed the threshold. Mimic that here, 4 records a batch.
  for (size_t i = 0; i < records.size(); i += 4) {
    std::vector<v1::WalRecord> batch(records.begin() + static_cast<long>(i),
                                     records.begin() + static_cast<long>(std::min(i + 4, records.size())));
    ASSERT_TRUE(st->append(batch, &err)) << err;
    ASSERT_TRUE(apply_all(s, batch).empty());
    if (st->snapshot_due()) ASSERT_TRUE(st->write_snapshot(s, &err)) << err;
  }
  EXPECT_EQ(st->stats().snapshots, records.size() / 12);  // every 12 records (3 batches)
  st.reset();
  MasterState recovered;
  RecoveryInfo info;
  ASSERT_TRUE(Storage::open(dir, o, &recovered, &info, &err)) << err;
  EXPECT_TRUE(info.had_snapshot);
  EXPECT_LT(info.replayed, 12u);
  EXPECT_EQ(state_diff(recovered, s), "");
}

// -------------------------------------------------------- group commit

TEST(GroupCommit, OneSyncPerBatchVersusOnePerRecord) {
  const std::vector<v1::WalRecord> records = busy_records();
  for (SyncPolicy policy : {SyncPolicy::kGroup, SyncPolicy::kPerRecord}) {
    const fs::path dir = scratch_dir(policy == SyncPolicy::kGroup ? "group" : "perrec");
    StorageOptions o;
    o.sync = SyncMode::kFsync;  // a real sync call, cheaper than F_FULLFSYNC
    o.policy = policy;
    o.snapshot_every = 0;
    MasterState s;
    std::string err;
    auto st = Storage::open(dir, o, &s, nullptr, &err);
    ASSERT_TRUE(st) << err;
    ASSERT_TRUE(st->append(records, &err)) << err;
    EXPECT_EQ(st->stats().syncs, policy == SyncPolicy::kGroup ? 1u : records.size());
    EXPECT_EQ(st->stats().bytes, fs::file_size(Storage::log_path(dir)));
    st.reset();
    MasterState back;
    ASSERT_TRUE(Storage::open(dir, o, &back, nullptr, &err)) << err;
    EXPECT_EQ(back.lsn(), records.size());
  }
}

TEST(GroupCommit, FullSyncWorksOnThisFileSystem) {
  // F_FULLFSYNC on macOS, fdatasync on Linux: the real thing, once.
  const fs::path dir = scratch_dir("fullsync");
  const int fd = ::open((dir / "f").c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  ASSERT_GE(fd, 0);
  ASSERT_EQ(::write(fd, "x", 1), 1);
  EXPECT_TRUE(sync_fd(fd, SyncMode::kFull));
  ::close(fd);
}

}  // namespace
}  // namespace fleet
