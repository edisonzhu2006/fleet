// Tests for RaftStorage (m4b): Raft's term, vote, log and snapshot files.
// Round trips through a reopen (which is what a crash looks like to the
// files), conflict truncation, snapshot plus log rewrite, every crash
// point between the snapshot write and the log rewrite (for compaction
// and for a snapshot installed from a leader), torn tails, corruption,
// and one sync per Ready.

#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>
#include <unistd.h>

#include "raft_storage.h"

namespace fleet::raft {
namespace {

namespace fs = std::filesystem;

v1::Entry entry(uint64_t term, uint64_t index, const std::string& cmd = "x") {
  v1::Entry e;
  e.set_term(term);
  e.set_index(index);
  e.set_type(v1::Entry::TYPE_COMMAND);
  e.set_command(cmd);
  return e;
}

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

class RaftStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::path("/private/tmp/fleet") /
           ("rs-" + std::to_string(::getpid()) + "-" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }

  std::unique_ptr<RaftStorage> open(PersistentState* st, RaftRecoveryInfo* info = nullptr,
                                    SyncMode mode = SyncMode::kNone) {
    std::string err;
    auto s = RaftStorage::open(dir_, RaftStorageOptions{mode}, st, info, &err);
    EXPECT_TRUE(s) << err;
    return s;
  }
  std::string open_error() {
    PersistentState st;
    std::string err;
    auto s = RaftStorage::open(dir_, RaftStorageOptions{SyncMode::kNone}, &st, nullptr, &err);
    EXPECT_FALSE(s);
    return err;
  }
  static void persist(RaftStorage& s, const Ready& r) {
    std::string err;
    ASSERT_TRUE(s.persist(r, &err)) << err;
  }
  static Ready append(std::vector<v1::Entry> entries, std::optional<uint64_t> truncate = {}) {
    Ready r;
    r.truncate_from = truncate ? truncate : std::optional<uint64_t>(entries.front().index());
    r.entries = std::move(entries);
    return r;
  }
  static std::vector<uint64_t> indices(const PersistentState& st) {
    std::vector<uint64_t> out;
    for (const auto& e : st.entries) out.push_back(e.index());
    return out;
  }
  fs::path dir_;
};

TEST_F(RaftStorageTest, AFreshDirectoryIsAnEmptyServer) {
  PersistentState st;
  ASSERT_TRUE(open(&st));
  EXPECT_EQ(st.current_term, 0u);
  EXPECT_EQ(st.voted_for, 0u);
  EXPECT_EQ(st.snapshot.index, 0u);
  EXPECT_TRUE(st.entries.empty());
}

TEST_F(RaftStorageTest, TermVoteAndEntriesSurviveAReopen) {
  {
    PersistentState st;
    auto s = open(&st);
    Ready r = append({entry(1, 1, "a"), entry(1, 2, "b")});
    v1::HardState hs;
    hs.set_current_term(3);
    hs.set_voted_for(2);
    r.hard_state = hs;
    persist(*s, r);
    persist(*s, append({entry(3, 3, "c")}));
  }
  PersistentState st;
  ASSERT_TRUE(open(&st));
  EXPECT_EQ(st.current_term, 3u);
  EXPECT_EQ(st.voted_for, 2u);
  ASSERT_EQ(indices(st), (std::vector<uint64_t>{1, 2, 3}));
  EXPECT_EQ(st.entries[2].command(), "c");
}

TEST_F(RaftStorageTest, AConflictTruncatesThenAppends) {
  {
    PersistentState st;
    auto s = open(&st);
    persist(*s, append({entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4), entry(1, 5)}));
    persist(*s, append({entry(2, 3, "new3"), entry(2, 4, "new4")}, 3));
    EXPECT_EQ(s->stats().truncations, 1u);
    persist(*s, append({entry(2, 5, "new5")}));  // appending after a truncation
  }
  PersistentState st;
  ASSERT_TRUE(open(&st));
  ASSERT_EQ(indices(st), (std::vector<uint64_t>{1, 2, 3, 4, 5}));
  EXPECT_EQ(st.entries[2].command(), "new3");
  EXPECT_EQ(st.entries[4].command(), "new5");
  EXPECT_EQ(st.entries[4].term(), 2u);
}

TEST_F(RaftStorageTest, ASnapshotReplacesTheLogPrefix) {
  {
    PersistentState st;
    auto s = open(&st);
    persist(*s, append({entry(1, 1), entry(1, 2), entry(1, 3), entry(2, 4), entry(2, 5)}));
    Ready r;
    r.snapshot = SnapshotRef{3, 1, std::make_shared<const std::string>("state@3")};
    r.entries = {entry(2, 4), entry(2, 5)};
    persist(*s, r);
    persist(*s, append({entry(2, 6)}));
  }
  PersistentState st;
  RaftRecoveryInfo info;
  ASSERT_TRUE(open(&st, &info));
  EXPECT_EQ(st.snapshot.index, 3u);
  EXPECT_EQ(st.snapshot.term, 1u);
  EXPECT_EQ(*st.snapshot.data, "state@3");
  ASSERT_EQ(indices(st), (std::vector<uint64_t>{4, 5, 6}));
  EXPECT_EQ(info.dropped, 0u);
}

TEST_F(RaftStorageTest, CrashAfterACompactionSnapshotBeforeTheLogRewriteDropsTheCoveredPrefix) {
  {
    PersistentState st;
    auto s = open(&st);
    persist(*s, append({entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4), entry(1, 5)}));
  }
  // The snapshot file is written (atomically), the crash comes before the
  // log is rewritten: the old log still holds 1..5.
  v1::SnapshotFile f;
  f.set_last_included_index(3);
  f.set_last_included_term(1);
  f.set_data("state@3");
  std::string frame;
  append_frame(&frame, f.SerializeAsString());
  std::string err;
  ASSERT_TRUE(write_file_atomic(RaftStorage::snapshot_path(dir_), frame, SyncMode::kNone, &err));
  PersistentState st;
  RaftRecoveryInfo info;
  ASSERT_TRUE(open(&st, &info));
  EXPECT_EQ(st.snapshot.index, 3u);
  ASSERT_EQ(indices(st), (std::vector<uint64_t>{4, 5}));
  EXPECT_EQ(info.dropped, 3u);
  // And the repaired files reopen to the same thing.
  PersistentState again;
  ASSERT_TRUE(open(&again));
  EXPECT_EQ(indices(again), (std::vector<uint64_t>{4, 5}));
}

TEST_F(RaftStorageTest, CrashAfterAnInstalledSnapshotThatConflictsDiscardsTheWholeLog) {
  {
    PersistentState st;
    auto s = open(&st);
    // A follower's log from a deposed leader of term 1.
    persist(*s, append({entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4), entry(1, 5)}));
  }
  // The leader's snapshot ends at index 4 with term 2: our entry 4 (term
  // 1) conflicts, so per Figure 13 the whole log goes, including 5.
  v1::SnapshotFile f;
  f.set_last_included_index(4);
  f.set_last_included_term(2);
  f.set_data("leader@4");
  std::string frame;
  append_frame(&frame, f.SerializeAsString());
  std::string err;
  ASSERT_TRUE(write_file_atomic(RaftStorage::snapshot_path(dir_), frame, SyncMode::kNone, &err));
  PersistentState st;
  RaftRecoveryInfo info;
  ASSERT_TRUE(open(&st, &info));
  EXPECT_EQ(st.snapshot.index, 4u);
  EXPECT_TRUE(st.entries.empty());
  EXPECT_EQ(info.dropped, 5u);
}

TEST_F(RaftStorageTest, ATornTailLosesOnlyTheLastEntryAndAppendingContinues) {
  {
    PersistentState st;
    auto s = open(&st);
    persist(*s, append({entry(1, 1), entry(1, 2), entry(1, 3, "torn")}));
  }
  const std::string bytes = read_bytes(RaftStorage::log_path(dir_));
  write_bytes(RaftStorage::log_path(dir_), bytes.substr(0, bytes.size() - 3));
  {
    PersistentState st;
    RaftRecoveryInfo info;
    auto s = open(&st, &info);
    ASSERT_TRUE(s);
    EXPECT_GT(info.torn_bytes, 0u);
    ASSERT_EQ(indices(st), (std::vector<uint64_t>{1, 2}));
    persist(*s, append({entry(1, 3, "again")}));
  }
  PersistentState st;
  ASSERT_TRUE(open(&st));
  ASSERT_EQ(indices(st), (std::vector<uint64_t>{1, 2, 3}));
  EXPECT_EQ(st.entries[2].command(), "again");
}

TEST_F(RaftStorageTest, DamageInTheMiddleIsRefused) {
  {
    PersistentState st;
    auto s = open(&st);
    persist(*s, append({entry(1, 1, "aaaa"), entry(1, 2, "bbbb"), entry(1, 3, "cccc")}));
  }
  std::string bytes = read_bytes(RaftStorage::log_path(dir_));
  const size_t at = bytes.find("aaaa");
  ASSERT_NE(at, std::string::npos);
  bytes[at] = 'z';
  write_bytes(RaftStorage::log_path(dir_), bytes);
  EXPECT_NE(open_error().find("corrupt record"), std::string::npos);
}

TEST_F(RaftStorageTest, NonConsecutiveEntriesAndCorruptStateFilesAreRefused) {
  {
    std::string log;
    append_frame(&log, entry(1, 1).SerializeAsString());
    append_frame(&log, entry(1, 3).SerializeAsString());  // a gap
    fs::create_directories(dir_);
    write_bytes(RaftStorage::log_path(dir_), log);
  }
  EXPECT_NE(open_error().find("does not follow"), std::string::npos);
  fs::remove(RaftStorage::log_path(dir_));
  write_bytes(RaftStorage::state_path(dir_), "garbage!");
  EXPECT_NE(open_error().find("corrupt"), std::string::npos);
}

TEST_F(RaftStorageTest, OneSyncPerReadyHoweverManyEntries) {
  PersistentState st;
  auto s = open(&st, nullptr, SyncMode::kFull);
  std::vector<v1::Entry> batch;
  for (uint64_t i = 1; i <= 500; ++i) batch.push_back(entry(1, i));
  persist(*s, append(batch));
  EXPECT_EQ(s->stats().log_syncs, 1u);
  EXPECT_EQ(s->stats().entries_written, 500u);
  persist(*s, Ready{});  // nothing to write: no sync
  EXPECT_EQ(s->stats().log_syncs, 1u);
}

TEST_F(RaftStorageTest, ACoreRebuiltFromTheFilesHasTheSameStateAsBeforeTheCrash) {
  // End to end with the real core: a one-server cluster commits, compacts
  // and commits more; a core rebuilt from the files matches.
  RaftOptions o;
  o.id = 1;
  o.members = {1};
  uint64_t term = 0, last = 0, snap = 0;
  {
    PersistentState st;
    auto s = open(&st);
    RaftCore c(o, st, 0);
    c.tick(1000);
    (void)c.propose({"a", "b", "c"}, 1001);
    persist(*s, c.take_ready());
    c.compact(3, std::make_shared<const std::string>("S3"));
    (void)c.propose({"d"}, 1002);
    persist(*s, c.take_ready());
    term = c.term();
    last = c.last_index();
    snap = c.snapshot_index();
  }
  PersistentState st;
  ASSERT_TRUE(open(&st));
  RaftCore c(o, st, 2000);
  EXPECT_EQ(c.term(), term);
  EXPECT_EQ(c.last_index(), last);
  EXPECT_EQ(c.snapshot_index(), snap);
  EXPECT_EQ(c.voted_for(), 1u);
  EXPECT_EQ(c.entries().back().command(), "d");
}

}  // namespace
}  // namespace fleet::raft
