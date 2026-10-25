// Storage implementation: WalRecord parsing on top of the shared frames,
// durable appends, snapshot + rotation, recovery. See wal.h for the
// design. (CRC-32C, frames and the torn-tail rule moved to
// common/durable_io.cpp in m4b, unchanged.)

#include "wal.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace fleet {
namespace fs = std::filesystem;

ParsedLog parse_log(std::string_view bytes) {
  ParsedLog r;
  const FrameScan scan = split_frames(bytes);
  for (size_t i = 0; i < scan.payloads.size(); ++i) {
    v1::WalRecord rec;
    if (!rec.ParseFromArray(scan.payloads[i].data(), static_cast<int>(scan.payloads[i].size()))) {
      // The checksum matched, so these are the bytes that were written:
      // not a torn write but a record this build cannot read. Never skip
      // it, even at the end.
      r.records.clear();
      r.error = "record at offset " + std::to_string(scan.offsets[i]) +
                " has a valid checksum but does not parse";
      return r;
    }
    r.records.push_back(std::move(rec));
  }
  r.valid_bytes = scan.valid_bytes;
  r.torn_bytes = scan.torn_bytes;
  r.error = scan.error;
  return r;
}

Storage::Storage(fs::path dir, StorageOptions options)
    : dir_(std::move(dir)), options_(options) {}

Storage::~Storage() {
  if (log_fd_ >= 0) ::close(log_fd_);
}

std::unique_ptr<Storage> Storage::open(const fs::path& dir, StorageOptions options,
                                       MasterState* state, RecoveryInfo* info,
                                       std::string* error) {
  const auto t0 = std::chrono::steady_clock::now();
  std::error_code ec;
  fs::create_directories(dir, ec);
  if (ec) {
    *error = "create " + dir.string() + ": " + ec.message();
    return nullptr;
  }
  // Leftovers of a snapshot or rotation that crashed half way. Neither is
  // ever read: until its rename, a temp file is not part of the state.
  fs::remove(dir / "snapshot.tmp", ec);
  fs::remove(dir / "wal.next", ec);

  std::unique_ptr<Storage> s(new Storage(dir, options));
  RecoveryInfo ri;

  std::string bytes;
  bool exists = false;
  if (!read_file(snapshot_path(dir), &bytes, &exists, error)) return nullptr;
  if (exists) {
    // The snapshot is renamed into place only once complete and synced,
    // so it is never torn: any problem with it is corruption.
    std::string_view payload;
    v1::Snapshot snap;
    if (!unframe_single(bytes, &payload) ||
        !snap.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
      *error = "snapshot " + snapshot_path(dir).string() + " is corrupt";
      return nullptr;
    }
    auto restored = MasterState::from_snapshot(snap, error);
    if (!restored) return nullptr;
    *state = std::move(*restored);
    ri.had_snapshot = true;
    ri.snapshot_lsn = snap.lsn();
  }

  if (!read_file(log_path(dir), &bytes, &exists, error)) return nullptr;
  ParsedLog log = parse_log(bytes);
  if (!log.error.empty()) {
    *error = log_path(dir).string() + ": " + log.error;
    return nullptr;
  }
  for (const v1::WalRecord& rec : log.records) {
    // Records the snapshot already contains: the crash came after the
    // snapshot's rename and before the log was rotated.
    if (rec.lsn() <= state->lsn()) {
      ++ri.skipped;
      continue;
    }
    if (std::string e = state->apply(rec); !e.empty()) {
      *error = "replay: " + e;
      return nullptr;
    }
    ++ri.replayed;
  }
  ri.torn_bytes = log.torn_bytes;
  ri.lsn = state->lsn();

  if (log.torn_bytes > 0) {
    // Cut the torn tail off before appending, or the next record would
    // sit behind garbage and be unreadable. The tail was never synced, so
    // no reply depended on it.
    const int fd = ::open(log_path(dir).c_str(), O_WRONLY | O_CLOEXEC);
    if (fd < 0 || ::ftruncate(fd, static_cast<off_t>(log.valid_bytes)) != 0 ||
        !sync_fd(fd, options.sync)) {
      *error = errno_text("truncate torn tail of " + log_path(dir).string());
      if (fd >= 0) ::close(fd);
      return nullptr;
    }
    ::close(fd);
  }
  if (!s->open_log_for_append(error)) return nullptr;
  // Count what is in the log now toward the next snapshot, so a master
  // that keeps crashing still rotates.
  s->stats_.records_since_snapshot = ri.replayed;
  ri.load_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0)
                   .count();
  if (info) *info = ri;
  return s;
}

bool Storage::open_log_for_append(std::string* error) {
  std::error_code ec;
  const bool existed = fs::exists(log_path(dir_), ec);
  const int fd = ::open(log_path(dir_).c_str(), O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
  if (fd < 0) {
    *error = errno_text("open " + log_path(dir_).string());
    return false;
  }
  if (log_fd_ >= 0) ::close(log_fd_);
  log_fd_ = fd;
  // A new file's directory entry must be durable before records in it
  // count as durable.
  return existed || sync_dir(dir_, options_.sync, error);
}

bool Storage::write_all(int fd, std::string_view data, std::string* error) {
  return fleet::write_all(fd, data, log_path(dir_).string(), error);
}

bool Storage::timed_sync(int fd, std::string* error) {
  const auto t0 = std::chrono::steady_clock::now();
  const bool ok = sync_fd(fd, options_.sync);
  stats_.sync_ms +=
      std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
  ++stats_.syncs;
  if (!ok) *error = errno_text("sync " + log_path(dir_).string());
  return ok;
}

bool Storage::append(const std::vector<v1::WalRecord>& records, std::string* error) {
  if (records.empty()) return true;
  std::string buf;
  std::string payload;
  for (const v1::WalRecord& r : records) {
    payload.clear();
    r.SerializeToString(&payload);
    if (options_.policy == SyncPolicy::kPerRecord) {
      // The comparison case: every record pays its own write and sync.
      buf.clear();
      append_frame(&buf, payload);
      if (!write_all(log_fd_, buf, error) || !timed_sync(log_fd_, error)) return false;
      stats_.bytes += buf.size();
    } else {
      append_frame(&buf, payload);
    }
  }
  if (options_.policy == SyncPolicy::kGroup) {
    // One write, one sync for the whole batch: group commit.
    if (!write_all(log_fd_, buf, error) || !timed_sync(log_fd_, error)) return false;
    stats_.bytes += buf.size();
  }
  ++stats_.appends;
  stats_.records += records.size();
  stats_.records_since_snapshot += records.size();
  return true;
}

bool Storage::write_snapshot(const MasterState& state, std::string* error) {
  v1::Snapshot snap;
  state.to_snapshot(&snap);
  std::string payload;
  snap.SerializeToString(&payload);
  std::string frame;
  append_frame(&frame, payload);

  // 1. snapshot.tmp, complete and synced.
  const fs::path tmp = dir_ / "snapshot.tmp";
  int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    *error = errno_text("open " + tmp.string());
    return false;
  }
  bool ok = write_all(fd, frame, error);
  if (ok && !sync_fd(fd, options_.sync)) {
    *error = errno_text("sync " + tmp.string());
    ok = false;
  }
  ::close(fd);
  if (!ok) return false;
  // 2. Atomically replace the old snapshot, 3. and make the rename durable.
  if (::rename(tmp.c_str(), snapshot_path(dir_).c_str()) != 0) {
    *error = errno_text("rename " + tmp.string());
    return false;
  }
  if (!sync_dir(dir_, options_.sync, error)) return false;

  // 4. Rotate: an empty, synced wal.next replaces the log. Until this
  // rename is durable, recovery sees the old log and skips its records.
  const fs::path next = dir_ / "wal.next";
  fd = ::open(next.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0 || !sync_fd(fd, options_.sync)) {
    *error = errno_text("create " + next.string());
    if (fd >= 0) ::close(fd);
    return false;
  }
  ::close(fd);
  if (::rename(next.c_str(), log_path(dir_).c_str()) != 0) {
    *error = errno_text("rename " + next.string());
    return false;
  }
  if (!sync_dir(dir_, options_.sync, error)) return false;
  // The open fd still points at the old, now unlinked log; switch.
  if (!open_log_for_append(error)) return false;
  ++stats_.snapshots;
  stats_.records_since_snapshot = 0;
  return true;
}

}  // namespace fleet
