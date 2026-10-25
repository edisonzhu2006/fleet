#pragma once
// RaftStorage: Raft's persistent state on disk (m4b): currentTerm and
// votedFor, the log, and the latest snapshot, made durable before any
// message that depends on them is sent.
//
// WHY THIS EXISTS. Figure 2 marks currentTerm, votedFor and log[] as
// "updated on stable storage before responding to RPCs". Without that a
// server could vote twice in one term after a crash (two leaders), or
// acknowledge an entry, crash, forget it, and let a leader count a
// majority that no longer exists (a committed entry lost). RaftCore
// describes each change in a Ready; this class carries it out with the
// m4 durability rules (common/durable_io.h: checksummed frames, the
// torn-tail rule, F_FULLFSYNC on macOS) and rebuilds a PersistentState
// from the files after a restart.
//
// FILES, in one directory (fleetmaster --state-dir):
//   raft_state     one frame: HardState (term, vote). Replaced atomically
//                  (temp file, sync, rename, sync dir) on every change:
//                  only on elections, so the cost (two syncs) is rare.
//   raft_snapshot  one frame: SnapshotFile (last included index and term,
//                  and the state machine bytes, a fleet.v1.Snapshot).
//                  Replaced atomically.
//   raft_log       frames of Entry, consecutive indices, appended. A
//                  conflict truncates the file at the first conflicting
//                  entry's offset (ftruncate) before appending; a new
//                  snapshot rewrites it (temp file + rename) with only the
//                  entries after the snapshot.
//
// ONE SYNC PER READY for the log, however many entries it appends (group
// commit, as in m4): a scheduling pass that places 500 tasks is 500
// entries and one sync.
//
// RECOVERY. The three files are written separately, so a crash can leave
// any prefix of a Ready's writes. Every such prefix is safe, because
// nothing that depends on the Ready was sent yet, and recovery repairs the
// one inconsistent combination: a new snapshot whose log rewrite did not
// happen. Then the old log still holds entries at or below the snapshot's
// index (dropped on load), and, if the snapshot was installed from a
// leader whose log differed, entries that conflict with it: if the log's
// entry at the snapshot index has a different term, everything after it
// is discarded too (Figure 13, step 7, applied at load). A torn tail is
// cut off as in m4; damage in the middle refuses to start.
//
// NOT THREAD-SAFE: the replica calls it under its lock.

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "durable_io.h"
#include "raft_core.h"

namespace fleet::raft {

struct RaftStorageOptions {
  SyncMode sync = SyncMode::kFull;
};

struct RaftRecoveryInfo {
  uint64_t term = 0;
  uint32_t voted_for = 0;
  uint64_t snapshot_index = 0;
  size_t entries = 0;          // log entries after the snapshot
  size_t dropped = 0;          // entries discarded as covered by, or conflicting with, the snapshot
  size_t torn_bytes = 0;
  double load_ms = 0;
};

struct RaftStorageStats {
  uint64_t log_syncs = 0;
  double log_sync_ms = 0;
  uint64_t entries_written = 0;
  uint64_t truncations = 0;
  uint64_t hard_state_writes = 0;
  uint64_t snapshots_written = 0;
};

class RaftStorage {
 public:
  ~RaftStorage();
  RaftStorage(const RaftStorage&) = delete;
  RaftStorage& operator=(const RaftStorage&) = delete;

  // Opens (creating) dir and rebuilds *state from it (see RECOVERY). On
  // corruption returns nullptr with *error set and leaves the files as
  // they are.
  static std::unique_ptr<RaftStorage> open(const std::filesystem::path& dir,
                                           RaftStorageOptions options, PersistentState* state,
                                           RaftRecoveryInfo* info, std::string* error);

  // Carries out the persistence half of a Ready (hard state, then
  // snapshot and log rewrite, or truncation and appends), synced. False
  // with *error on any I/O failure: the caller must stop (see sync_fd).
  bool persist(const Ready& ready, std::string* error);

  const RaftStorageStats& stats() const { return stats_; }

  static std::filesystem::path state_path(const std::filesystem::path& d) { return d / "raft_state"; }
  static std::filesystem::path log_path(const std::filesystem::path& d) { return d / "raft_log"; }
  static std::filesystem::path snapshot_path(const std::filesystem::path& d) {
    return d / "raft_snapshot";
  }

 private:
  RaftStorage(std::filesystem::path dir, RaftStorageOptions options);
  bool write_hard_state(const v1::HardState& hs, std::string* error);
  bool write_snapshot(const SnapshotRef& snap, std::string* error);
  // Replaces the log file with exactly these entries (temp + rename) and
  // reopens it for appending.
  bool rewrite_log(const std::vector<v1::Entry>& entries, std::string* error);
  bool open_log_for_append(std::string* error);

  std::filesystem::path dir_;
  RaftStorageOptions options_;
  int log_fd_ = -1;
  // The persisted log: entry with index first_index_ + k starts at byte
  // offsets_[k]; log_size_ is the file's length. Lets a truncation find
  // its offset without rereading the file.
  uint64_t first_index_ = 1;
  std::vector<uint64_t> offsets_;
  uint64_t log_size_ = 0;
  RaftStorageStats stats_;
};

}  // namespace fleet::raft
