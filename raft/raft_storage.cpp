// RaftStorage implementation: load and repair at open, then hard state,
// snapshot, truncate and append, each made durable. See raft_storage.h.

#include "raft_storage.h"

#include <cerrno>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>

namespace fleet::raft {
namespace fs = std::filesystem;

namespace {

std::string frame_of(const google::protobuf::Message& m) {
  std::string out;
  append_frame(&out, m.SerializeAsString());
  return out;
}

// Reads a file written by write_file_atomic: missing is fine (*exists
// false), anything but one valid frame that parses is corruption.
bool read_single(const fs::path& p, google::protobuf::Message* m, bool* exists,
                 std::string* error) {
  std::string bytes;
  if (!read_file(p, &bytes, exists, error)) return false;
  if (!*exists) return true;
  std::string_view payload;
  if (!unframe_single(bytes, &payload) ||
      !m->ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
    *error = p.string() + " is corrupt";
    return false;
  }
  return true;
}

}  // namespace

RaftStorage::RaftStorage(fs::path dir, RaftStorageOptions options)
    : dir_(std::move(dir)), options_(options) {}

RaftStorage::~RaftStorage() {
  if (log_fd_ >= 0) ::close(log_fd_);
}

std::unique_ptr<RaftStorage> RaftStorage::open(const fs::path& dir, RaftStorageOptions options,
                                               PersistentState* state, RaftRecoveryInfo* info,
                                               std::string* error) {
  const auto t0 = std::chrono::steady_clock::now();
  std::error_code ec;
  fs::create_directories(dir, ec);
  if (ec) {
    *error = "create " + dir.string() + ": " + ec.message();
    return nullptr;
  }
  // Half-written replacements from a crash; until renamed they are not
  // part of the state, so they are never read.
  for (const fs::path& p : {state_path(dir), snapshot_path(dir), log_path(dir)}) {
    fs::remove(p.string() + ".tmp", ec);
  }
  std::unique_ptr<RaftStorage> s(new RaftStorage(dir, options));
  RaftRecoveryInfo ri;
  *state = PersistentState{};

  bool exists = false;
  v1::HardState hs;
  if (!read_single(state_path(dir), &hs, &exists, error)) return nullptr;
  state->current_term = hs.current_term();
  state->voted_for = hs.voted_for();

  v1::SnapshotFile snap;
  if (!read_single(snapshot_path(dir), &snap, &exists, error)) return nullptr;
  if (exists) {
    state->snapshot.index = snap.last_included_index();
    state->snapshot.term = snap.last_included_term();
    state->snapshot.data = std::make_shared<const std::string>(std::move(*snap.mutable_data()));
  }

  std::string bytes;
  if (!read_file(log_path(dir), &bytes, &exists, error)) return nullptr;
  const FrameScan scan = split_frames(bytes);
  if (!scan.error.empty()) {
    *error = log_path(dir).string() + ": " + scan.error;
    return nullptr;
  }
  std::vector<v1::Entry> all;
  all.reserve(scan.payloads.size());
  for (size_t i = 0; i < scan.payloads.size(); ++i) {
    v1::Entry e;
    if (!e.ParseFromArray(scan.payloads[i].data(), static_cast<int>(scan.payloads[i].size()))) {
      *error = log_path(dir).string() + ": entry at offset " + std::to_string(scan.offsets[i]) +
               " has a valid checksum but does not parse";
      return nullptr;
    }
    // Consecutive indices and terms that never decrease: what the writer
    // always produces. Anything else is damage.
    if (!all.empty() && (e.index() != all.back().index() + 1 || e.term() < all.back().term())) {
      *error = log_path(dir).string() + ": entry " + std::to_string(e.index()) +
               " does not follow entry " + std::to_string(all.back().index());
      return nullptr;
    }
    all.push_back(std::move(e));
  }
  ri.torn_bytes = scan.torn_bytes;

  // Reconcile with the snapshot (see RECOVERY in the header).
  const uint64_t si = state->snapshot.index;
  bool conflict = false;
  for (const v1::Entry& e : all) {
    if (e.index() == si && e.term() != state->snapshot.term) conflict = true;
  }
  for (v1::Entry& e : all) {
    if (e.index() <= si || conflict) {
      ++ri.dropped;
      continue;
    }
    state->entries.push_back(std::move(e));
  }
  if (!state->entries.empty() && state->entries.front().index() != si + 1) {
    *error = log_path(dir).string() + ": log starts at entry " +
             std::to_string(state->entries.front().index()) + " but the snapshot ends at " +
             std::to_string(si);
    return nullptr;
  }

  if (ri.dropped > 0 || ri.torn_bytes > 0) {
    // Rewrite rather than edit in place: the result is exactly the kept
    // entries, atomically, whatever was wrong with the old file.
    s->first_index_ = si + 1;
    if (!s->rewrite_log(state->entries, error)) return nullptr;
  } else {
    s->first_index_ = all.empty() ? si + 1 : state->entries.front().index();
    s->offsets_.assign(scan.offsets.begin(), scan.offsets.end());
    s->log_size_ = bytes.size();
    if (!s->open_log_for_append(error)) return nullptr;
  }
  ri.term = state->current_term;
  ri.voted_for = state->voted_for;
  ri.snapshot_index = si;
  ri.entries = state->entries.size();
  ri.load_ms =
      std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
  if (info) *info = ri;
  return s;
}

bool RaftStorage::open_log_for_append(std::string* error) {
  std::error_code ec;
  const bool existed = fs::exists(log_path(dir_), ec);
  const int fd = ::open(log_path(dir_).c_str(), O_WRONLY | O_APPEND | O_CREAT | O_CLOEXEC, 0644);
  if (fd < 0) {
    *error = errno_text("open " + log_path(dir_).string());
    return false;
  }
  if (log_fd_ >= 0) ::close(log_fd_);
  log_fd_ = fd;
  // A new file's directory entry must be durable before entries in it
  // count as durable.
  return existed || sync_dir(dir_, options_.sync, error);
}

bool RaftStorage::write_hard_state(const v1::HardState& hs, std::string* error) {
  ++stats_.hard_state_writes;
  return write_file_atomic(state_path(dir_), frame_of(hs), options_.sync, error);
}

bool RaftStorage::write_snapshot(const SnapshotRef& snap, std::string* error) {
  v1::SnapshotFile f;
  f.set_last_included_index(snap.index);
  f.set_last_included_term(snap.term);
  if (snap.data) f.set_data(*snap.data);
  ++stats_.snapshots_written;
  return write_file_atomic(snapshot_path(dir_), frame_of(f), options_.sync, error);
}

bool RaftStorage::rewrite_log(const std::vector<v1::Entry>& entries, std::string* error) {
  std::string buf;
  std::vector<uint64_t> offsets;
  offsets.reserve(entries.size());
  for (const v1::Entry& e : entries) {
    offsets.push_back(buf.size());
    append_frame(&buf, e.SerializeAsString());
  }
  if (!write_file_atomic(log_path(dir_), buf, options_.sync, error)) return false;
  offsets_ = std::move(offsets);
  log_size_ = buf.size();
  // The caller's entries start right after the snapshot; with none, the
  // next append will be first_index_.
  first_index_ = entries.empty() ? first_index_ : entries.front().index();
  // The open fd (if any) points at the replaced file; switch.
  return open_log_for_append(error);
}

bool RaftStorage::persist(const Ready& ready, std::string* error) {
  // Hard state first: a vote must be durable before the response that
  // grants it, and a new term before any message of that term.
  if (ready.hard_state && !write_hard_state(*ready.hard_state, error)) return false;
  if (ready.snapshot) {
    if (!write_snapshot(*ready.snapshot, error)) return false;
    // The log now starts after the snapshot.
    first_index_ = ready.snapshot->index + 1;
    return rewrite_log(ready.entries, error);
  }
  if (!ready.truncate_from && ready.entries.empty()) return true;
  bool changed = false;
  if (ready.truncate_from) {
    const uint64_t t = *ready.truncate_from;
    const uint64_t persisted_end = first_index_ + offsets_.size();  // one past the last
    if (t < first_index_) {
      *error = "raft log truncation at " + std::to_string(t) + " is before the log start " +
               std::to_string(first_index_);
      return false;
    }
    if (t < persisted_end) {
      const uint64_t off = offsets_[t - first_index_];
      if (::ftruncate(log_fd_, static_cast<off_t>(off)) != 0) {
        *error = errno_text("truncate " + log_path(dir_).string());
        return false;
      }
      offsets_.resize(t - first_index_);
      log_size_ = off;
      ++stats_.truncations;
      changed = true;
    }
  }
  if (!ready.entries.empty()) {
    if (ready.entries.front().index() != first_index_ + offsets_.size()) {
      *error = "raft log append of entry " + std::to_string(ready.entries.front().index()) +
               " does not continue the log at " + std::to_string(first_index_ + offsets_.size());
      return false;
    }
    std::string buf;
    for (const v1::Entry& e : ready.entries) {
      offsets_.push_back(log_size_ + buf.size());
      append_frame(&buf, e.SerializeAsString());
    }
    if (!write_all(log_fd_, buf, log_path(dir_).string(), error)) return false;
    log_size_ += buf.size();
    stats_.entries_written += ready.entries.size();
    changed = true;
  }
  if (!changed) return true;
  // One sync for the truncation and every appended entry (group commit).
  const auto t0 = std::chrono::steady_clock::now();
  const bool ok = sync_fd(log_fd_, options_.sync);
  stats_.log_sync_ms +=
      std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
  ++stats_.log_syncs;
  if (!ok) *error = errno_text("sync " + log_path(dir_).string());
  return ok;
}

}  // namespace fleet::raft
