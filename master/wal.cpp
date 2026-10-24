// Storage implementation: CRC-32C, frames, the torn-tail rule, durable
// appends, snapshot + rotation, recovery. See wal.h for the design.

#include "wal.h"

#include <array>
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

namespace {

// 256-entry table for the reflected Castagnoli polynomial, built once at
// compile time: entry i is the CRC of the single byte i.
constexpr std::array<uint32_t, 256> make_crc_table() {
  std::array<uint32_t, 256> t{};
  for (uint32_t i = 0; i < 256; ++i) {
    uint32_t c = i;
    for (int k = 0; k < 8; ++k) c = (c & 1) ? (c >> 1) ^ 0x82F63B78u : c >> 1;
    t[i] = c;
  }
  return t;
}
constexpr std::array<uint32_t, 256> kCrcTable = make_crc_table();

// Frames longer than this are treated as invalid rather than allocated:
// a corrupted length must not make the reader try to read 4 GB. Records
// are hundreds of bytes; a snapshot is one frame of up to a few MB per
// ten thousand tasks.
constexpr uint32_t kMaxFrame = 1u << 30;
constexpr size_t kHeader = 8;

void put_u32(std::string* out, uint32_t v) {
  for (int i = 0; i < 4; ++i) out->push_back(static_cast<char>((v >> (8 * i)) & 0xFF));
}

uint32_t get_u32(const char* p) {
  uint32_t v = 0;
  for (int i = 0; i < 4; ++i) v |= static_cast<uint32_t>(static_cast<unsigned char>(p[i])) << (8 * i);
  return v;
}

bool all_zero(std::string_view s) {
  for (char c : s) {
    if (c != 0) return false;
  }
  return true;
}

std::string errno_text(const std::string& what) {
  return what + ": " + std::strerror(errno);
}

// Reads a whole file. A missing file is an empty string with *exists
// false; any other failure is an error.
bool read_file(const fs::path& p, std::string* out, bool* exists, std::string* error) {
  out->clear();
  std::error_code ec;
  *exists = fs::exists(p, ec);
  if (!*exists) return true;
  std::ifstream in(p, std::ios::binary);
  if (!in) {
    *error = errno_text("open " + p.string());
    return false;
  }
  std::ostringstream ss;
  ss << in.rdbuf();
  *out = ss.str();
  return true;
}

// rename() and file creation change the directory, not the file; they are
// durable only once the directory itself is synced.
bool sync_dir(const fs::path& dir, SyncMode mode, std::string* error) {
  if (mode == SyncMode::kNone) return true;
  const int fd = ::open(dir.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    *error = errno_text("open dir " + dir.string());
    return false;
  }
  const bool ok = sync_fd(fd, mode);
  if (!ok) *error = errno_text("sync dir " + dir.string());
  ::close(fd);
  return ok;
}

}  // namespace

uint32_t crc32c(const void* data, size_t n, uint32_t crc) {
  const auto* p = static_cast<const unsigned char*>(data);
  crc = ~crc;
  for (size_t i = 0; i < n; ++i) crc = kCrcTable[(crc ^ p[i]) & 0xFF] ^ (crc >> 8);
  return ~crc;
}

bool sync_fd(int fd, SyncMode mode) {
  switch (mode) {
    case SyncMode::kNone: return true;
    case SyncMode::kFsync: return ::fsync(fd) == 0;
    case SyncMode::kFull:
#if defined(__APPLE__)
      // F_FULLFSYNC asks the drive to flush its cache too. Some file
      // systems (network mounts, some FUSE) do not support it; there fsync
      // is the best available, so fall back rather than fail.
      if (::fcntl(fd, F_FULLFSYNC) == 0) return true;
      if (errno != ENOTSUP && errno != EINVAL && errno != ENOTTY) return false;
      return ::fsync(fd) == 0;
#else
      // On Linux fsync/fdatasync already flush the device cache.
      // fdatasync skips metadata the data does not need (mtime), which an
      // append still needs (the file size), so it writes that too.
      return ::fdatasync(fd) == 0;
#endif
  }
  return false;
}

void append_frame(std::string* out, const std::string& payload) {
  const auto len = static_cast<uint32_t>(payload.size());
  std::string len_bytes;
  put_u32(&len_bytes, len);
  const uint32_t crc = crc32c(payload.data(), payload.size(), crc32c(len_bytes.data(), 4));
  out->append(len_bytes);
  put_u32(out, crc);
  out->append(payload);
}

ParsedLog parse_log(std::string_view bytes) {
  ParsedLog r;
  size_t pos = 0;
  while (pos < bytes.size()) {
    const size_t left = bytes.size() - pos;
    // Where the frame starting at pos ends, as far as the file goes, and
    // (if it is not a complete, valid record) why not.
    size_t end = bytes.size();
    std::string why;
    if (left < kHeader) {
      why = "partial header";
    } else {
      const uint32_t len = get_u32(bytes.data() + pos);
      const uint32_t crc = get_u32(bytes.data() + pos + 4);
      if (len > kMaxFrame) {
        // The writer never produces such a length, and a torn write
        // leaves a correct prefix of what it wrote, so a complete header
        // with an impossible length is damage, not a tear.
        r.error = "record at offset " + std::to_string(pos) + " has an impossible length " +
                  std::to_string(len);
        return r;
      }
      if (left - kHeader < len) {
        // The frame runs past the end of the file: what a crash in the
        // middle of the last write leaves. (A length corrupted in the
        // middle of the log to point past the end looks the same; see
        // NOTES.md m4 for that limit.)
        why = "partial payload";
      } else {
        end = pos + kHeader + len;
        const char* payload = bytes.data() + pos + kHeader;
        if (crc32c(payload, len, crc32c(bytes.data() + pos, 4)) != crc) {
            why = "checksum mismatch";
        } else {
          v1::WalRecord rec;
          if (!rec.ParseFromArray(payload, static_cast<int>(len))) {
            // The checksum matched, so these are the bytes that were
            // written: not a torn write but a record this build cannot
            // read. Never skip it.
            r.error = "record at offset " + std::to_string(pos) + " has a valid checksum but does not parse";
            return r;
          }
          r.records.push_back(std::move(rec));
          pos = end;
          r.valid_bytes = pos;
          continue;
        }
      }
    }
    // A bad frame. A crash mid-append leaves a prefix of the last batch
    // (and, on some file systems, zero-filled space after it); anything
    // non-zero after the bad frame means valid-looking data follows it,
    // i.e. the damage is in the middle of the log.
    if (all_zero(bytes.substr(end))) {
      r.torn_bytes = bytes.size() - pos;
      return r;
    }
    r.error = "corrupt record at offset " + std::to_string(pos) + " (" + why + ") followed by " +
              std::to_string(bytes.size() - end) + " more bytes of data";
    return r;
  }
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
    const bool frame_ok = bytes.size() >= kHeader &&
                          get_u32(bytes.data()) == bytes.size() - kHeader &&
                          crc32c(bytes.data() + kHeader, bytes.size() - kHeader,
                                 crc32c(bytes.data(), 4)) == get_u32(bytes.data() + 4);
    v1::Snapshot snap;
    if (!frame_ok || !snap.ParseFromArray(bytes.data() + kHeader,
                                          static_cast<int>(bytes.size() - kHeader))) {
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
  while (!data.empty()) {
    const ssize_t n = ::write(fd, data.data(), data.size());
    if (n < 0) {
      if (errno == EINTR) continue;
      *error = errno_text("write " + log_path(dir_).string());
      return false;
    }
    data.remove_prefix(static_cast<size_t>(n));  // a short write: carry on
  }
  return true;
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
