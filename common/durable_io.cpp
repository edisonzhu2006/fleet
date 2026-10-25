// durable_io implementation: CRC-32C, frames and the torn-tail rule,
// fsync per platform, whole-file and atomic-replace helpers. Moved here
// from master/wal.cpp in m4b, unchanged in behavior, so the Raft log can
// share it. See durable_io.h.

#include "durable_io.h"

#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <sstream>
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

}  // namespace

std::string errno_text(const std::string& what) { return what + ": " + std::strerror(errno); }

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

void append_frame(std::string* out, std::string_view payload) {
  const auto len = static_cast<uint32_t>(payload.size());
  std::string len_bytes;
  put_u32(&len_bytes, len);
  const uint32_t crc = crc32c(payload.data(), payload.size(), crc32c(len_bytes.data(), 4));
  out->append(len_bytes);
  put_u32(out, crc);
  out->append(payload);
}

FrameScan split_frames(std::string_view bytes) {
  FrameScan r;
  size_t pos = 0;
  while (pos < bytes.size()) {
    const size_t left = bytes.size() - pos;
    // Where the frame starting at pos ends, as far as the file goes, and
    // (if it is not a complete, valid frame) why not.
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
          r.payloads.emplace_back(payload, len);
          r.offsets.push_back(pos);
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

bool unframe_single(std::string_view bytes, std::string_view* payload) {
  if (bytes.size() < kHeader || get_u32(bytes.data()) != bytes.size() - kHeader) return false;
  if (crc32c(bytes.data() + kHeader, bytes.size() - kHeader, crc32c(bytes.data(), 4)) !=
      get_u32(bytes.data() + 4)) {
    return false;
  }
  *payload = bytes.substr(kHeader);
  return true;
}

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

bool write_all(int fd, std::string_view data, const std::string& what, std::string* error) {
  while (!data.empty()) {
    const ssize_t n = ::write(fd, data.data(), data.size());
    if (n < 0) {
      if (errno == EINTR) continue;
      *error = errno_text("write " + what);
      return false;
    }
    data.remove_prefix(static_cast<size_t>(n));  // a short write: carry on
  }
  return true;
}

bool write_file_atomic(const fs::path& path, std::string_view data, SyncMode mode,
                       std::string* error) {
  const fs::path tmp = path.string() + ".tmp";
  const int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd < 0) {
    *error = errno_text("open " + tmp.string());
    return false;
  }
  bool ok = write_all(fd, data, tmp.string(), error);
  if (ok && !sync_fd(fd, mode)) {
    *error = errno_text("sync " + tmp.string());
    ok = false;
  }
  ::close(fd);
  if (!ok) return false;
  if (::rename(tmp.c_str(), path.c_str()) != 0) {
    *error = errno_text("rename " + tmp.string());
    return false;
  }
  return sync_dir(path.parent_path(), mode, error);
}

}  // namespace fleet
