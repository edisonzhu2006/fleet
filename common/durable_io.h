#pragma once
// durable_io: checksummed frames and the fsync rules, shared by the m4
// write-ahead log (master/wal.cpp) and the Raft log (raft/raft_storage.cpp).
//
// WHY THIS EXISTS. m4 wrote these for the master's WAL. m4b's Raft storage
// needs exactly the same things (a record format that tells a torn write
// from damage, a sync that really reaches the platter on macOS, an atomic
// file replace), so they moved here instead of being written twice. The
// behavior is unchanged; NOTES.md m4 has the reasoning for each rule.
//
// FRAME = [u32 length][u32 crc32c(length bytes ++ payload)][payload],
// little-endian. The length lets a reader find record boundaries; the
// checksum tells a completely written frame from one torn by a crash; it
// covers the length too, so a corrupted length cannot make a wrong span of
// bytes look valid.
//
// TORN TAIL vs CORRUPTION (split_frames). The first bad frame is a torn
// tail when everything after it is zero bytes or nothing: what a crash in
// the middle of the last, never-synced write leaves. A bad frame followed
// by non-zero data is damage in the middle, and is an error: skipping it
// would silently drop records that were acknowledged.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <string>
#include <string_view>
#include <vector>

namespace fleet {

// CRC-32C (Castagnoli polynomial, reflected 0x82F63B78), table-driven.
// `crc` continues a previous call: crc32c(b, crc32c(a)) == crc32c(a ++ b).
uint32_t crc32c(const void* data, size_t n, uint32_t crc = 0);

// How a write is made durable.
enum class SyncMode : uint8_t {
  kFull,   // F_FULLFSYNC on Apple, fdatasync elsewhere: survives power loss
  kFsync,  // plain fsync: on macOS does not flush the drive cache
  kNone,   // page cache only: survives a process crash, not a power cut
};

// Makes everything written to fd durable, per mode. Returns false with
// errno set on failure. A failed sync must never be retried and trusted
// (after a failed fsync the kernel may have dropped the dirty pages and
// marked them clean): callers abort.
bool sync_fd(int fd, SyncMode mode);
// rename() and file creation change the directory, not the file; they are
// durable only once the directory itself is synced.
bool sync_dir(const std::filesystem::path& dir, SyncMode mode, std::string* error);

// Appends one frame holding payload to *out.
void append_frame(std::string* out, std::string_view payload);

// The frames found in a byte string (a log file's contents).
struct FrameScan {
  std::vector<std::string_view> payloads;  // views into the scanned bytes
  std::vector<size_t> offsets;             // where each frame starts
  size_t valid_bytes = 0;  // prefix made of complete, valid frames
  size_t torn_bytes = 0;   // bytes after it, discarded as a torn tail
  std::string error;       // non-empty: corruption, not a torn tail
};
FrameScan split_frames(std::string_view bytes);
// A file that holds exactly one frame (a snapshot or a small state file,
// always written whole and renamed into place, so never torn). False if it
// is not exactly one valid frame.
bool unframe_single(std::string_view bytes, std::string_view* payload);

// Whole-file helpers. read_file: a missing file is an empty string with
// *exists false; any other failure is an error.
bool read_file(const std::filesystem::path& p, std::string* out, bool* exists,
               std::string* error);
// write() until done (short writes and EINTR handled).
bool write_all(int fd, std::string_view data, const std::string& what, std::string* error);
// Replaces `path` atomically with `data`: write path.tmp, sync it, rename
// it over path, sync the directory. A crash at any point leaves either the
// old file or the new one, never a mix; the leftover .tmp is never read.
bool write_file_atomic(const std::filesystem::path& path, std::string_view data, SyncMode mode,
                       std::string* error);

std::string errno_text(const std::string& what);

}  // namespace fleet
