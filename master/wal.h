#pragma once
// The master's on-disk state: a write-ahead log of records plus a snapshot
// (m4). Framing, checksums, durability (fsync), recovery, rotation.
//
// WHY THIS EXISTS. MasterState lives in memory; a SIGKILL or a power cut
// erases it. Storage makes every change durable before the master acts
// on it (the ordering rule, enforced by MasterServer: no reply leaves the
// process until the records behind it are on disk), and rebuilds the state
// after a crash from the newest snapshot plus the records logged after it.
//
// FILES, in one directory (fleetmaster --state-dir):
//   snapshot   one frame holding a v1::Snapshot: the state as of some LSN.
//   wal        frames holding v1::WalRecords, in LSN order, appended.
// A frame is
//     [u32 length][u32 crc32c(length bytes ++ payload)][payload]
// little-endian, payload = the serialized message. The length prefix lets
// a reader find record boundaries; the checksum tells a record that was
// written completely from one that was torn by a crash mid-write (or
// rotted on disk). The CRC covers the length too, so a corrupted length
// cannot make a wrong span of bytes look valid. CRC-32C (Castagnoli), as
// in ext4, iSCSI, LevelDB and RocksDB: better error detection than the
// zlib CRC-32 for the same cost, and small enough to write by hand here
// instead of adding a dependency.
//
// DURABILITY. write() only reaches the kernel's page cache; that survives
// a process crash but not a power cut or kernel panic. fsync() on Linux
// flushes the file through to the device. On macOS fsync() hands the data
// to the drive but does not make the drive flush its own write cache, so
// a power cut can still lose it (Apple's fsync(2) man page says so and
// points to F_FULLFSYNC). Storage therefore syncs with fcntl(F_FULLFSYNC)
// on Apple and fdatasync() elsewhere. Measured on this Mac (NOTES.md m4):
// about 3 ms per F_FULLFSYNC versus 27 us per plain fsync, which is
// exactly why group commit matters.
//
// GROUP COMMIT. append() takes a batch (everything one MasterCore call,
// e.g. one scheduler iteration, decided), writes it with one write() and
// syncs once. SyncPolicy::kPerRecord instead syncs after every record; it
// exists to measure what group commit saves (bench/wal_commit.sh).
//
// TORN TAIL. A crash can leave the last batch partly written. Since that
// batch was never synced, nothing was acted on, and recovery discards it:
// the first invalid frame is a torn tail when everything after it is zero
// bytes or nothing. An invalid frame followed by non-zero data is
// corruption in the middle of the log, which recovery refuses to paper
// over (it would silently drop acknowledged records); the master then
// does not start, and says where. NOTES.md m4 has the two cases this
// misjudges: a power cut that persists pages of the last batch out of
// order (refused, the safe direction), and a length field corrupted to
// point past the end of the file (taken for a tear).
//
// SNAPSHOT AND ROTATION. write_snapshot(): write snapshot.tmp, sync it,
// rename it over snapshot, sync the directory (rename is a directory
// change and is not durable until the directory is synced), then rotate:
// an empty wal.next, synced, renamed over wal, directory synced. Every
// crash point in between recovers: before the first rename, the old
// snapshot and the full log; after it, the new snapshot and the old log,
// whose records at or below the snapshot's LSN replay skips.
//
// NOT THREAD-SAFE. MasterServer calls it under the master's lock.

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "master_state.h"
#include "wal.pb.h"

namespace fleet {

// CRC-32C (Castagnoli polynomial, reflected 0x82F63B78), table-driven.
// `crc` continues a previous call: crc32c(b, crc32c(a)) == crc32c(a ++ b).
uint32_t crc32c(const void* data, size_t n, uint32_t crc = 0);

// How a batch is made durable.
enum class SyncMode : uint8_t {
  kFull,   // F_FULLFSYNC on Apple, fdatasync elsewhere: survives power loss
  kFsync,  // plain fsync: on macOS does not flush the drive cache
  kNone,   // page cache only: survives a process crash, not a power cut
};
enum class SyncPolicy : uint8_t {
  kGroup,      // one sync per append() batch (the default)
  kPerRecord,  // one sync per record (for comparison)
};

// Makes everything written to fd durable, per mode. Returns false with
// errno set on failure.
bool sync_fd(int fd, SyncMode mode);

// Frames and parses (exposed for tests and the benchmark).
void append_frame(std::string* out, const std::string& payload);

struct ParsedLog {
  std::vector<v1::WalRecord> records;
  size_t valid_bytes = 0;  // prefix made of complete, valid frames
  size_t torn_bytes = 0;   // bytes after it that were discarded as a torn tail
  std::string error;       // non-empty: corruption, not a torn tail
};
ParsedLog parse_log(std::string_view bytes);

struct StorageOptions {
  SyncMode sync = SyncMode::kFull;
  SyncPolicy policy = SyncPolicy::kGroup;
  // Take a snapshot and rotate the log once this many records were logged
  // since the last snapshot (0 = never). It bounds replay time and log
  // size; the cost is one state serialization and four syncs.
  uint64_t snapshot_every = 10000;
};

struct RecoveryInfo {
  bool had_snapshot = false;
  uint64_t snapshot_lsn = 0;
  size_t replayed = 0;  // records applied after the snapshot
  size_t skipped = 0;   // records at or below the snapshot's LSN
  size_t torn_bytes = 0;
  uint64_t lsn = 0;     // the recovered state's LSN
  double load_ms = 0;   // reading + parsing + applying
};

struct StorageStats {
  uint64_t appends = 0;  // append() calls with at least one record
  uint64_t records = 0;
  uint64_t bytes = 0;
  uint64_t syncs = 0;
  double sync_ms = 0;  // total time inside sync_fd for the log
  uint64_t snapshots = 0;
  uint64_t records_since_snapshot = 0;
};

class Storage {
 public:
  ~Storage();
  Storage(const Storage&) = delete;
  Storage& operator=(const Storage&) = delete;

  // Opens (creating) dir, rebuilds *state from the snapshot and the log,
  // truncates a torn tail, and opens the log for appending. *state must be
  // empty on entry. On corruption returns nullptr with *error set and
  // leaves the files untouched, so an operator can look at them.
  static std::unique_ptr<Storage> open(const std::filesystem::path& dir, StorageOptions options,
                                       MasterState* state, RecoveryInfo* info,
                                       std::string* error);

  // Appends records (consecutive LSNs, continuing the log) and makes them
  // durable per the options. False with *error set on any I/O failure; the
  // caller must then stop (MasterServer aborts): after a failed fsync the
  // kernel may already have dropped the dirty pages, so retrying cannot
  // prove the data is on disk (the PostgreSQL "fsyncgate" lesson).
  bool append(const std::vector<v1::WalRecord>& records, std::string* error);

  // Snapshot of `state` (which must include every appended record), then
  // log rotation. See the header comment for the order of steps.
  bool write_snapshot(const MasterState& state, std::string* error);

  bool snapshot_due() const {
    return options_.snapshot_every > 0 &&
           stats_.records_since_snapshot >= options_.snapshot_every;
  }
  const StorageStats& stats() const { return stats_; }
  const std::filesystem::path& dir() const { return dir_; }

  static std::filesystem::path log_path(const std::filesystem::path& dir) { return dir / "wal"; }
  static std::filesystem::path snapshot_path(const std::filesystem::path& dir) {
    return dir / "snapshot";
  }

 private:
  Storage(std::filesystem::path dir, StorageOptions options);
  bool open_log_for_append(std::string* error);
  bool write_all(int fd, std::string_view data, std::string* error);
  bool timed_sync(int fd, std::string* error);

  std::filesystem::path dir_;
  StorageOptions options_;
  int log_fd_ = -1;
  StorageStats stats_;
};

}  // namespace fleet
