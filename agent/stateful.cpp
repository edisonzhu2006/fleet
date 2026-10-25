// Stateful tasks on the agent side (m5): the resume decision and the
// attempt fence. See stateful.h for the model.

#include "stateful.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include "durable_io.h"

namespace fleet {
namespace fs = std::filesystem;

namespace {

// Closes the fd (and so drops its flock) on every return path.
struct Fd {
  int fd = -1;
  ~Fd() {
    if (fd >= 0) ::close(fd);
  }
};

// The whole file from offset 0 (it is one short line).
bool read_all(int fd, std::string* out) {
  out->clear();
  char buf[256];
  off_t off = 0;
  for (;;) {
    const ssize_t n = ::pread(fd, buf, sizeof(buf), off);
    if (n < 0) {
      if (errno == EINTR) continue;
      return false;
    }
    if (n == 0) return true;
    out->append(buf, static_cast<size_t>(n));
    off += n;
    if (out->size() > 4096) return true;  // not a fence file; parse will refuse it
  }
}

}  // namespace

CheckpointDirState inspect_checkpoint_dir(const fs::path& dir, std::string* error) {
  CheckpointDirState s;
  std::error_code ec;
  const fs::file_status st = fs::status(dir, ec);
  if (ec && ec != std::errc::no_such_file_or_directory) {
    *error = "cannot stat checkpoint dir " + dir.string() + ": " + ec.message();
    return {};
  }
  if (!fs::exists(st)) return s;  // never created: a first start
  if (!fs::is_directory(st)) {
    *error = "checkpoint dir " + dir.string() + " exists and is not a directory";
    return {};
  }
  s.exists = true;
  for (fs::directory_iterator it(dir, ec), end; !ec && it != end; it.increment(ec)) {
    if (it->path().filename() == "MANIFEST") {
      s.has_manifest = true;
    } else {
      s.has_other = true;
    }
  }
  if (ec) {
    *error = "cannot list checkpoint dir " + dir.string() + ": " + ec.message();
    return {};
  }
  return s;
}

StartMode decide_start_mode(const CheckpointDirState& s) {
  // Anything there at all came from an earlier attempt, committed or not;
  // only the resume command knows what to do with it (stateful.h).
  if (s.has_manifest || s.has_other) return StartMode::kResume;
  return StartMode::kFresh;
}

const char* to_string(StartMode m) { return m == StartMode::kResume ? "resume" : "fresh"; }

std::string FenceToken::to_string() const {
  return "task " + std::to_string(task_id) + " attempt " + std::to_string(attempt);
}

fs::path fence_path(const fs::path& checkpoint_dir) {
  // Appended to the full path, not replace_extension(): "/ckpt/job.v2"
  // must not become "/ckpt/job.fleet-fence".
  return fs::path(checkpoint_dir.string() + ".fleet-fence");
}

bool fence_allows(const std::optional<FenceToken>& recorded, const FenceToken& mine) {
  return !recorded || *recorded <= mine;
}

std::string format_fence(const FenceToken& t) { return t.to_string() + "\n"; }

std::optional<FenceToken> parse_fence(const std::string& text) {
  unsigned long task = 0, attempt = 0;
  int consumed = 0;
  // %n records how far the parse got, so trailing garbage is detected.
  if (std::sscanf(text.c_str(), "task %lu attempt %lu\n%n", &task, &attempt, &consumed) != 2) {
    return std::nullopt;
  }
  if (static_cast<size_t>(consumed) != text.size()) return std::nullopt;
  if (task > UINT32_MAX || attempt > UINT32_MAX) return std::nullopt;
  // The canonical form must round-trip, which rejects "+5", leading
  // zeros and spaces that sscanf would have accepted.
  const FenceToken t{static_cast<uint32_t>(task), static_cast<uint32_t>(attempt)};
  if (format_fence(t) != text) return std::nullopt;
  return t;
}

FenceResult fence_acquire(const fs::path& fence_file, const FenceToken& mine) {
  FenceResult r;
  Fd f;
  f.fd = ::open(fence_file.c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0644);
  if (f.fd < 0) {
    r.error = errno_text("open " + fence_file.string());
    return r;
  }
  // Exclusive for the whole read-compare-write, so two agents racing to
  // start attempts of one task cannot both see the old token and both
  // write theirs. flock is per open file description and works across
  // processes on a local file system; NOTES.md m5 says what it does not
  // cover on network file systems.
  while (::flock(f.fd, LOCK_EX) != 0) {
    if (errno != EINTR) {
      r.error = errno_text("flock " + fence_file.string());
      return r;
    }
  }
  std::string text;
  if (!read_all(f.fd, &text)) {
    r.error = errno_text("read " + fence_file.string());
    return r;
  }
  if (!text.empty()) {
    r.recorded = parse_fence(text);
    if (!r.recorded) {
      r.error = "fence file " + fence_file.string() + " is not a fence record";
      return r;
    }
  }
  if (!fence_allows(r.recorded, mine)) return r;  // superseded: not granted, no error
  if (r.recorded != mine) {
    // Rewritten in place, under the lock (a temp file + rename would swap
    // the inode out from under another agent's flock). A crash mid-write
    // leaves a record that does not parse, which refuses every start
    // until an operator looks: the safe direction.
    const std::string line = format_fence(mine);
    if (::ftruncate(f.fd, 0) != 0 ||
        ::pwrite(f.fd, line.data(), line.size(), 0) != static_cast<ssize_t>(line.size())) {
      r.error = errno_text("write " + fence_file.string());
      return r;
    }
    // Durable before the task starts: after a power cut the file must not
    // show an older token than one an attempt has already run under.
    if (!sync_fd(f.fd, SyncMode::kFull)) {
      r.error = errno_text("sync " + fence_file.string());
      return r;
    }
  }
  r.granted = true;
  return r;
}

std::optional<FenceToken> fence_read(const fs::path& fence_file, std::string* error) {
  Fd f;
  f.fd = ::open(fence_file.c_str(), O_RDONLY | O_CLOEXEC);
  if (f.fd < 0) {
    if (errno != ENOENT) *error = errno_text("open " + fence_file.string());
    return std::nullopt;
  }
  // Shared: never sees the half-written state of an acquire in progress.
  while (::flock(f.fd, LOCK_SH) != 0) {
    if (errno != EINTR) {
      *error = errno_text("flock " + fence_file.string());
      return std::nullopt;
    }
  }
  std::string text;
  if (!read_all(f.fd, &text)) {
    *error = errno_text("read " + fence_file.string());
    return std::nullopt;
  }
  if (text.empty()) return std::nullopt;  // created by an acquire that failed before writing
  std::optional<FenceToken> t = parse_fence(text);
  if (!t) *error = "fence file " + fence_file.string() + " is not a fence record";
  return t;
}

}  // namespace fleet
