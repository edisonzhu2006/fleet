#pragma once
// Stateful tasks on the agent side (m5): which command line starts an
// attempt, and the attempt fence that keeps an old attempt from starting
// once a newer one has.
//
// WHAT A STATEFUL TASK IS. A task with a checkpoint directory on storage
// every node can reach (on one machine, a common directory), plus two
// command lines: argv for the first start and resume_argv for continuing
// from a checkpoint (for streamkit, the same flags plus --resume). The
// master never looks at the storage; the agent decides, at the moment it
// starts the attempt, because only then is the directory's content the
// one the new process will see.
//
// THE RESUME DECISION (decide_start_mode). Fresh, with argv, when the
// directory is missing or empty: no earlier attempt left anything. Resume,
// with resume_argv, in every other case:
//   - a MANIFEST exists: the previous attempt committed a checkpoint, and
//     resuming continues from it instead of starting over;
//   - no MANIFEST but other entries: the previous attempt died before its
//     first commit (or while removing the directory at the very end). The
//     resume command is still the right one: by its contract it continues
//     from a committed checkpoint "if there is one, otherwise starts over"
//     (streamkit --resume deletes the uncommitted leftovers and starts at
//     byte 0), while argv would be refused, because streamkit will not
//     start fresh over a directory holding a previous run. The other choice,
//     the agent deleting the leftovers itself and running argv, would put
//     knowledge of the task's file format into fleet and have fleet delete
//     user data; NOTES.md m5 compares the two.
//
// THE ATTEMPT FENCE (fence_acquire). Two attempts of one task running at
// once would race on one checkpoint directory and one output file. The
// master is the first line: it places a stateful task again only after its
// previous node is declared dead or re-registered (whose agent killed its
// predecessor's tasks first), never preempts one, and moves one off a
// draining node only after its exit. The fence is the second line, at the
// storage: a small file next to the checkpoint directory,
//     <checkpoint dir>.fleet-fence   holding   "task <id> attempt <k>"
// records the newest (task id, attempt) that has started on it. Attempts
// are numbered by the master and recorded in its replicated log, so a
// later placement always has a larger number. Before starting attempt k an
// agent locks the file, and
//   - refuses (START_FAILED, "superseded") if a newer token is recorded:
//     some node already started a later attempt, so this start is stale;
//   - otherwise records its own token and starts.
// While an attempt runs, its agent re-reads the file every heartbeat and
// stops the attempt if a newer token appears (Agent::check_fences), which
// shortens the window in which a node that lost contact with the master
// (so cannot be fenced by epoch) runs beside its replacement.
//
// The token is (task id, attempt), compared in that order, not the attempt
// alone: a directory reused by a later submission (after the earlier task
// finished) gets a larger task id, so its attempt 1 outranks attempt 5 of
// the old task. Task ids only grow because the master's state is durable
// and replicated (m4, m4b).
//
// WHAT THIS DOES NOT GIVE (NOTES.md m5 has the full discussion). The fence
// is checked when an attempt starts and polled while it runs; the task's
// own writes do not carry the token, so a process that is already running
// keeps writing until something stops it. Real systems close that with a
// lease the task must hold to write (Chubby, a ZooKeeper ephemeral node)
// and storage that rejects writes carrying an older token than the newest
// it has seen (the fencing tokens of Chubby sequencers, or GFS/Colossus
// chunk version numbers).

#include <compare>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>

namespace fleet {

// ---------------------------------------------------------- resume decision

// What a checkpoint directory holds, as far as the start decision cares.
struct CheckpointDirState {
  bool exists = false;
  bool has_manifest = false;  // a committed checkpoint
  bool has_other = false;     // any entry besides MANIFEST
};

// Looks at the directory. A path that exists but is not a directory, or
// cannot be read, sets *error and returns a default state.
CheckpointDirState inspect_checkpoint_dir(const std::filesystem::path& dir, std::string* error);

enum class StartMode {
  kFresh,   // argv
  kResume,  // resume_argv
};

// Pure: fresh only when nothing is there (see the file comment).
StartMode decide_start_mode(const CheckpointDirState& s);
const char* to_string(StartMode m);

// ---------------------------------------------------------- attempt fence

struct FenceToken {
  uint32_t task_id = 0;
  uint32_t attempt = 0;
  // Task id first, then attempt (see the file comment).
  friend auto operator<=>(const FenceToken&, const FenceToken&) = default;
  std::string to_string() const;
};

// Where the fence for a checkpoint directory lives: beside it, not in it,
// because the task owns the directory's content (streamkit deletes the
// whole directory when its run completes, which would erase the fence
// exactly when a stale attempt could still try to start).
std::filesystem::path fence_path(const std::filesystem::path& checkpoint_dir);

// Pure: may `mine` start, given the newest token recorded so far (nullopt:
// none)? Equal is allowed: it is this attempt's own record, written by an
// earlier start of the same instance that the agent did not get to report.
bool fence_allows(const std::optional<FenceToken>& recorded, const FenceToken& mine);

// The file format: one line "task <id> attempt <k>". nullopt on anything
// else, which callers treat as "cannot tell", never as "no token".
std::string format_fence(const FenceToken& t);
std::optional<FenceToken> parse_fence(const std::string& text);

struct FenceResult {
  bool granted = false;
  std::optional<FenceToken> recorded;  // the token found before this call
  std::string error;  // set when the file could not be used; granted is false
};

// Under an exclusive flock on the fence file: read the recorded token; if
// fence_allows(), write `mine` (fsync'd, so a power cut cannot bring back
// an older token) and grant. A file that exists but does not parse is an
// error, not a grant: refusing to start is the safe failure.
FenceResult fence_acquire(const std::filesystem::path& fence_file, const FenceToken& mine);

// The recorded token without changing it (for the running check). nullopt
// with *error empty: no file yet.
std::optional<FenceToken> fence_read(const std::filesystem::path& fence_file, std::string* error);

}  // namespace fleet
