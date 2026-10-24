#pragma once
// ProcessRuntime: tasks as plain forked processes (m2).
//
// WHY THIS EXISTS. It is the runtime that works everywhere (unit tests,
// CI runners, a laptop without Docker). Each task:
//   - is fork/exec'd into its own process group (setpgid), so stopping it
//     signals the whole group, including any children the task spawned
//     (a shell script's subprocesses would otherwise survive a stop);
//   - has stdout and stderr in its own log file, stdin from /dev/null;
//   - gets RLIMIT_AS = its memory request where the OS enforces it, and
//     RLIMIT_CORE = 0 so a crashing task cannot fill the disk with cores;
//   - is reaped with waitpid() on its own pid (never waitpid(-1), which
//     would steal exits from other runtimes in the same process, as the
//     in-process integration tests have several agents).
//
// LEFTOVERS FROM A CRASHED AGENT (m3). If fleetlet is SIGKILLed its tasks
// keep running (macOS has no PR_SET_PDEATHSIG to take them along). So, with
// state_dir set, each task's pid and its kernel start time are recorded in
// a file there, removed when the task is reaped; a fresh fleetlet calls
// kill_leftovers(), which kills the process group of every recorded pid
// whose process still has the recorded start time. The start time is what
// makes this safe: pids are reused, and a pid alone could name some
// unrelated process by now; pid plus start time cannot. Limits: a task
// whose leader exited while the agent was down leaves its other group
// members alive (only a live leader can be matched), and a crash between
// fork and writing the file leaves that one task unrecorded.
//
// WHAT macOS DOES NOT GIVE US. There are no cgroups. setrlimit(RLIMIT_AS)
// and RLIMIT_DATA fail with EINVAL for any value below the very large
// virtual reservation every macOS process already has (measured: refused
// at 64 GB, accepted at ~500 GB), so on macOS memory is not enforced per
// task; the child writes a one-line warning to the task's log saying so.
// No rlimit limits CPU *rate* (RLIMIT_CPU caps total CPU seconds, which
// would kill a long-running server), so CPU is enforced only by the
// scheduler's accounting (no node is ever over-committed on paper).
// Real enforcement is DockerRuntime's job (m2b). On Linux, RLIMIT_AS does
// apply, with the known caveat that it limits virtual address space, not
// resident memory, so runtimes that reserve large heaps (JVM, Go) need
// headroom.

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <map>
#include <optional>
#include <sys/types.h>

#include "runtime.h"

namespace fleet {

struct ProcessRuntimeOptions {
  // Time between SIGTERM and SIGKILL on stop().
  std::chrono::milliseconds stop_grace{2000};
  // Apply RLIMIT_AS = request. Off for tests that probe behavior without
  // limits; on by default.
  bool limit_memory = true;
  // Where running tasks' pids are recorded for kill_leftovers(). Empty:
  // not recorded (tests that run several runtimes in one process). fleetlet
  // uses <work-dir>/pids, which is per node.
  std::filesystem::path state_dir;
};

// When the process started, in microseconds since the epoch on macOS and
// clock ticks since boot on Linux (only ever compared for equality), or
// nullopt if there is no such process.
std::optional<uint64_t> process_start_time(pid_t pid);

class ProcessRuntime final : public Runtime {
 public:
  explicit ProcessRuntime(ProcessRuntimeOptions options = {});
  ~ProcessRuntime() override;  // kill_all()

  StartResult start(const LaunchSpec& spec) override;
  void stop(RuntimeHandle handle) override;
  std::vector<ExitInfo> reap() override;
  void kill_all() override;
  size_t kill_leftovers() override;
  size_t live_count() const override { return procs_.size(); }

  // The pid behind a handle, or -1. For tests and logging only.
  pid_t pid_of(RuntimeHandle handle) const;

 private:
  struct Proc {
    pid_t pid = -1;  // also the process group id
    bool stopping = false;
    std::chrono::steady_clock::time_point kill_at;  // SIGKILL deadline
  };
  // Collect the exit of one process if it has exited. block = wait for it.
  bool try_collect(RuntimeHandle h, Proc& p, bool block, ExitInfo* out);
  void record_pid(pid_t pid) const;
  void forget_pid(pid_t pid) const;

  ProcessRuntimeOptions options_;
  std::map<RuntimeHandle, Proc> procs_;
  RuntimeHandle next_handle_ = 1;
};

}  // namespace fleet
