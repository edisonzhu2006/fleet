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
#include <map>
#include <sys/types.h>

#include "runtime.h"

namespace fleet {

struct ProcessRuntimeOptions {
  // Time between SIGTERM and SIGKILL on stop().
  std::chrono::milliseconds stop_grace{2000};
  // Apply RLIMIT_AS = request. Off for tests that probe behavior without
  // limits; on by default.
  bool limit_memory = true;
};

class ProcessRuntime final : public Runtime {
 public:
  explicit ProcessRuntime(ProcessRuntimeOptions options = {});
  ~ProcessRuntime() override;  // kill_all()

  StartResult start(const LaunchSpec& spec) override;
  void stop(RuntimeHandle handle) override;
  std::vector<ExitInfo> reap() override;
  void kill_all() override;
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

  ProcessRuntimeOptions options_;
  std::map<RuntimeHandle, Proc> procs_;
  RuntimeHandle next_handle_ = 1;
};

}  // namespace fleet
