#pragma once
// Runtime: how fleetlet runs a task, behind an interface (m2).
//
// WHY AN INTERFACE. The agent's job (register, heartbeat, reconcile what
// the master wants against what is running) does not care whether a task
// is a forked process or a container. ProcessRuntime (fork/exec, m2) and
// DockerRuntime (m2b, cgroup limits) both implement this, and the agent
// never learns which one it has.
//
// THE CONTRACT IS NON-BLOCKING. start() returns without waiting for the
// task to be fully up, stop() only begins stopping, and reap() collects
// exits without waiting. The agent calls all three from its one loop
// thread, so a slow task (or a slow `docker run`) can never delay a
// heartbeat. A runtime whose start is itself slow (DockerRuntime) may
// accept the start and report a failure later, through reap(), as an
// ExitInfo with start_failed set. Implementations need not be
// thread-safe.

#include <cstdint>
#include <filesystem>
#include <string>
#include <vector>

#include "model.h"

namespace fleet {

struct LaunchSpec {
  std::vector<std::string> argv;  // argv[0] is looked up on PATH if it has no '/'
  Resources limits;               // the task's request
  std::filesystem::path log_path; // stdout and stderr both go here
  // Container runtimes only (ProcessRuntime ignores both). image: empty
  // means the runtime's default. name: unique per instance, the agent
  // builds it from (task, attempt, incarnation).
  std::string image;
  std::string name;
};

// Opaque id a runtime hands out for a started task. Not a pid, so a
// container runtime can use the same type.
using RuntimeHandle = uint64_t;

struct StartResult {
  bool ok = false;
  RuntimeHandle handle = 0;
  std::string error;  // why it did not start (e.g. "command not found")
};

struct ExitInfo {
  RuntimeHandle handle = 0;
  int exit_code = 0;  // meaningful when signal == 0
  int signal = 0;     // nonzero if the task was killed by a signal
  // The kernel killed the task for exceeding its memory limit (a cgroup
  // OOM kill). Only a runtime with real memory enforcement sets it.
  bool oom_killed = false;
  // The task never ran: start() accepted it, but the runtime found out
  // later that it could not start (e.g. `docker run` failed to pull).
  bool start_failed = false;
  std::string message;  // human-readable detail, e.g. docker's error text
};

class Runtime {
 public:
  virtual ~Runtime() = default;
  virtual StartResult start(const LaunchSpec& spec) = 0;
  // Begin a graceful stop: ask nicely now, force after a grace period
  // (enforced inside reap()). Idempotent; unknown handles are ignored.
  virtual void stop(RuntimeHandle handle) = 0;
  // Exits since the last call. Each handle is reported exactly once and is
  // invalid afterwards.
  virtual std::vector<ExitInfo> reap() = 0;
  // Kill everything immediately and wait for it. For agent shutdown.
  virtual void kill_all() = 0;
  virtual size_t live_count() const = 0;
};

}  // namespace fleet
