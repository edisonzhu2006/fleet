#pragma once
// Subprocess: run a short-lived helper command (the docker CLI) without
// blocking the agent's loop thread (m2b).
//
// WHY THIS EXISTS. DockerRuntime drives Docker by running the docker CLI:
// `docker run -d` takes 100 to 350 ms on this Mac (measured, NOTES m2b)
// and much longer when the image has to be pulled, and `docker inspect`
// about 40 ms. The Runtime contract (runtime.h) is non-blocking, because
// the agent calls it from the same thread that heartbeats; a start that
// waited for `docker run` would delay heartbeats by seconds. So every CLI
// call is spawned, and the caller polls it from the loop:
//
//   auto cmd = Subprocess::spawn({"docker", "inspect", ...}, 30s);
//   ...  each loop iteration:  if (cmd->poll()) use(cmd->result());
//
// poll() never blocks: it drains whatever output is ready from
// non-blocking pipes and checks for exit with waitpid(WNOHANG). A command
// that runs past its timeout is SIGKILLed and reported as timed_out, so a
// hung Docker daemon cannot wedge the agent.
//
// FORK SAFETY. Same rules as ProcessRuntime: fleetlet is multithreaded
// (gRPC), so the child does only async-signal-safe system calls between
// fork and execve; argv, the resolved path and every fd are prepared in
// the parent.

#include <chrono>
#include <memory>
#include <string>
#include <sys/types.h>
#include <vector>

namespace fleet {

// argv[0] as given if it contains '/', else the first executable regular
// file of that name on PATH, else "". Done in the parent so the child can
// call execve (async-signal-safe) instead of execvp (not guaranteed to be).
std::string resolve_command(const std::string& cmd);

// In a freshly forked child, before exec: clear the signal mask and reset
// every disposition to default. fleetlet blocks SIGINT/SIGTERM in all its
// threads (common/signals.h) and both the mask and SIG_IGN survive exec,
// so without this the child could never be stopped with SIGTERM.
// Async-signal-safe.
void reset_signals_in_child();

struct CommandResult {
  int exit_code = -1;      // valid when signal == 0
  int signal = 0;          // nonzero if the command died of a signal
  bool timed_out = false;  // we killed it at its deadline
  std::string out;         // captured stdout (empty if redirected)
  std::string err;         // captured stderr (empty if redirected)
  bool ok() const { return !timed_out && signal == 0 && exit_code == 0; }
};

class Subprocess {
 public:
  // Starts argv. Returns nullptr and sets *error if it could not be
  // started (command not on PATH, fork or pipe failure). If output_fd >= 0
  // the child's stdout and stderr both go to that fd instead of being
  // captured (used to copy `docker logs` straight into a task's log file);
  // the caller keeps ownership of output_fd and may close it after spawn.
  static std::unique_ptr<Subprocess> spawn(const std::vector<std::string>& argv,
                                           std::chrono::milliseconds timeout,
                                           std::string* error, int output_fd = -1);
  ~Subprocess();  // a still-running command is killed and reaped

  Subprocess(const Subprocess&) = delete;
  Subprocess& operator=(const Subprocess&) = delete;

  // Non-blocking. True once the command has exited and its output is
  // fully read; result() is valid from then on. Enforces the timeout.
  bool poll();
  // Blocks until poll() would return true (the timeout still applies).
  void wait();
  // SIGKILL now. The command still has to be poll()ed or wait()ed.
  void kill();
  bool done() const { return done_; }
  const CommandResult& result() const { return result_; }

 private:
  Subprocess() = default;
  void drain(int& fd, std::string& into);

  pid_t pid_ = -1;
  int out_fd_ = -1;  // read ends of the stdout / stderr pipes, non-blocking
  int err_fd_ = -1;
  bool exited_ = false;
  bool done_ = false;
  std::chrono::steady_clock::time_point deadline_;
  CommandResult result_;
};

}  // namespace fleet
