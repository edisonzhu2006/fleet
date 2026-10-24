// Subprocess implementation: fork/exec with non-blocking output pipes and
// a deadline, polled by the caller. See subprocess.h for why.

#include "subprocess.h"

#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

extern char** environ;

namespace fleet {
namespace {

// Captured output is capped. The docker CLI prints a few lines; the cap
// only matters if something unexpected floods a pipe, and then keeping the
// first MiB (where the error is) beats growing without bound.
constexpr size_t kMaxCapture = 1 << 20;

void close_quietly(int& fd) {
  if (fd >= 0) ::close(fd);
  fd = -1;
}

void record_status(int status, CommandResult* r) {
  if (WIFSIGNALED(status)) {
    r->signal = WTERMSIG(status);
  } else {
    r->exit_code = WEXITSTATUS(status);
  }
}

}  // namespace

std::string resolve_command(const std::string& cmd) {
  if (cmd.empty()) return {};
  if (cmd.find('/') != std::string::npos) return cmd;
  const char* env = std::getenv("PATH");
  const std::string path = env ? env : "/usr/bin:/bin";
  size_t start = 0;
  while (start <= path.size()) {
    size_t end = path.find(':', start);
    if (end == std::string::npos) end = path.size();
    std::string dir = path.substr(start, end - start);
    if (dir.empty()) dir = ".";
    const std::string candidate = dir + "/" + cmd;
    struct stat sb {};
    if (::stat(candidate.c_str(), &sb) == 0 && S_ISREG(sb.st_mode) &&
        ::access(candidate.c_str(), X_OK) == 0) {
      return candidate;
    }
    start = end + 1;
  }
  return {};
}

void reset_signals_in_child() {
  sigset_t none;
  sigemptyset(&none);
  ::sigprocmask(SIG_SETMASK, &none, nullptr);
  struct sigaction dfl {};
  dfl.sa_handler = SIG_DFL;
  for (int s = 1; s < NSIG; ++s) {
    if (s != SIGKILL && s != SIGSTOP) ::sigaction(s, &dfl, nullptr);
  }
}

std::unique_ptr<Subprocess> Subprocess::spawn(const std::vector<std::string>& argv,
                                              std::chrono::milliseconds timeout,
                                              std::string* error, int output_fd) {
  if (argv.empty()) {
    *error = "empty argv";
    return nullptr;
  }
  const std::string path = resolve_command(argv[0]);
  // resolve_command passes a path with a '/' through unchecked (for
  // ProcessRuntime, whose exec-error pipe reports the exact errno); here
  // a missing CLI should fail at spawn, not as exit 127 a poll later.
  if (path.empty() || ::access(path.c_str(), X_OK) != 0) {
    *error = "command not found: " + argv[0];
    return nullptr;
  }
  // Every fd the parent opens is close-on-exec, so it cannot leak into a
  // task or another helper forked later. The child dup2()s what it needs
  // onto 0/1/2, which clears the flag on the copies. macOS has no pipe2(),
  // so the flag is set right after pipe(). In fleetlet only the agent loop
  // thread forks, so nothing forks in between; in the in-process tests
  // several agents fork concurrently, and a child forked in that window
  // could keep a copy of a pipe end. That costs an fd, not correctness:
  // poll() finishes on the child's exit, not on EOF.
  int out_pipe[2] = {-1, -1};
  int err_pipe[2] = {-1, -1};
  int null_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
  const bool capture = output_fd < 0;
  if (null_fd < 0 || (capture && (::pipe(out_pipe) != 0 || ::pipe(err_pipe) != 0))) {
    *error = std::string("pipe/open failed: ") + std::strerror(errno);
    int fds[] = {null_fd, out_pipe[0], out_pipe[1], err_pipe[0], err_pipe[1]};
    for (int& fd : fds) close_quietly(fd);
    return nullptr;
  }
  for (int fd : {out_pipe[0], out_pipe[1], err_pipe[0], err_pipe[1]}) {
    if (fd >= 0) ::fcntl(fd, F_SETFD, FD_CLOEXEC);
  }
  // Read ends are non-blocking: poll() must never wait for output.
  for (int fd : {out_pipe[0], err_pipe[0]}) {
    if (fd >= 0) ::fcntl(fd, F_SETFL, ::fcntl(fd, F_GETFL) | O_NONBLOCK);
  }
  const int child_out = capture ? out_pipe[1] : output_fd;
  const int child_err = capture ? err_pipe[1] : output_fd;

  std::vector<char*> cargv;
  cargv.reserve(argv.size() + 1);
  for (const std::string& a : argv) cargv.push_back(const_cast<char*>(a.c_str()));
  cargv.push_back(nullptr);

  const pid_t pid = ::fork();
  if (pid < 0) {
    *error = std::string("fork failed: ") + std::strerror(errno);
    int fds[] = {null_fd, out_pipe[0], out_pipe[1], err_pipe[0], err_pipe[1]};
    for (int& fd : fds) close_quietly(fd);
    return nullptr;
  }
  if (pid == 0) {
    // ---- child: async-signal-safe calls only, until execve ----
    // Own process group, so a Ctrl-C on fleetlet's terminal (delivered to
    // the foreground group) does not kill a `docker stop` halfway; the
    // agent decides when its helpers die.
    ::setpgid(0, 0);
    reset_signals_in_child();
    ::dup2(null_fd, STDIN_FILENO);
    ::dup2(child_out, STDOUT_FILENO);
    ::dup2(child_err, STDERR_FILENO);
    ::execve(path.c_str(), cargv.data(), environ);
    // The path was checked executable in the parent, so this is rare; the
    // caller sees exit code 127, the shell convention for "could not run".
    ::_exit(127);
  }

  // ---- parent ----
  close_quietly(null_fd);
  close_quietly(out_pipe[1]);
  close_quietly(err_pipe[1]);
  std::unique_ptr<Subprocess> p(new Subprocess());
  p->pid_ = pid;
  p->out_fd_ = out_pipe[0];
  p->err_fd_ = err_pipe[0];
  p->deadline_ = std::chrono::steady_clock::now() + timeout;
  return p;
}

Subprocess::~Subprocess() {
  if (pid_ > 0 && !exited_) {
    ::kill(pid_, SIGKILL);
    int status = 0;
    while (::waitpid(pid_, &status, 0) < 0 && errno == EINTR) {
    }
  }
  close_quietly(out_fd_);
  close_quietly(err_fd_);
}

void Subprocess::drain(int& fd, std::string& into) {
  char buf[4096];
  while (fd >= 0) {
    const ssize_t n = ::read(fd, buf, sizeof(buf));
    if (n > 0) {
      const size_t room = kMaxCapture > into.size() ? kMaxCapture - into.size() : 0;
      into.append(buf, std::min(room, static_cast<size_t>(n)));
      continue;
    }
    if (n < 0 && errno == EINTR) continue;
    if (n == 0) close_quietly(fd);  // EOF: the writer (the child) is gone
    return;                         // EOF, or EAGAIN: nothing more right now
  }
}

bool Subprocess::poll() {
  if (done_) return true;
  // Drain before checking for exit: a child blocked writing into a full
  // pipe never exits, so output must keep moving.
  drain(out_fd_, result_.out);
  drain(err_fd_, result_.err);
  if (!exited_) {
    int status = 0;
    pid_t r;
    do {
      r = ::waitpid(pid_, &status, WNOHANG);
    } while (r < 0 && errno == EINTR);
    if (r == pid_) {
      exited_ = true;
      record_status(status, &result_);
    } else if (!result_.timed_out && std::chrono::steady_clock::now() >= deadline_) {
      ::kill(pid_, SIGKILL);
      result_.timed_out = true;
    }
  }
  if (!exited_) return false;
  // Whatever the child wrote before exiting is still in the pipe; read it.
  // If a grandchild inherited the write end and is still alive we would
  // see EAGAIN rather than EOF; we do not wait for it (the docker CLI does
  // not leave children behind, and a helper's helper is not our output).
  drain(out_fd_, result_.out);
  drain(err_fd_, result_.err);
  close_quietly(out_fd_);
  close_quietly(err_fd_);
  done_ = true;
  return true;
}

void Subprocess::wait() {
  // A 2 ms sleep between polls instead of poll(2) on the fds: wait() is
  // only used off the hot path (startup check, kill_all), and this keeps
  // one code path for reading output and enforcing the deadline.
  while (!poll()) std::this_thread::sleep_for(std::chrono::milliseconds(2));
}

void Subprocess::kill() {
  if (pid_ > 0 && !exited_) ::kill(pid_, SIGKILL);
}

}  // namespace fleet
