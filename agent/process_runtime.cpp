// ProcessRuntime implementation. The delicate part is start(): fleetlet is
// multithreaded (gRPC runs its own threads), and after fork() in a
// multithreaded process the child may only call async-signal-safe
// functions until it execs: no malloc, no locks, no C++ strings, no
// printf. So everything the child needs (argv as char*, the resolved
// path, open file descriptors, the rlimit value, the warning text) is
// prepared in the parent, and the child only makes system calls.

#include "process_runtime.h"

#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

extern char** environ;

namespace fleet {
namespace {

// argv[0] as given if it contains '/', else the first executable regular
// file of that name on PATH. Done in the parent, so the child can use
// execve (async-signal-safe) instead of execvp (which searches PATH and is
// not guaranteed to be).
std::string resolve_command(const std::string& cmd) {
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

void close_quietly(int fd) {
  if (fd >= 0) ::close(fd);
}

void write_all(int fd, const char* s, size_t n) {
  while (n > 0) {
    ssize_t w = ::write(fd, s, n);
    if (w < 0 && errno == EINTR) continue;
    if (w <= 0) return;
    s += w;
    n -= static_cast<size_t>(w);
  }
}

constexpr char kNoMemLimit[] =
    "fleet: setrlimit(RLIMIT_AS) refused; memory limit NOT enforced on this "
    "OS (macOS rejects any limit below its large default VM reservation)\n";

}  // namespace

ProcessRuntime::ProcessRuntime(ProcessRuntimeOptions options) : options_(options) {}

ProcessRuntime::~ProcessRuntime() { kill_all(); }

StartResult ProcessRuntime::start(const LaunchSpec& spec) {
  StartResult r;
  if (spec.argv.empty()) {
    r.error = "empty argv";
    return r;
  }
  const std::string path = resolve_command(spec.argv[0]);
  if (path.empty()) {
    r.error = "command not found: " + spec.argv[0];
    return r;
  }

  // O_CLOEXEC on everything the parent opens: these fds must not leak into
  // other tasks forked later. The child dup2()s what it needs onto 0/1/2,
  // which clears the flag on the copies.
  const int log_fd = ::open(spec.log_path.c_str(),
                            O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (log_fd < 0) {
    r.error = "cannot open log " + spec.log_path.string() + ": " + std::strerror(errno);
    return r;
  }
  const int null_fd = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
  // The exec-error pipe: the child writes errno into it only if execve
  // fails. On success execve closes the write end (FD_CLOEXEC) and the
  // parent's read returns 0. That turns "exec failed" into an error from
  // start() instead of a mysterious exit code 127 a heartbeat later.
  // macOS has no pipe2(), so the flag is set right after. Another thread
  // forking in between could inherit these two fds; no other fleetlet
  // thread forks, so that window is harmless here.
  int err_pipe[2] = {-1, -1};
  if (null_fd < 0 || ::pipe(err_pipe) != 0) {
    r.error = std::string("pipe/open failed: ") + std::strerror(errno);
    close_quietly(log_fd);
    close_quietly(null_fd);
    return r;
  }
  ::fcntl(err_pipe[0], F_SETFD, FD_CLOEXEC);
  ::fcntl(err_pipe[1], F_SETFD, FD_CLOEXEC);

  std::vector<char*> cargv;
  cargv.reserve(spec.argv.size() + 1);
  for (const std::string& a : spec.argv) cargv.push_back(const_cast<char*>(a.c_str()));
  cargv.push_back(nullptr);
  struct rlimit mem_limit {};
  mem_limit.rlim_cur = mem_limit.rlim_max =
      static_cast<rlim_t>(spec.limits.memory_mb) * 1024 * 1024;
  struct rlimit no_core {};
  const bool limit_memory = options_.limit_memory && spec.limits.memory_mb > 0;

  const pid_t pid = ::fork();
  if (pid < 0) {
    r.error = std::string("fork failed: ") + std::strerror(errno);
    close_quietly(log_fd);
    close_quietly(null_fd);
    close_quietly(err_pipe[0]);
    close_quietly(err_pipe[1]);
    return r;
  }
  if (pid == 0) {
    // ---- child: async-signal-safe calls only, until execve ----
    ::setpgid(0, 0);  // own process group, id == our pid
    // fleetlet blocks SIGINT/SIGTERM in all its threads (common/signals.h)
    // and the mask survives fork and exec, so without this a task could
    // not be stopped with SIGTERM. Also reset dispositions to default:
    // an ignored signal (SIG_IGN, e.g. SIGPIPE) stays ignored across exec.
    sigset_t none;
    sigemptyset(&none);
    ::sigprocmask(SIG_SETMASK, &none, nullptr);
    struct sigaction dfl {};
    dfl.sa_handler = SIG_DFL;
    for (int s = 1; s < NSIG; ++s) {
      if (s != SIGKILL && s != SIGSTOP) ::sigaction(s, &dfl, nullptr);
    }
    ::dup2(null_fd, STDIN_FILENO);
    ::dup2(log_fd, STDOUT_FILENO);
    ::dup2(log_fd, STDERR_FILENO);
    ::setrlimit(RLIMIT_CORE, &no_core);
    if (limit_memory && ::setrlimit(RLIMIT_AS, &mem_limit) != 0) {
      write_all(STDERR_FILENO, kNoMemLimit, sizeof(kNoMemLimit) - 1);
    }
    ::execve(path.c_str(), cargv.data(), environ);
    const int e = errno;
    write_all(err_pipe[1], reinterpret_cast<const char*>(&e), sizeof(e));
    ::_exit(127);
  }

  // ---- parent ----
  // Also set the group from this side. Whichever of parent and child runs
  // first, the group exists before start() returns, so a stop() right
  // after start() cannot miss it. EACCES (the child already exec'd) is
  // fine: the child's own setpgid ran before its exec.
  ::setpgid(pid, pid);
  close_quietly(err_pipe[1]);
  close_quietly(log_fd);
  close_quietly(null_fd);
  int child_errno = 0;
  ssize_t n;
  do {
    n = ::read(err_pipe[0], &child_errno, sizeof(child_errno));
  } while (n < 0 && errno == EINTR);
  close_quietly(err_pipe[0]);
  if (n == static_cast<ssize_t>(sizeof(child_errno))) {
    int status = 0;
    while (::waitpid(pid, &status, 0) < 0 && errno == EINTR) {
    }
    r.error = "exec " + path + ": " + std::strerror(child_errno);
    return r;
  }

  const RuntimeHandle h = next_handle_++;
  procs_[h] = Proc{pid, false, {}};
  r.ok = true;
  r.handle = h;
  return r;
}

void ProcessRuntime::stop(RuntimeHandle handle) {
  auto it = procs_.find(handle);
  if (it == procs_.end() || it->second.stopping) return;
  // Negative pid = the whole process group.
  ::kill(-it->second.pid, SIGTERM);
  it->second.stopping = true;
  it->second.kill_at = std::chrono::steady_clock::now() + options_.stop_grace;
}

bool ProcessRuntime::try_collect(RuntimeHandle h, Proc& p, bool block, ExitInfo* out) {
  // Peek first with WNOWAIT, which leaves the exited child a zombie. While
  // it is a zombie its pid (= its group id) cannot be reused, so the group
  // kill below cannot hit an unrelated process. Reaping first and then
  // killing -pid would race with pid reuse.
  siginfo_t si {};
  const int flags = WEXITED | WNOWAIT | (block ? 0 : WNOHANG);
  int rc;
  do {
    rc = ::waitid(P_PID, static_cast<id_t>(p.pid), &si, flags);
  } while (rc < 0 && errno == EINTR);
  if (rc < 0 || si.si_pid == 0) return false;  // still running
  // A task is its process group: when the leader exits, anything it left
  // behind in the group goes too, so nothing outlives its accounting.
  ::kill(-p.pid, SIGKILL);
  int status = 0;
  while (::waitpid(p.pid, &status, 0) < 0 && errno == EINTR) {
  }
  out->handle = h;
  if (WIFSIGNALED(status)) {
    out->signal = WTERMSIG(status);
  } else {
    out->exit_code = WEXITSTATUS(status);
  }
  return true;
}

std::vector<ExitInfo> ProcessRuntime::reap() {
  std::vector<ExitInfo> exits;
  const auto now = std::chrono::steady_clock::now();
  for (auto it = procs_.begin(); it != procs_.end();) {
    ExitInfo e;
    if (try_collect(it->first, it->second, false, &e)) {
      exits.push_back(e);
      it = procs_.erase(it);
      continue;
    }
    // Grace period over: escalate. SIGKILL cannot be caught or ignored.
    if (it->second.stopping && now >= it->second.kill_at) {
      ::kill(-it->second.pid, SIGKILL);
    }
    ++it;
  }
  return exits;
}

void ProcessRuntime::kill_all() {
  for (auto& [h, p] : procs_) ::kill(-p.pid, SIGKILL);
  for (auto& [h, p] : procs_) {
    ExitInfo ignored;
    try_collect(h, p, true, &ignored);
  }
  procs_.clear();
}

pid_t ProcessRuntime::pid_of(RuntimeHandle handle) const {
  auto it = procs_.find(handle);
  return it == procs_.end() ? -1 : it->second.pid;
}

}  // namespace fleet
