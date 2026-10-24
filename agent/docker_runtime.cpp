// DockerRuntime implementation: a small state machine per container
// (starting -> running -> collecting -> done), advanced from reap() by
// polling docker CLI subprocesses. See docker_runtime.h for the model and
// the reasons behind each docker flag.

#include "docker_runtime.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sstream>
#include <thread>
#include <unistd.h>

#include "model.h"

namespace fleet {
namespace {

using Clock = std::chrono::steady_clock;

// Go template for docker inspect: four space-free tokens per container,
// e.g. "/fleet-3-1-9f2c4a1b exited 137 true". Docker does the field
// extraction, so we parse a line, not JSON (fleet has no JSON library).
// Status values (created, running, paused, restarting, removing, exited,
// dead) never contain spaces, and names cannot.
constexpr char kInspectFormat[] =
    "{{.Name}} {{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}}";

// The first non-empty line of docker's stderr, trimmed and capped. Docker
// puts the useful part first ("docker: Error response from daemon: ...")
// and a generic "Run 'docker run --help'" hint after it.
std::string first_line(const std::string& text) {
  std::istringstream in(text);
  std::string line;
  while (std::getline(in, line)) {
    const auto b = line.find_first_not_of(" \t\r");
    if (b == std::string::npos) continue;
    const auto e = line.find_last_not_of(" \t\r");
    line = line.substr(b, e - b + 1);
    if (line.size() > 300) line.resize(300);
    return line;
  }
  return {};
}

std::string lower(std::string s) {
  for (char& c : s) c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
  return s;
}

int64_t ceil_seconds(std::chrono::milliseconds ms) {
  return ms.count() <= 0 ? 0 : (ms.count() + 999) / 1000;
}

}  // namespace

std::string format_cpus(int64_t milli_cpu) {
  // Integer formatting rather than printf("%g", m / 1000.0): exact for
  // every value, and no locale can turn the '.' into a ','.
  std::string out = std::to_string(milli_cpu / 1000);
  int64_t frac = milli_cpu % 1000;
  if (frac == 0) return out;
  std::string digits = std::to_string(frac);
  digits.insert(0, 3 - digits.size(), '0');  // 5 -> "005"
  while (digits.back() == '0') digits.pop_back();
  return out + "." + digits;
}

std::vector<std::string> docker_run_argv(const DockerRuntimeOptions& options,
                                         const LaunchSpec& spec) {
  std::vector<std::string> a = {options.docker, "run", "-d", "--name", spec.name, "--init"};
  // A zero request means "not limited" in that dimension, as in Docker
  // itself. The master never sends one (ClusterState::submit rejects
  // non-positive requests); this only matters for direct callers. Values
  // Docker refuses (under 0.01 CPUs or 6 MB) fail the run, and its error
  // text becomes the start failure's message.
  if (spec.limits.milli_cpu > 0) a.push_back("--cpus=" + format_cpus(spec.limits.milli_cpu));
  if (spec.limits.memory_mb > 0) {
    const std::string mem = std::to_string(spec.limits.memory_mb) + "m";
    a.push_back("--memory=" + mem);
    a.push_back("--memory-swap=" + mem);  // = memory: no swap beyond the limit
  }
  a.push_back(spec.image.empty() ? options.default_image : spec.image);
  // Everything after the image is the container's command. docker run
  // stops parsing its own flags at the image, so the task's argv is passed
  // through untouched, dashes and all.
  a.insert(a.end(), spec.argv.begin(), spec.argv.end());
  return a;
}

DockerProbe probe_docker(const std::string& docker, std::chrono::milliseconds timeout) {
  DockerProbe p;
  std::string err;
  auto cmd = Subprocess::spawn({docker, "version", "--format", "{{.Server.Version}}"},
                               timeout, &err);
  if (!cmd) {
    p.detail = err;
    return p;
  }
  cmd->wait();
  const CommandResult& r = cmd->result();
  if (r.timed_out) {
    p.detail = "docker version timed out after " + std::to_string(timeout.count()) +
               " ms (is the Docker VM running?)";
  } else if (!r.ok() || first_line(r.out).empty()) {
    const std::string why = first_line(r.err);
    p.detail = why.empty() ? "docker version exited " + std::to_string(r.exit_code) : why;
  } else {
    p.available = true;
    p.detail = first_line(r.out);
  }
  return p;
}

DockerRuntime::DockerRuntime(DockerRuntimeOptions options) : options_(std::move(options)) {}

DockerRuntime::~DockerRuntime() { kill_all(); }

StartResult DockerRuntime::start(const LaunchSpec& spec) {
  StartResult r;
  if (spec.argv.empty()) {
    r.error = "empty argv";
    return r;
  }
  if (spec.name.empty()) {
    r.error = "container name required";
    return r;
  }
  const std::string& image = spec.image.empty() ? options_.default_image : spec.image;
  if (!valid_image_ref(image)) {
    r.error = "invalid image reference: '" + image + "'";
    return r;
  }
  std::string err;
  auto cmd = Subprocess::spawn(docker_run_argv(options_, spec), options_.run_timeout, &err);
  if (!cmd) {
    r.error = "docker: " + err;
    return r;
  }
  const RuntimeHandle h = next_handle_++;
  Container& c = containers_[h];
  c.name = spec.name;
  c.log_path = spec.log_path;
  c.memory_mb = spec.limits.memory_mb;
  c.cmd = std::move(cmd);
  c.exit.handle = h;
  r.ok = true;
  r.handle = h;
  return r;
}

void DockerRuntime::stop(RuntimeHandle handle) {
  auto it = containers_.find(handle);
  if (it == containers_.end() || it->second.stop_wanted) return;
  Container& c = it->second;
  c.stop_wanted = true;
  // Still starting: the stop is sent the moment docker run returns (see
  // advance), because there is nothing to stop yet. Already exited: there
  // is nothing to stop at all.
  if (c.phase == Phase::kRunning) send_stop(c);
}

void DockerRuntime::send_stop(Container& c) {
  if (c.stop_cmd) return;
  std::string err;
  // The CLI's own deadline is the grace plus our usual command timeout:
  // docker stop legitimately takes up to the grace before it SIGKILLs.
  c.stop_cmd = Subprocess::spawn(
      {options_.docker, "stop", "-t", std::to_string(ceil_seconds(options_.stop_grace)), c.name},
      options_.stop_grace + options_.command_timeout, &err);
  // If spawning failed, stop_cmd stays null and the next inspect that
  // still sees the container running sends the stop again.
  if (!c.stop_cmd) c.stop_done_at = Clock::now();
}

void DockerRuntime::background(std::vector<std::string> argv) {
  std::string err;
  auto cmd = Subprocess::spawn(argv, options_.command_timeout, &err);
  if (cmd) background_.push_back(std::move(cmd));
}

void DockerRuntime::begin_collect(Container& c) {
  // An in-flight docker stop is moot once the container has exited; let
  // it finish in the background rather than block or leave a zombie.
  if (c.stop_cmd) background_.push_back(std::move(c.stop_cmd));
  // Copy the container's output into the task's log file before reporting
  // the exit, so "exited" means the log is complete (ProcessRuntime gives
  // the same guarantee by writing the log directly). docker logs writes
  // the container's stdout and stderr to its own stdout and stderr; both
  // go to the file, so their relative order is only approximately kept.
  const int fd = ::open(c.log_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  if (fd >= 0) {
    std::string err;
    c.cmd = Subprocess::spawn({options_.docker, "logs", c.name}, options_.command_timeout,
                              &err, fd);
    ::close(fd);  // the child has its own copy
  }
  c.phase = c.cmd ? Phase::kCollecting : Phase::kDone;
}

bool DockerRuntime::advance(Container& c) {
  switch (c.phase) {
    case Phase::kStarting: {
      if (!c.cmd->poll()) return false;
      const CommandResult r = c.cmd->result();
      c.cmd.reset();
      if (r.ok()) {
        c.phase = Phase::kRunning;
        if (c.stop_wanted) send_stop(c);
        return false;
      }
      // docker run failed: bad image, pull denied, command not found in
      // the image, daemon down. Docker may have created the container
      // before failing (an exec failure leaves it in "created"), so remove
      // it by name either way.
      c.exit.start_failed = true;
      c.exit.exit_code = r.exit_code;
      c.exit.message =
          r.timed_out ? "docker run timed out after " +
                            std::to_string(options_.run_timeout.count()) + " ms"
                      : "docker run: " + first_line(r.err);
      background({options_.docker, "rm", "-f", c.name});
      return true;
    }
    case Phase::kRunning:
      // The batched inspect notices the exit. Here we only reap a finished
      // docker stop; whether it worked shows up in the next inspect.
      if (c.stop_cmd && c.stop_cmd->poll()) {
        c.stop_cmd.reset();
        c.stop_done_at = Clock::now();
      }
      return false;
    case Phase::kCollecting:
      if (!c.cmd->poll()) return false;
      c.cmd.reset();
      c.phase = Phase::kDone;
      [[fallthrough]];
    case Phase::kDone:
      background({options_.docker, "rm", c.name});
      return true;
  }
  return false;
}

void DockerRuntime::apply_inspect(const CommandResult& r) {
  // Parse "/name status code oom" lines into name -> tokens. docker
  // inspect prints the containers it found even when it fails on others
  // (exit 1 with "no such object" on stderr), so stdout is used whatever
  // the exit code.
  struct Seen {
    std::string status;
    int code = 0;
    bool oom = false;
  };
  std::map<std::string, Seen> seen;
  std::istringstream in(r.out);
  std::string line;
  while (std::getline(in, line)) {
    std::istringstream f(line);
    std::string name, status, code, oom;
    if (!(f >> name >> status >> code >> oom)) continue;
    if (!name.empty() && name[0] == '/') name.erase(0, 1);
    Seen s;
    s.status = status;
    s.oom = oom == "true";
    try {
      s.code = std::stoi(code);
    } catch (const std::exception&) {
      continue;  // not a line we understand; treat the container as unseen
    }
    seen[name] = s;
  }
  const std::string err = lower(r.err);
  for (RuntimeHandle h : inspect_handles_) {
    auto it = containers_.find(h);
    if (it == containers_.end() || it->second.phase != Phase::kRunning) continue;
    Container& c = it->second;
    auto s = seen.find(c.name);
    if (s == seen.end()) {
      // Absent from the output. Only "no such object: NAME" means the
      // container is really gone (someone ran docker rm behind our back);
      // anything else (daemon restarting, the inspect timed out) means we
      // simply did not learn anything this round, and the next poll asks
      // again.
      if (err.find("no such object: " + lower(c.name)) != std::string::npos) {
        c.exit.exit_code = -1;
        c.exit.message = "container " + c.name + " vanished (removed outside fleet)";
        c.phase = Phase::kDone;
      }
      continue;
    }
    if (s->second.status == "exited" || s->second.status == "dead") {
      c.exit.exit_code = s->second.code;
      c.exit.oom_killed = s->second.oom;
      if (s->second.oom) {
        c.exit.message = "OOM-killed: exceeded its " + std::to_string(c.memory_mb) +
                         " MB memory limit";
      }
      begin_collect(c);
    } else if (c.stop_wanted && !c.stop_cmd && c.stop_done_at < inspect_started_at_) {
      // Still up, according to an inspect that began after our last
      // docker stop had finished: that stop failed (daemon hiccup, or it
      // could not be spawned). Ask again; this is the retry, at the poll
      // cadence. An inspect that began earlier proves nothing, since it
      // may have looked before the stop took effect.
      send_stop(c);
    }
  }
}

void DockerRuntime::maybe_start_inspect(Clock::time_point now) {
  if (inspect_ || now < next_inspect_) return;
  std::vector<std::string> argv = {options_.docker, "inspect", "--format", kInspectFormat};
  inspect_handles_.clear();
  for (const auto& [h, c] : containers_) {
    if (c.phase != Phase::kRunning) continue;
    argv.push_back(c.name);
    inspect_handles_.push_back(h);
  }
  if (inspect_handles_.empty()) return;
  // One inspect for all running containers: the cost is one fork/exec
  // per poll_interval per node, not per container.
  std::string err;
  inspect_ = Subprocess::spawn(argv, options_.command_timeout, &err);
  inspect_started_at_ = now;
  next_inspect_ = now + options_.poll_interval;
}

std::vector<ExitInfo> DockerRuntime::reap() {
  std::vector<ExitInfo> exits;
  if (inspect_ && inspect_->poll()) {
    apply_inspect(inspect_->result());
    inspect_.reset();
  }
  for (auto it = containers_.begin(); it != containers_.end();) {
    if (advance(it->second)) {
      exits.push_back(std::move(it->second.exit));
      it = containers_.erase(it);
    } else {
      ++it;
    }
  }
  maybe_start_inspect(Clock::now());
  std::erase_if(background_, [](const std::unique_ptr<Subprocess>& p) { return p->poll(); });
  return exits;
}

void DockerRuntime::kill_all() {
  // 1. Give in-flight `docker run`s a bounded chance to finish. Killing
  //    the CLI between its "create" and "start" API calls leaves a created
  //    container behind, and killing it mid-"create" can let the daemon
  //    finish creating after our rm below. Waiting closes most of that
  //    window; a run still going after shutdown_wait (a slow pull) is
  //    killed and may leave one created, never-started container.
  const auto deadline = Clock::now() + options_.shutdown_wait;
  auto starting = [&] {
    for (auto& [h, c] : containers_) {
      if (c.phase == Phase::kStarting && c.cmd && !c.cmd->poll()) return true;
    }
    return false;
  };
  while (starting() && Clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  // 2. Every other helper is moot: destroying a Subprocess kills and reaps.
  inspect_.reset();
  std::vector<std::string> argv = {options_.docker, "rm", "-f"};
  for (auto& [h, c] : containers_) {
    c.cmd.reset();
    c.stop_cmd.reset();
    argv.push_back(c.name);
  }
  // 3. rm -f kills (SIGKILL) and removes in one call, for all of them.
  if (argv.size() > 3) {
    std::string err;
    if (auto rm = Subprocess::spawn(argv, options_.command_timeout, &err)) rm->wait();
  }
  containers_.clear();
  // 4. Let earlier background removals finish, with their own budget.
  const auto rm_deadline = Clock::now() + options_.shutdown_wait;
  while (!background_.empty() && Clock::now() < rm_deadline) {
    std::erase_if(background_, [](const std::unique_ptr<Subprocess>& p) { return p->poll(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  background_.clear();
}

}  // namespace fleet
