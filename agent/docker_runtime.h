#pragma once
// DockerRuntime: tasks as Docker containers with cgroup CPU and memory
// limits (m2b).
//
// WHY THIS EXISTS. macOS has no cgroups and refuses any useful
// RLIMIT_AS (NOTES m2), so ProcessRuntime cannot enforce a task's memory
// or CPU. Docker runs a Linux VM on macOS (Docker Desktop, OrbStack), and
// inside it every container gets a cgroup: `--memory` is a hard limit the
// kernel enforces by OOM-killing the container, and `--cpus` is a CFS
// quota that throttles the container to its share. That is the same
// mechanism Borg's Borglet and Kubernetes' kubelet use.
//
// HOW. Each task is one container, driven entirely through the docker CLI
// (fork/exec of an argv array, never a shell, so no quoting or injection
// questions):
//   start   docker run -d --name NAME --init --cpus=C --memory=Mm
//                      --memory-swap=Mm IMAGE CMD...
//   poll    docker inspect --format '{{.Name}} {{.State.Status}}
//                      {{.State.ExitCode}} {{.State.OOMKilled}}' NAME...
//           one call covers every running container, every poll_interval
//   exit    docker logs NAME  > the task's log file, then docker rm NAME
//   stop    docker stop -t GRACE NAME  (SIGTERM, then SIGKILL after GRACE)
// Every CLI call is a Subprocess polled from reap(), so the Runtime
// contract stays non-blocking even though `docker run` takes 100+ ms
// (seconds with an image pull). A start that docker later rejects comes
// back from reap() with start_failed set.
//
// WHY THE CLI, NOT THE ENGINE API. The Engine API is HTTP over the unix
// socket /var/run/docker.sock and would avoid a fork per call (~40 ms per
// `docker inspect` measured here). But it needs an HTTP/1.1 client and a
// JSON parser, and fleet takes no dependencies beyond gRPC and Protobuf.
// The CLI already speaks the API, handles contexts (OrbStack's socket is
// not at the default path) and gives us one line per container through a
// Go template. The API over the socket, with the /events stream instead of
// polling, is the next step if per-call cost matters (NOTES m2b).
//
// FLAGS BEYOND THE PLAIN `docker run --cpus --memory`, each on purpose:
//   --init               tini as PID 1. The kernel does not deliver a
//                        default-action SIGTERM to PID 1 of a PID
//                        namespace, so without it `sh -c 'sleep 600'`
//                        ignores docker stop and is SIGKILLed only after
//                        the full grace; tini forwards the signal and
//                        reaps zombies, matching ProcessRuntime's
//                        process-group stop.
//   --memory-swap=M      equal to --memory, so the limit covers RAM plus
//                        swap. Docker's default lets a container swap as
//                        much again as its limit, which turns an OOM kill
//                        into a slow swap storm on a host with swap.
//   no --rm              the exit code and OOMKilled flag live on the
//                        stopped container; --rm would delete them before
//                        we inspect. We rm ourselves after reading them.
//
// EXIT CODES are Docker's, passed through: a container killed by a signal
// shows 128 + signal (137 = SIGKILL, 143 = SIGTERM), and signal stays 0,
// because Docker reports one integer and an exit(137) looks the same.
// oom_killed comes from Docker's own State.OOMKilled, not from guessing at
// 137.

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "runtime.h"
#include "subprocess.h"

namespace fleet {

struct DockerRuntimeOptions {
  std::string docker = "docker";  // CLI name (looked up on PATH) or path
  // Used when a task names no image. A pinned tag, not :latest, so the
  // same task runs the same bits next month. busybox has sh, sleep, dd,
  // head and yes in ~4 MB.
  std::string default_image = "busybox:1.36";
  // docker stop -t, rounded up to whole seconds (the CLI takes seconds).
  std::chrono::milliseconds stop_grace{2000};
  // How often one batched docker inspect checks every running container.
  // This bounds exit-detection latency; each poll is one fork/exec.
  std::chrono::milliseconds poll_interval{250};
  // docker run may pull the image first, so it gets a long deadline.
  std::chrono::milliseconds run_timeout{120000};
  // inspect, logs, rm.
  std::chrono::milliseconds command_timeout{30000};
  // kill_all(): how long an in-flight docker run may finish before it is
  // killed (see kill_all for why it is not killed at once).
  std::chrono::milliseconds shutdown_wait{5000};
  // The node that owns these containers (m3). When set, every container
  // gets the label fleet.owner=<owner>, and kill_leftovers() removes all
  // containers carrying it: what an earlier, crashed fleetlet of the same
  // node left behind. A label and not the fleet- name prefix, because
  // several nodes can share one Docker daemon (the bench and tests do) and
  // must not remove each other's containers. fleetlet passes its --name.
  std::string owner;
};

// Docker's --cpus value for a milli-CPU request, exactly, with no
// floating point: 500 -> "0.5", 1500 -> "1.5", 2000 -> "2", 1 -> "0.001".
std::string format_cpus(int64_t milli_cpu);

// The full `docker run` argv for a launch. Public for tests and so the
// exact command line can be shown in docs.
std::vector<std::string> docker_run_argv(const DockerRuntimeOptions& options,
                                         const LaunchSpec& spec);

// Is a Docker daemon reachable? Runs `docker version` with a deadline
// (a CLI that is installed but whose daemon/VM is down hangs or fails).
// detail is the server version when available, else why not.
struct DockerProbe {
  bool available = false;
  std::string detail;
};
DockerProbe probe_docker(const std::string& docker, std::chrono::milliseconds timeout);

class DockerRuntime final : public Runtime {
 public:
  explicit DockerRuntime(DockerRuntimeOptions options = {});
  ~DockerRuntime() override;  // kill_all()

  // Fails synchronously only for problems visible without Docker: no name,
  // an invalid image reference, the CLI not found. Everything Docker
  // itself rejects arrives later from reap() with start_failed.
  StartResult start(const LaunchSpec& spec) override;
  void stop(RuntimeHandle handle) override;
  std::vector<ExitInfo> reap() override;
  // Removes every container this runtime started (docker rm -f) and waits.
  void kill_all() override;
  size_t kill_leftovers() override;
  size_t live_count() const override { return containers_.size(); }

 private:
  enum class Phase {
    kStarting,    // `docker run -d` in flight
    kRunning,     // up; watched by the batched inspect
    kCollecting,  // exited; `docker logs` copying output to the log file
    kDone,        // exit known and logs saved: report on this reap()
  };
  struct Container {
    std::string name;
    std::filesystem::path log_path;
    int64_t memory_mb = 0;
    Phase phase = Phase::kStarting;
    std::unique_ptr<Subprocess> cmd;       // run, then logs
    std::unique_ptr<Subprocess> stop_cmd;  // docker stop in flight
    bool stop_wanted = false;
    // When the last docker stop finished (or failed to spawn). A stop is
    // retried only if an inspect that *started after* this still sees the
    // container running; an older inspect may simply predate the stop.
    std::chrono::steady_clock::time_point stop_done_at{};
    ExitInfo exit;
  };

  // Moves one container along its phases; true when its exit is ready.
  bool advance(Container& c);
  void apply_inspect(const CommandResult& r);
  void maybe_start_inspect(std::chrono::steady_clock::time_point now);
  void begin_collect(Container& c);
  void send_stop(Container& c);
  // Fire-and-forget docker command (rm): kept only so it gets reaped.
  void background(std::vector<std::string> argv);

  DockerRuntimeOptions options_;
  std::map<RuntimeHandle, Container> containers_;
  RuntimeHandle next_handle_ = 1;
  // The batched inspect in flight and the containers it covers.
  std::unique_ptr<Subprocess> inspect_;
  std::vector<RuntimeHandle> inspect_handles_;
  std::chrono::steady_clock::time_point inspect_started_at_{};
  std::chrono::steady_clock::time_point next_inspect_{};
  // Commands whose result nobody needs any more (rm, a stop that was
  // overtaken by the exit). Polled until they finish so none is a zombie.
  std::vector<std::unique_ptr<Subprocess>> background_;
};

}  // namespace fleet
