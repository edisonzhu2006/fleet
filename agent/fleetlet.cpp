// fleetlet: the node agent binary. Parses capacity and labels from flags,
// picks a runtime (--runtime=process, the default, or --runtime=docker),
// runs an Agent until SIGINT/SIGTERM, then kills its tasks and exits. All
// logic is in fleet_agent_lib.
//
// With --runtime=docker, fleetlet checks at startup that a Docker daemon
// answers (`docker version`, 5 s deadline) and exits with an error if not,
// rather than registering a node on which every start would fail.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "agent.h"
#include "docker_runtime.h"
#include "log.h"
#include "process_runtime.h"
#include "signals.h"

namespace {

void usage() {
  std::fprintf(stderr,
               "usage: fleetlet --name NAME --cpu MILLICPU --mem MB [--master HOST:PORT]\n"
               "                [--label k=v]... [--work-dir DIR] [--stop-grace-ms N]\n"
               "                [--runtime process|docker] [--docker-image IMAGE]\n"
               "                [--docker CLI] [--docker-poll-ms N] [--event-log FILE] [--quiet]\n"
               "  flags also accept --flag=value\n");
}

}  // namespace

int main(int argc, char** argv) {
  fleet::AgentOptions opt;
  opt.log_events = true;
  fleet::ProcessRuntimeOptions rt;
  fleet::DockerRuntimeOptions dk;
  std::string runtime = "process";
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    // --flag=value is the same as --flag value.
    std::string inline_value;
    bool has_inline = false;
    if (a.rfind("--", 0) == 0 && a.find('=') != std::string::npos) {
      inline_value = a.substr(a.find('=') + 1);
      a = a.substr(0, a.find('='));
      has_inline = true;
    }
    auto next = [&]() -> std::string {
      if (has_inline) return inline_value;
      if (i + 1 >= argc) {
        usage();
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--master") {
      opt.master_address = next();
    } else if (a == "--name") {
      opt.name = next();
    } else if (a == "--cpu") {
      opt.capacity.milli_cpu = std::stoll(next());
    } else if (a == "--mem") {
      opt.capacity.memory_mb = std::stoll(next());
    } else if (a == "--label") {
      const std::string kv = next();
      const size_t eq = kv.find('=');
      if (eq == std::string::npos) {
        usage();
        return 2;
      }
      opt.labels[kv.substr(0, eq)] = kv.substr(eq + 1);
    } else if (a == "--work-dir") {
      opt.work_dir = next();
    } else if (a == "--stop-grace-ms") {
      rt.stop_grace = std::chrono::milliseconds(std::stoi(next()));
      dk.stop_grace = rt.stop_grace;
    } else if (a == "--runtime") {
      runtime = next();
    } else if (a == "--docker-image") {
      dk.default_image = next();
    } else if (a == "--docker") {
      dk.docker = next();
    } else if (a == "--docker-poll-ms") {
      dk.poll_interval = std::chrono::milliseconds(std::stoi(next()));
    } else if (a == "--event-log") {
      opt.event_log = next();
    } else if (a == "--quiet") {
      opt.log_events = false;
    } else {
      usage();
      return 2;
    }
  }
  if (opt.name.empty() || opt.capacity.milli_cpu <= 0 || opt.capacity.memory_mb <= 0) {
    usage();
    return 2;
  }
  if (opt.work_dir.empty()) opt.work_dir = "/private/tmp/fleet/" + opt.name;
  // Leftover tracking (m3): ProcessRuntime records task pids under the
  // node's own work dir; DockerRuntime labels containers with the node
  // name. Either way a restarted fleetlet finds what a crashed one left.
  rt.state_dir = opt.work_dir / "pids";
  dk.owner = opt.name;
  if (runtime != "process" && runtime != "docker") {
    usage();
    return 2;
  }

  const std::string tag = "fleetlet[" + opt.name + "]";
  std::unique_ptr<fleet::Runtime> rt_impl;
  if (runtime == "docker") {
    if (!fleet::valid_image_ref(dk.default_image)) {
      std::fprintf(stderr, "fleetlet: invalid --docker-image '%s'\n", dk.default_image.c_str());
      return 2;
    }
    // Before any thread exists, like the signal setup below: probe_docker
    // forks, and a fork with no other threads has no hazards at all.
    const fleet::DockerProbe probe = fleet::probe_docker(dk.docker, std::chrono::seconds(5));
    if (!probe.available) {
      std::fprintf(stderr, "fleetlet: --runtime=docker but Docker is not available: %s\n",
                   probe.detail.c_str());
      return 1;
    }
    fleet::log_line(tag, "runtime docker (server " + probe.detail + "), default image " +
                             dk.default_image);
    rt_impl = std::make_unique<fleet::DockerRuntime>(dk);
  } else {
    fleet::log_line(tag, "runtime process");
    rt_impl = std::make_unique<fleet::ProcessRuntime>(rt);
  }

  // Before any thread exists (see common/signals.h).
  fleet::block_shutdown_signals();

  fleet::Agent agent(opt, std::move(rt_impl));
  agent.start_background();
  const int sig = fleet::wait_for_shutdown_signal();
  fleet::log_line(tag, std::string(strsignal(sig)) + ", stopping");
  agent.request_stop();
  agent.join();
  return 0;
}
