// fleetlet: the node agent binary. Parses capacity and labels from flags,
// runs an Agent with a ProcessRuntime until SIGINT/SIGTERM, then kills its
// tasks and exits. All logic is in fleet_agent_lib.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "agent.h"
#include "log.h"
#include "process_runtime.h"
#include "signals.h"

namespace {

void usage() {
  std::fprintf(stderr,
               "usage: fleetlet --name NAME --cpu MILLICPU --mem MB [--master HOST:PORT]\n"
               "                [--label k=v]... [--work-dir DIR] [--stop-grace-ms N]\n"
               "                [--quiet]\n");
}

}  // namespace

int main(int argc, char** argv) {
  fleet::AgentOptions opt;
  opt.log_events = true;
  fleet::ProcessRuntimeOptions rt;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&]() -> std::string {
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

  // Before any thread exists (see common/signals.h).
  fleet::block_shutdown_signals();

  fleet::Agent agent(opt, std::make_unique<fleet::ProcessRuntime>(rt));
  agent.start_background();
  const int sig = fleet::wait_for_shutdown_signal();
  fleet::log_line("fleetlet[" + opt.name + "]", std::string(strsignal(sig)) + ", stopping");
  agent.request_stop();
  agent.join();
  return 0;
}
