// fleetmaster: the control plane binary. Parses flags, starts a
// MasterServer, and shuts it down cleanly on SIGINT or SIGTERM. All logic
// is in fleet_master_lib (master_core, master_server) so the integration
// tests run exactly the same code in-process.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "log.h"
#include "master_server.h"
#include "signals.h"

namespace {

void usage() {
  std::fprintf(stderr,
               "usage: fleetmaster [--listen HOST:PORT] [--schedule-ms N]\n"
               "                   [--heartbeat-ms N] [--strategy best-fit|worst-fit|first-fit]\n"
               "                   [--no-preempt] [--quiet]\n");
}

}  // namespace

int main(int argc, char** argv) {
  fleet::MasterServerOptions opt;
  opt.core.log_events = true;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    auto next = [&]() -> std::string {
      if (i + 1 >= argc) {
        usage();
        std::exit(2);
      }
      return argv[++i];
    };
    if (a == "--listen") {
      opt.listen_address = next();
    } else if (a == "--schedule-ms") {
      opt.schedule_interval = std::chrono::milliseconds(std::stoi(next()));
    } else if (a == "--heartbeat-ms") {
      opt.core.heartbeat_interval_ms = static_cast<uint32_t>(std::stoul(next()));
    } else if (a == "--strategy") {
      auto s = fleet::parse_strategy(next());
      if (!s) {
        usage();
        return 2;
      }
      opt.core.scheduler.strategy = *s;
    } else if (a == "--no-preempt") {
      opt.core.scheduler.enable_preemption = false;
    } else if (a == "--quiet") {
      opt.core.log_events = false;
    } else {
      usage();
      return 2;
    }
  }

  // Block SIGINT/SIGTERM before any thread exists so every thread
  // inherits the mask, then wait for them synchronously below.
  fleet::block_shutdown_signals();

  fleet::MasterServer server(opt);
  std::string err;
  if (!server.start(&err)) {
    std::fprintf(stderr, "fleetmaster: %s\n", err.c_str());
    return 1;
  }
  fleet::log_line("fleetmaster", "listening on port " + std::to_string(server.port()));
  const int sig = fleet::wait_for_shutdown_signal();
  fleet::log_line("fleetmaster", std::string(strsignal(sig)) + ", shutting down");
  server.shutdown();
  return 0;
}
