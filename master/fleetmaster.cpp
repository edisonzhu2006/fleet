// fleetmaster: the control plane binary. Parses flags, starts a
// MasterServer, and shuts it down cleanly on SIGINT or SIGTERM. All logic
// is in fleet_master_lib (master_core, master_server, failure_detector) so
// the integration tests run exactly the same code in-process.

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <string>

#include "log.h"
#include "master_server.h"
#include "signals.h"

namespace {

void usage() {
  std::fprintf(stderr,
               "usage: fleetmaster [--listen HOST:PORT] [--tick-ms N] [--heartbeat-ms N]\n"
               "                   [--suspect-after MISSED] [--dead-after MISSED]\n"
               "                   [--backoff-base-ms N] [--backoff-max-ms N] [--backoff-reset-ms N]\n"
               "                   [--strategy best-fit|worst-fit|first-fit] [--no-preempt] [--quiet]\n"
               "  defaults: tick 100, heartbeat 500, suspect after 3 missed, dead after 6,\n"
               "            backoff 1000 doubling to 60000, reset after 60000 ms up\n");
}

// Integer flag in [lo, hi]; exits with usage on anything else, so a typo
// cannot become a zero-millisecond heartbeat.
long long parse_int(const std::string& s, long long lo, long long hi) {
  try {
    size_t used = 0;
    const long long v = std::stoll(s, &used);
    if (used == s.size() && v >= lo && v <= hi) return v;
  } catch (const std::exception&) {
  }
  usage();
  std::exit(2);
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
    } else if (a == "--tick-ms") {
      opt.tick_interval = std::chrono::milliseconds(parse_int(next(), 1, 60000));
    } else if (a == "--heartbeat-ms") {
      opt.core.heartbeat_interval_ms = static_cast<uint32_t>(parse_int(next(), 1, 600000));
    } else if (a == "--suspect-after") {
      opt.core.suspect_after_missed = static_cast<uint32_t>(parse_int(next(), 1, 1000));
    } else if (a == "--dead-after") {
      opt.core.dead_after_missed = static_cast<uint32_t>(parse_int(next(), 2, 1000));
    } else if (a == "--backoff-base-ms") {
      opt.core.backoff.base = std::chrono::milliseconds(parse_int(next(), 0, 86400000));
    } else if (a == "--backoff-max-ms") {
      opt.core.backoff.max = std::chrono::milliseconds(parse_int(next(), 0, 86400000));
    } else if (a == "--backoff-reset-ms") {
      opt.core.backoff.reset_after = std::chrono::milliseconds(parse_int(next(), 0, 86400000));
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

  if (opt.core.dead_after_missed <= opt.core.suspect_after_missed) {
    std::fprintf(stderr, "fleetmaster: --dead-after must be greater than --suspect-after\n");
    return 2;
  }
  if (opt.core.backoff.max < opt.core.backoff.base) {
    std::fprintf(stderr, "fleetmaster: --backoff-max-ms must be at least --backoff-base-ms\n");
    return 2;
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
  fleet::log_line("fleetmaster",
                  "listening on port " + std::to_string(server.port()) + "; heartbeat " +
                      std::to_string(opt.core.heartbeat_interval_ms) + " ms, suspect after " +
                      std::to_string(opt.core.suspect_after_missed) + " missed, dead after " +
                      std::to_string(opt.core.dead_after_missed) + ", tick " +
                      std::to_string(opt.tick_interval.count()) + " ms");
  const int sig = fleet::wait_for_shutdown_signal();
  fleet::log_line("fleetmaster", std::string(strsignal(sig)) + ", shutting down");
  server.shutdown();
  return 0;
}
