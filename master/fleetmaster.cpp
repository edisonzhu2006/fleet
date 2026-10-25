// fleetmaster: the control plane binary. Parses flags, starts a
// MasterServer, and shuts it down cleanly on SIGINT or SIGTERM. All logic
// is in fleet_master_lib (master_core, master_server, failure_detector,
// and from m4 master_state and wal) so the integration tests run exactly
// the same code in-process.
//
// With --state-dir (m4) the master keeps a write-ahead log and snapshots
// there and recovers from them on start; nothing special happens at
// shutdown, because every change is already durable when its RPC returns.
// A SIGKILL and a clean stop leave the same files.
//
// With --id and --peers (m4b) it runs as one replica of a Raft group
// instead (ReplicaServer): the Raft log, term and snapshot live in
// --state-dir, only the leader serves, and the others redirect. Without
// --peers it is the m4 single master, unchanged. In replica mode SIGUSR1
// cuts the replica off from every other replica and SIGUSR2 reconnects it
// (the partition test in bench/leader_failover.sh).

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <map>
#include <string>

#include "leader_redirect.h"
#include "log.h"
#include "master_server.h"
#include "replica_server.h"
#include "signals.h"

namespace {

void usage() {
  std::fprintf(stderr,
               "usage: fleetmaster [--listen HOST:PORT] [--tick-ms N] [--heartbeat-ms N]\n"
               "                   [--suspect-after MISSED] [--dead-after MISSED]\n"
               "                   [--backoff-base-ms N] [--backoff-max-ms N] [--backoff-reset-ms N]\n"
               "                   [--strategy best-fit|worst-fit|first-fit] [--no-preempt] [--quiet]\n"
               "                   [--state-dir DIR] [--wal-sync full|fsync|none] [--wal-per-record]\n"
               "                   [--snapshot-every RECORDS] [--recovery-window-ms N]\n"
               "  replicated (m4b): --id N --peers 1=HOST:PORT,2=HOST:PORT,3=HOST:PORT --state-dir DIR\n"
               "                   [--election-min-ms N] [--election-max-ms N] [--raft-heartbeat-ms N]\n"
               "                   [--reply-timeout-ms N]\n"
               "  defaults: tick 100, heartbeat 500, suspect after 3 missed, dead after 6,\n"
               "            backoff 1000 doubling to 60000, reset after 60000 ms up;\n"
               "            no state dir (state is lost on exit); with one: wal-sync full\n"
               "            (F_FULLFSYNC on macOS), one sync per batch, snapshot every 10000\n"
               "            records, recovery window 1000 ms\n"
               "  replicated defaults: listen on the --peers address of --id, election timeout\n"
               "            150..300 ms, raft heartbeat 50 ms, reply timeout 1000 ms, snapshot\n"
               "            (log compaction) every 10000 entries; SIGUSR1/SIGUSR2 isolate/rejoin\n");
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

// "1=127.0.0.1:7071,2=..." -> {1: "127.0.0.1:7071", ...}; empty on error.
std::map<uint32_t, std::string> parse_peers(const std::string& list) {
  std::map<uint32_t, std::string> out;
  for (const std::string& item : fleet::split_addresses(list)) {
    const size_t eq = item.find('=');
    if (eq == std::string::npos || eq + 1 >= item.size()) return {};
    const long long id = parse_int(item.substr(0, eq), 1, 1000);
    if (!out.emplace(static_cast<uint32_t>(id), item.substr(eq + 1)).second) return {};
  }
  return out;
}

int run_replica(const fleet::MasterServerOptions& opt, fleet::ReplicaOptions ro) {
  ro.core = opt.core;
  ro.tick_interval = opt.tick_interval;
  ro.state_dir = opt.state_dir;
  ro.storage.sync = opt.storage.sync;
  ro.snapshot_every = opt.storage.snapshot_every;
  if (!opt.listen_address.empty() && opt.listen_address != "127.0.0.1:7070") {
    ro.listen_address = opt.listen_address;
  }
  // Before any thread exists, so every thread inherits the mask.
  fleet::block_control_signals();
  fleet::ReplicaServer server(ro);
  std::string err;
  if (!server.start(&err)) {
    std::fprintf(stderr, "fleetmaster: %s\n", err.c_str());
    return 1;
  }
  std::string members;
  for (const auto& [id, addr] : ro.members) members += " " + std::to_string(id) + "=" + addr;
  const std::string tag = "fleetmaster[" + std::to_string(ro.id) + "]";
  fleet::log_line(tag, "replica " + std::to_string(ro.id) + " listening on port " +
                           std::to_string(server.port()) + "; members" + members +
                           "; election timeout " + std::to_string(ro.election_timeout_min.count()) +
                           ".." + std::to_string(ro.election_timeout_max.count()) +
                           " ms, raft heartbeat " + std::to_string(ro.raft_heartbeat.count()) +
                           " ms, agent heartbeat " + std::to_string(opt.core.heartbeat_interval_ms) +
                           " ms");
  while (true) {
    const int sig = fleet::wait_for_control_signal();
    if (sig == SIGUSR1) {
      server.set_isolated(true);
    } else if (sig == SIGUSR2) {
      server.set_isolated(false);
    } else {
      fleet::log_line(tag, std::string(strsignal(sig)) + ", shutting down");
      break;
    }
  }
  server.shutdown();
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  fleet::MasterServerOptions opt;
  opt.core.log_events = true;
  fleet::ReplicaOptions ro;
  bool replicated = false;
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
    } else if (a == "--state-dir") {
      opt.state_dir = next();
    } else if (a == "--wal-sync") {
      const std::string m = next();
      if (m == "full") {
        opt.storage.sync = fleet::SyncMode::kFull;
      } else if (m == "fsync") {
        opt.storage.sync = fleet::SyncMode::kFsync;
      } else if (m == "none") {
        opt.storage.sync = fleet::SyncMode::kNone;
      } else {
        usage();
        return 2;
      }
    } else if (a == "--wal-per-record") {
      opt.storage.policy = fleet::SyncPolicy::kPerRecord;
    } else if (a == "--snapshot-every") {
      opt.storage.snapshot_every = static_cast<uint64_t>(parse_int(next(), 0, 1'000'000'000));
    } else if (a == "--recovery-window-ms") {
      opt.core.recovery_window = std::chrono::milliseconds(parse_int(next(), 0, 600000));
    } else if (a == "--id") {
      ro.id = static_cast<uint32_t>(parse_int(next(), 1, 1000));
      replicated = true;
    } else if (a == "--peers") {
      ro.members = parse_peers(next());
      if (ro.members.empty()) {
        usage();
        return 2;
      }
      replicated = true;
    } else if (a == "--election-min-ms") {
      ro.election_timeout_min = std::chrono::milliseconds(parse_int(next(), 2, 600000));
    } else if (a == "--election-max-ms") {
      ro.election_timeout_max = std::chrono::milliseconds(parse_int(next(), 2, 600000));
    } else if (a == "--raft-heartbeat-ms") {
      ro.raft_heartbeat = std::chrono::milliseconds(parse_int(next(), 1, 600000));
    } else if (a == "--reply-timeout-ms") {
      ro.reply_timeout = std::chrono::milliseconds(parse_int(next(), 1, 600000));
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

  if (replicated) {
    if (ro.id == 0 || ro.members.empty() || !ro.members.count(ro.id) || opt.state_dir.empty()) {
      std::fprintf(stderr, "fleetmaster: a replica needs --id, --peers including that id, and "
                           "--state-dir\n");
      return 2;
    }
    if (ro.raft_heartbeat >= ro.election_timeout_min ||
        ro.election_timeout_max < ro.election_timeout_min) {
      std::fprintf(stderr, "fleetmaster: need --raft-heartbeat-ms < --election-min-ms <= "
                           "--election-max-ms\n");
      return 2;
    }
    return run_replica(opt, std::move(ro));
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
