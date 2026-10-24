// walbench: measures what the write-ahead log costs (m4), for
// bench/wal_commit.sh and NOTES.md. Three experiments, all in-process
// with the real MasterCore and Storage (no network, no agents):
//
//   1. sync latency: one small record appended and synced, per SyncMode
//      (none = page cache only, fsync, full = F_FULLFSYNC on macOS);
//   2. group commit vs per-record sync for one scheduler iteration that
//      places K tasks (K = 1, 10, 100, 1000): the iteration makes K
//      TaskPlaced records, committed with one sync or with K syncs;
//   3. recovery time: replaying a log of N records, and loading a
//      snapshot of the same state instead.
//
// Every number it prints comes from timing these calls with the steady
// clock. Prints a line at least every few seconds (the slowest case,
// 1000 per-record F_FULLFSYNCs, takes about 3 s).

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

#include "master_core.h"
#include "wal.h"

namespace fs = std::filesystem;
using namespace fleet;
using Ms = std::chrono::duration<double, std::milli>;

namespace {

double median(std::vector<double> v) {
  std::sort(v.begin(), v.end());
  return v.empty() ? 0 : v[v.size() / 2];
}
double pct(std::vector<double> v, double p) {
  std::sort(v.begin(), v.end());
  if (v.empty()) return 0;
  return v[std::min(v.size() - 1, static_cast<size_t>(p * static_cast<double>(v.size())))];
}

const char* name(SyncMode m) {
  switch (m) {
    case SyncMode::kNone: return "none (page cache)";
    case SyncMode::kFsync: return "fsync";
    case SyncMode::kFull: return "full (F_FULLFSYNC)";
  }
  return "?";
}

MasterOptions bench_options() {
  MasterOptions o;
  o.recovery_window = std::chrono::milliseconds(0);
  return o;
}

// A master with `nodes` 16-core nodes, registered.
void add_nodes(MasterCore& core, int nodes) {
  for (int i = 0; i < nodes; ++i) {
    v1::RegisterNodeRequest req;
    req.set_name("n" + std::to_string(i));
    req.mutable_capacity()->set_milli_cpu(16000);
    req.mutable_capacity()->set_memory_mb(65536);
    v1::RegisterNodeResponse resp;
    if (!core.register_node(req, &resp, Clock::now()).ok()) std::abort();
  }
}

void submit(MasterCore& core, uint32_t count) {
  while (count > 0) {
    const uint32_t n = std::min<uint32_t>(count, 1000);
    v1::SubmitRequest req;
    req.set_job("bench");
    req.set_priority(1);
    req.mutable_request()->set_milli_cpu(100);
    req.mutable_request()->set_memory_mb(64);
    req.add_argv("true");
    req.set_count(n);
    v1::SubmitResponse resp;
    if (!core.submit(req, &resp).ok()) std::abort();
    count -= n;
  }
}

std::unique_ptr<Storage> open_fresh(const fs::path& dir, StorageOptions o) {
  fs::remove_all(dir);
  MasterState s;
  std::string err;
  auto st = Storage::open(dir, o, &s, nullptr, &err);
  if (!st) {
    std::fprintf(stderr, "walbench: %s\n", err.c_str());
    std::exit(1);
  }
  return st;
}

}  // namespace

int main(int argc, char** argv) {
  const fs::path root = argc > 1 ? argv[1] : "/private/tmp/fleet/walbench";
  fs::create_directories(root);

  // 1. Sync latency for one ~40-byte record.
  std::printf("== 1. append + sync of one TaskPlaced record (200 appends each)\n");
  std::printf("  %-20s %10s %10s %10s\n", "sync mode", "p50 ms", "p99 ms", "max ms");
  for (SyncMode m : {SyncMode::kNone, SyncMode::kFsync, SyncMode::kFull}) {
    StorageOptions o;
    o.sync = m;
    o.snapshot_every = 0;
    auto st = open_fresh(root / "lat", o);
    std::vector<double> t;
    for (uint64_t i = 1; i <= 200; ++i) {
      v1::WalRecord r;
      r.set_lsn(i);
      v1::TaskPlaced* p = r.mutable_task_placed();
      p->set_task_id(static_cast<uint32_t>(i));
      p->set_node_id(3);
      p->set_attempt(1);
      p->set_placed_at_ms(1792876001374);
      std::string err;
      const auto t0 = Clock::now();
      if (!st->append({r}, &err)) {
        std::fprintf(stderr, "walbench: %s\n", err.c_str());
        return 1;
      }
      t.push_back(Ms(Clock::now() - t0).count());
    }
    std::printf("  %-20s %10.3f %10.3f %10.3f\n", name(m), median(t), pct(t, 0.99),
                *std::max_element(t.begin(), t.end()));
    std::fflush(stdout);
  }

  // 2. One scheduler iteration placing K tasks, committed both ways.
  std::printf("\n== 2. one scheduling pass placing K tasks on 100 nodes, F_FULLFSYNC\n");
  std::printf("  %6s %-11s %6s %9s %8s %12s %14s\n", "K", "policy", "syncs", "bytes", "runs",
              "commit ms", "records/s");
  for (uint32_t k : {1u, 10u, 100u, 1000u}) {
    for (SyncPolicy pol : {SyncPolicy::kGroup, SyncPolicy::kPerRecord}) {
      const int runs = (k == 1000 && pol == SyncPolicy::kPerRecord) ? 1 : 5;
      std::vector<double> t;
      uint64_t syncs = 0, bytes = 0;
      for (int run = 0; run < runs; ++run) {
        MasterCore core(bench_options());
        add_nodes(core, 100);
        submit(core, k);
        StorageOptions o;
        o.policy = pol;
        o.snapshot_every = 0;
        auto st = open_fresh(root / "pass", o);
        std::string err;
        if (!st->append(core.take_records(), &err)) return 1;  // setup, not timed
        const uint64_t s0 = st->stats().syncs, b0 = st->stats().bytes;
        core.tick(Clock::now());  // the scheduler iteration: K placements
        const std::vector<v1::WalRecord> batch = core.take_records();
        if (batch.size() != k) {
          std::fprintf(stderr, "walbench: expected %u placements, got %zu\n", k, batch.size());
          return 1;
        }
        const auto t0 = Clock::now();
        if (!st->append(batch, &err)) return 1;
        t.push_back(Ms(Clock::now() - t0).count());
        syncs = st->stats().syncs - s0;
        bytes = st->stats().bytes - b0;
      }
      const double ms = median(t);
      std::printf("  %6u %-11s %6llu %9llu %8d %12.2f %14.0f\n", k,
                  pol == SyncPolicy::kGroup ? "group" : "per-record",
                  static_cast<unsigned long long>(syncs), static_cast<unsigned long long>(bytes),
                  runs, ms, ms > 0 ? k / (ms / 1000.0) : 0.0);
      std::fflush(stdout);
    }
  }

  // 3. Recovery: replay N records vs load a snapshot of the same state.
  std::printf("\n== 3. recovery: replay the log vs load a snapshot (100 nodes)\n");
  std::printf("  %9s %8s %11s %12s %12s %14s %12s\n", "records", "tasks", "log bytes",
              "replay ms", "us/record", "snapshot bytes", "load ms");
  for (uint64_t target : {10'000ull, 100'000ull}) {
    const fs::path dir = root / ("rec" + std::to_string(target));
    StorageOptions o;
    o.sync = SyncMode::kNone;  // building the log is setup, not measured
    o.snapshot_every = 0;
    auto st = open_fresh(dir, o);
    MasterCore core(bench_options());
    add_nodes(core, 100);
    std::string err;
    // Rounds of: submit 1000, place them, every node reports its tasks
    // exited. About 2 records per task (placed, exited) plus submissions.
    uint64_t logged = 0;
    while (logged < target) {
      submit(core, 1000);
      core.tick(Clock::now());
      for (NodeId n = 0; n < 100; ++n) {
        v1::HeartbeatRequest hb;
        hb.set_node_id(n);
        hb.set_epoch(core.epoch(n));
        for (TaskId t : core.state().tasks_on(n)) {
          v1::TaskStatus* s = hb.add_tasks();
          s->mutable_instance()->set_task_id(t);
          s->mutable_instance()->set_attempt(core.attempt(t));
          s->mutable_instance()->set_epoch(core.epoch(n));
          s->set_phase(v1::TaskStatus::PHASE_EXITED);
        }
        v1::HeartbeatResponse resp;
        if (!core.heartbeat(hb, &resp, Clock::now()).ok()) return 1;
      }
      auto recs = core.take_records();
      logged += recs.size();
      if (!st->append(recs, &err)) return 1;
    }
    const uint64_t log_bytes = fs::file_size(Storage::log_path(dir));
    st.reset();
    MasterState replayed;
    RecoveryInfo info;
    auto again = Storage::open(dir, o, &replayed, &info, &err);
    if (!again) return 1;
    // The same state as a snapshot, then a recovery that only loads it.
    if (!again->write_snapshot(replayed, &err)) return 1;
    const uint64_t snap_bytes = fs::file_size(Storage::snapshot_path(dir));
    again.reset();
    MasterState from_snap;
    RecoveryInfo info2;
    if (!Storage::open(dir, o, &from_snap, &info2, &err)) return 1;
    std::printf("  %9zu %8zu %11llu %12.1f %12.2f %14llu %12.1f\n", info.replayed,
                replayed.cluster().tasks().size(), static_cast<unsigned long long>(log_bytes),
                info.load_ms, 1000.0 * info.load_ms / static_cast<double>(info.replayed),
                static_cast<unsigned long long>(snap_bytes), info2.load_ms);
    std::fflush(stdout);
  }
  fs::remove_all(root);
  return 0;
}
