// fleetsim: a simulated cluster for measuring the m1 scheduler.
//
// WHY THIS EXISTS. Placement quality (utilization, how many tasks stay
// pending, how fragmented the free space gets) and decision latency are the
// numbers an interviewer asks for, and they only mean something under a
// realistic workload. fleetsim generates one from a seed so every number is
// reproducible, then runs it through the real fleet_core library:
//
//   - N nodes in three shapes (standard, small, high-memory), some with an
//     ssd label.
//   - M tasks in six size classes with +/-25% jitter, four priority bands,
//     a few requiring ssd, each with a lifetime in simulation steps.
//   - Online arrival: each step finishes tasks whose lifetime is over,
//     submits the next batch, and calls schedule_pending() once. Arrival
//     over time (not all at once) is what makes preemption happen: a
//     production task arriving at step 80 finds the cluster full of batch
//     work placed earlier. Lifetimes create churn, which is what fragments
//     free space and separates the strategies.
//
// check_invariants() runs after every step; any violation aborts the run.
// --compare runs all three strategies on the identical workload and prints
// a markdown table (the one recorded in NOTES.md).

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include "cluster_state.h"
#include "scheduler.h"

namespace {

using fleet::ClusterState;
using fleet::NodeId;
using fleet::Resources;
using fleet::Scheduler;
using fleet::Strategy;
using fleet::TaskId;

// splitmix64: a tiny, well-known PRNG. Used instead of <random>'s
// distributions because those are implementation-defined (libc++ and
// libstdc++ produce different streams from the same seed), and the
// workload must be identical on the Mac and on Linux CI.
class Rng {
 public:
  explicit Rng(uint64_t seed) : s_(seed) {}
  uint64_t next() {
    uint64_t z = (s_ += 0x9e3779b97f4a7c15ULL);
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
  }
  // Uniform in [0, 1).
  double unit() { return static_cast<double>(next() >> 11) * 0x1.0p-53; }
  // Uniform integer in [lo, hi]. Modulo bias is below 2^-40 for these
  // ranges, irrelevant for a workload generator.
  int64_t range(int64_t lo, int64_t hi) {
    return lo + static_cast<int64_t>(next() % static_cast<uint64_t>(hi - lo + 1));
  }
  // Index chosen with the given weights (they need not sum to 1).
  size_t pick(const std::vector<double>& weights) {
    double total = 0;
    for (double w : weights) total += w;
    double x = unit() * total;
    for (size_t i = 0; i < weights.size(); ++i) {
      if (x < weights[i]) return i;
      x -= weights[i];
    }
    return weights.size() - 1;
  }

 private:
  uint64_t s_;
};

struct Config {
  size_t nodes = 100;
  size_t tasks = 5000;
  size_t batch = 50;  // tasks submitted per step
  int64_t life_min = 10, life_max = 50;  // task lifetime in steps
  size_t jobs = 50;
  int64_t job_quota_mcpu = 0;  // 0 = no quotas
  uint64_t seed = 42;
  Strategy strategy = Strategy::kBestFit;
  bool compare = false;
  size_t seeds = 1;  // --compare averages over seeds seed .. seed+seeds-1
  bool no_preempt = false;
};

struct SimTask {
  fleet::TaskSpec spec;
  int64_t lifetime;
};

struct Workload {
  std::vector<std::pair<Resources, fleet::Labels>> nodes;
  std::vector<SimTask> tasks;
};

// Task size classes (before jitter) and how often each is drawn. Shared by
// the generator and the fragmentation metric, which asks "how much of the
// free space could the tasks this workload actually submits use?".
const std::vector<Resources> kTaskShapes{{250, 512},    {1000, 2048},
                                         {2000, 8192},  {8000, 4096},
                                         {1000, 32768}, {8000, 32768}};
const std::vector<double> kTaskWeights{0.30, 0.30, 0.20, 0.08, 0.08, 0.04};
constexpr Resources kLargeTask{8000, 32768};  // the biggest class

Workload generate(const Config& cfg) {
  Rng rng(cfg.seed);
  Workload w;
  // Node shapes: standard 32c/128G, small 16c/64G, high-memory 16c/256G.
  const std::vector<Resources> node_shapes{{32000, 131072}, {16000, 65536},
                                           {16000, 262144}};
  const std::vector<double> node_weights{0.5, 0.3, 0.2};
  for (size_t i = 0; i < cfg.nodes; ++i) {
    fleet::Labels labels;
    labels["zone"] = (i % 2 == 0) ? "a" : "b";
    if (rng.unit() < 0.25) labels["disk"] = "ssd";
    w.nodes.emplace_back(node_shapes[rng.pick(node_weights)], labels);
  }
  // Task classes: tiny, small, medium, cpu-heavy, memory-heavy, large.
  // Priority mix: mostly batch and normal, a little critical, like a real
  // shared cluster where production is the minority by count.
  const std::vector<double> prio_weights{0.40, 0.35, 0.20, 0.05};
  for (size_t i = 0; i < cfg.tasks; ++i) {
    const Resources base = kTaskShapes[rng.pick(kTaskWeights)];
    // +/-25% jitter so tasks are not all identical bricks; identical sizes
    // make every strategy pack perfectly and hide the differences.
    auto jitter = [&](int64_t v) {
      return std::max<int64_t>(1, v * rng.range(75, 125) / 100);
    };
    SimTask t;
    t.spec.job = "job" + std::to_string(rng.range(0, static_cast<int64_t>(cfg.jobs) - 1));
    t.spec.priority = static_cast<int>(rng.pick(prio_weights));
    t.spec.request = {jitter(base.milli_cpu), jitter(base.memory_mb)};
    if (rng.unit() < 0.05) t.spec.required_labels["disk"] = "ssd";
    t.lifetime = rng.range(cfg.life_min, cfg.life_max);
    w.tasks.push_back(std::move(t));
  }
  return w;
}

struct Report {
  Strategy strategy;
  size_t submitted = 0, rejected = 0, running = 0, pending = 0;
  size_t finished = 0, preemptions = 0, decisions = 0, truncated = 0;
  size_t invariant_checks = 0;
  double cpu_util = 0, mem_util = 0;          // at end of arrivals
  double mean_cpu_util = 0, mean_mem_util = 0;  // averaged over steps
  // See fragmentation() below: share of free CPU and of cluster CPU.
  double frag_free = 0, frag_cluster = 0, mean_frag_cluster = 0;
  int64_t large_slots = 0;  // how many more kLargeTask would fit
  size_t stranded = 0;      // pending tasks that fit the aggregate free space but no node
  double mean_ns = 0, p50_ns = 0, p99_ns = 0, max_ns = 0;
  double total_sched_ms = 0;
};

void utilization(const ClusterState& st, double& cpu, double& mem) {
  Resources cap, alloc;
  for (const auto& n : st.nodes()) {
    if (n.state != fleet::NodeState::kReady) continue;
    cap += n.capacity;
    alloc += n.allocated;
  }
  cpu = static_cast<double>(alloc.milli_cpu) / static_cast<double>(cap.milli_cpu);
  mem = static_cast<double>(alloc.memory_mb) / static_cast<double>(cap.memory_mb);
}

// Fragmentation in the sense of Weng et al., "Beware of Fragmentation"
// (USENIX ATC '23): the expected share of the cluster's free CPU that sits
// on nodes too full to host a task drawn from the workload's own size
// distribution. For each size class, free CPU on a node where that class
// does not fit (in CPU or memory) is "fragmented for that class"; the
// metric is the class-weighted average. 0% means every free core is usable
// by any task; 100% means none is. Plain "free CPU on nodes below X" would
// depend on an arbitrary X; weighting by the real mix does not.
//
// Two normalizations are reported. As a share of FREE CPU it says how
// usable the leftover space is, but a strategy that packs tighter has less
// free CPU, so its few crumbs score badly. As a share of CLUSTER CPU it
// says how much capacity is wasted, which is comparable across strategies.
struct Frag {
  double of_free = 0;     // fragmented / free CPU
  double of_cluster = 0;  // fragmented / total CPU
};
Frag fragmentation(const ClusterState& st) {
  double total_w = 0, frag = 0;
  int64_t free_cpu = 0, cap_cpu = 0;
  for (const auto& n : st.nodes()) {
    if (n.state != fleet::NodeState::kReady) continue;
    free_cpu += n.free().milli_cpu;
    cap_cpu += n.capacity.milli_cpu;
  }
  if (free_cpu == 0) return {};
  for (size_t c = 0; c < kTaskShapes.size(); ++c) {
    int64_t stuck = 0;
    for (const auto& n : st.nodes()) {
      if (n.state != fleet::NodeState::kReady) continue;
      if (!kTaskShapes[c].fits_in(n.free())) stuck += n.free().milli_cpu;
    }
    frag += kTaskWeights[c] * static_cast<double>(stuck);
    total_w += kTaskWeights[c];
  }
  const double stuck_cpu = frag / total_w;
  return {stuck_cpu / static_cast<double>(free_cpu),
          stuck_cpu / static_cast<double>(cap_cpu)};
}

Report run(const Config& cfg, const Workload& w, Strategy strategy) {
  ClusterState st;
  for (const auto& [cap, labels] : w.nodes) st.add_node(cap, labels);
  if (cfg.job_quota_mcpu > 0) {
    for (size_t j = 0; j < cfg.jobs; ++j) {
      // Memory quota proportional to CPU at 4 GB per core.
      st.set_quota("job" + std::to_string(j),
                   {cfg.job_quota_mcpu, cfg.job_quota_mcpu * 4});
    }
  }
  fleet::SchedulerOptions opts;
  opts.strategy = strategy;
  opts.enable_preemption = !cfg.no_preempt;
  opts.record_latency = true;
  Scheduler sched(opts);

  Report r;
  r.strategy = strategy;
  // Per submitted task: workload index (for lifetime) and the step it
  // started running, or -1 when not running.
  std::vector<size_t> workload_index;
  std::vector<int64_t> started;
  std::vector<int64_t> ns;
  size_t next = 0;
  int64_t step = 0;
  double sum_cpu = 0, sum_mem = 0, sum_frag = 0;

  while (next < w.tasks.size()) {
    // 1. Tasks whose lifetime is over exit successfully.
    for (TaskId id = 0; id < started.size(); ++id) {
      if (started[id] >= 0 &&
          step - started[id] >= w.tasks[workload_index[id]].lifetime) {
        st.finish(id, true);
        started[id] = -1;
        ++r.finished;
      }
    }
    // 2. Submit this step's batch through admission control.
    for (size_t k = 0; k < cfg.batch && next < w.tasks.size(); ++k, ++next) {
      auto res = st.submit(w.tasks[next].spec);
      ++r.submitted;
      if (!res.ok) {
        ++r.rejected;
        continue;
      }
      workload_index.push_back(next);
      started.push_back(-1);
    }
    // 3. One scheduling pass.
    const auto t0 = std::chrono::steady_clock::now();
    auto result = sched.schedule_pending(st);
    r.total_sched_ms += std::chrono::duration<double, std::milli>(
                            std::chrono::steady_clock::now() - t0)
                            .count();
    for (const auto& p : result.preemptions) started[p.victim] = -1;
    for (const auto& p : result.placements) started[p.task] = step;
    r.preemptions += result.preemptions.size();
    r.decisions += result.decisions;
    r.truncated += result.truncated_searches;
    ns.insert(ns.end(), result.decision_ns.begin(), result.decision_ns.end());

    const std::string err = st.check_invariants();
    ++r.invariant_checks;
    if (!err.empty()) {
      std::fprintf(stderr, "INVARIANT VIOLATION at step %lld:\n%s",
                   static_cast<long long>(step), err.c_str());
      std::exit(1);
    }
    double c = 0, m = 0;
    utilization(st, c, m);
    sum_cpu += c;
    sum_mem += m;
    sum_frag += fragmentation(st).of_cluster;
    ++step;
  }

  r.mean_cpu_util = sum_cpu / static_cast<double>(step);
  r.mean_mem_util = sum_mem / static_cast<double>(step);
  r.mean_frag_cluster = sum_frag / static_cast<double>(step);
  const Frag fr = fragmentation(st);
  r.frag_free = fr.of_free;
  r.frag_cluster = fr.of_cluster;
  utilization(st, r.cpu_util, r.mem_util);
  r.pending = st.pending().size();
  Resources free_total;
  for (const auto& n : st.nodes()) {
    r.running += st.tasks_on(n.id).size();
    if (n.state != fleet::NodeState::kReady) continue;
    const Resources f = n.free();
    free_total += f;
    r.large_slots += std::min(f.milli_cpu / kLargeTask.milli_cpu,
                              f.memory_mb / kLargeTask.memory_mb);
  }
  for (const auto& key : st.pending()) {
    if (st.task(key.id).request.fits_in(free_total)) ++r.stranded;
  }
  if (!ns.empty()) {
    std::sort(ns.begin(), ns.end());
    double sum = 0;
    for (int64_t v : ns) sum += static_cast<double>(v);
    r.mean_ns = sum / static_cast<double>(ns.size());
    r.p50_ns = static_cast<double>(ns[ns.size() / 2]);
    r.p99_ns = static_cast<double>(ns[ns.size() * 99 / 100]);
    r.max_ns = static_cast<double>(ns.back());
  }
  return r;
}

void print_report(const Report& r) {
  std::printf("strategy            %s\n", std::string(fleet::to_string(r.strategy)).c_str());
  std::printf("submitted           %zu (rejected by quota: %zu)\n", r.submitted, r.rejected);
  std::printf("running at end      %zu\n", r.running);
  std::printf("pending at end      %zu (stranded by fragmentation: %zu)\n", r.pending, r.stranded);
  std::printf("finished            %zu\n", r.finished);
  std::printf("preemptions         %zu\n", r.preemptions);
  std::printf("utilization at end  cpu %.1f%%  mem %.1f%%\n", 100 * r.cpu_util, 100 * r.mem_util);
  std::printf("mean utilization    cpu %.1f%%  mem %.1f%%\n", 100 * r.mean_cpu_util, 100 * r.mean_mem_util);
  std::printf("fragmented CPU      end %.1f%% of free, %.1f%% of cluster; mean over steps %.1f%% of cluster\n",
              100 * r.frag_free, 100 * r.frag_cluster, 100 * r.mean_frag_cluster);
  std::printf("large-task slots    %lld\n", static_cast<long long>(r.large_slots));
  std::printf("decisions           %zu (truncated preemption searches: %zu)\n", r.decisions, r.truncated);
  std::printf("time per decision   mean %.2f us  p50 %.2f us  p99 %.2f us  max %.1f us\n",
              r.mean_ns / 1e3, r.p50_ns / 1e3, r.p99_ns / 1e3, r.max_ns / 1e3);
  std::printf("total schedule time %.1f ms\n", r.total_sched_ms);
  std::printf("invariants          ok (checked after each of %zu steps)\n", r.invariant_checks);
}

// Field-by-field mean of several runs (one per seed) of one strategy.
Report mean_of(const std::vector<Report>& rs) {
  Report m;
  m.strategy = rs.front().strategy;
  const double k = static_cast<double>(rs.size());
  auto avg = [&](auto field) {
    double sum = 0;
    for (const auto& r : rs) sum += static_cast<double>(r.*field);
    return sum / k;
  };
  auto avg_count = [&](auto field) {
    return static_cast<size_t>(avg(field) + 0.5);
  };
  m.submitted = avg_count(&Report::submitted);
  m.rejected = avg_count(&Report::rejected);
  m.running = avg_count(&Report::running);
  m.pending = avg_count(&Report::pending);
  m.finished = avg_count(&Report::finished);
  m.preemptions = avg_count(&Report::preemptions);
  m.decisions = avg_count(&Report::decisions);
  m.truncated = avg_count(&Report::truncated);
  m.stranded = avg_count(&Report::stranded);
  m.invariant_checks = avg_count(&Report::invariant_checks);
  m.large_slots = static_cast<int64_t>(avg(&Report::large_slots) + 0.5);
  m.cpu_util = avg(&Report::cpu_util);
  m.mem_util = avg(&Report::mem_util);
  m.mean_cpu_util = avg(&Report::mean_cpu_util);
  m.mean_mem_util = avg(&Report::mean_mem_util);
  m.frag_free = avg(&Report::frag_free);
  m.frag_cluster = avg(&Report::frag_cluster);
  m.mean_frag_cluster = avg(&Report::mean_frag_cluster);
  m.mean_ns = avg(&Report::mean_ns);
  m.p50_ns = avg(&Report::p50_ns);
  m.p99_ns = avg(&Report::p99_ns);
  m.max_ns = avg(&Report::max_ns);
  m.total_sched_ms = avg(&Report::total_sched_ms);
  return m;
}

void print_table(const std::vector<Report>& rs) {
  std::printf("| strategy | running | pending | preemptions | cpu util end / mean | mem util end / mean | fragmented CPU end (%% of free / %% of cluster) | 8c/32G slots | decision mean / p99 (us) |\n");
  std::printf("|---|---|---|---|---|---|---|---|---|\n");
  for (const auto& r : rs) {
    std::printf("| %s | %zu | %zu | %zu | %.1f%% / %.1f%% | %.1f%% / %.1f%% | %.1f%% / %.1f%% | %lld | %.2f / %.2f |\n",
                std::string(fleet::to_string(r.strategy)).c_str(), r.running,
                r.pending, r.preemptions, 100 * r.cpu_util,
                100 * r.mean_cpu_util, 100 * r.mem_util, 100 * r.mean_mem_util,
                100 * r.frag_free, 100 * r.frag_cluster,
                static_cast<long long>(r.large_slots), r.mean_ns / 1e3,
                r.p99_ns / 1e3);
  }
}

[[noreturn]] void usage() {
  std::fprintf(stderr,
               "usage: fleetsim [--nodes N] [--tasks M] [--batch B] [--seed S]\n"
               "                [--life MIN MAX] [--jobs J] [--job-quota-mcpu Q]\n"
               "                [--strategy best-fit|worst-fit|first-fit]\n"
               "                [--no-preempt] [--compare [--seeds K]]\n");
  std::exit(2);
}

int64_t parse_int(const char* s) {
  char* end = nullptr;
  const long long v = std::strtoll(s, &end, 10);
  if (end == s || *end != '\0' || v < 0) usage();
  return v;
}

}  // namespace

int main(int argc, char** argv) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string_view a = argv[i];
    auto need = [&](int n) {
      if (i + n >= argc) usage();
    };
    if (a == "--nodes") { need(1); cfg.nodes = static_cast<size_t>(parse_int(argv[++i])); }
    else if (a == "--tasks") { need(1); cfg.tasks = static_cast<size_t>(parse_int(argv[++i])); }
    else if (a == "--batch") { need(1); cfg.batch = static_cast<size_t>(parse_int(argv[++i])); }
    else if (a == "--seed") { need(1); cfg.seed = static_cast<uint64_t>(parse_int(argv[++i])); }
    else if (a == "--jobs") { need(1); cfg.jobs = static_cast<size_t>(parse_int(argv[++i])); }
    else if (a == "--job-quota-mcpu") { need(1); cfg.job_quota_mcpu = parse_int(argv[++i]); }
    else if (a == "--life") { need(2); cfg.life_min = parse_int(argv[++i]); cfg.life_max = parse_int(argv[++i]); }
    else if (a == "--strategy") {
      need(1);
      auto s = fleet::parse_strategy(argv[++i]);
      if (!s) usage();
      cfg.strategy = *s;
    }
    else if (a == "--no-preempt") cfg.no_preempt = true;
    else if (a == "--compare") cfg.compare = true;
    else if (a == "--seeds") { need(1); cfg.seeds = static_cast<size_t>(parse_int(argv[++i])); }
    else usage();
  }
  if (cfg.nodes == 0 || cfg.batch == 0 || cfg.jobs == 0 ||
      cfg.seeds == 0 || cfg.life_min < 1 || cfg.life_max < cfg.life_min) {
    usage();
  }

  std::printf("fleetsim: %zu nodes, %zu tasks, batch %zu, lifetime %lld..%lld steps, seed %llu%s\n",
              cfg.nodes, cfg.tasks, cfg.batch, static_cast<long long>(cfg.life_min),
              static_cast<long long>(cfg.life_max), static_cast<unsigned long long>(cfg.seed),
              cfg.no_preempt ? ", preemption off" : "");
  if (!cfg.compare) {
    print_report(run(cfg, generate(cfg), cfg.strategy));
    return 0;
  }
  // Every strategy sees exactly the same workloads: one generated per seed,
  // reused across strategies.
  std::vector<Workload> workloads;
  for (size_t k = 0; k < cfg.seeds; ++k) {
    Config c = cfg;
    c.seed = cfg.seed + k;
    workloads.push_back(generate(c));
  }
  std::vector<Report> means;
  for (Strategy s : {Strategy::kBestFit, Strategy::kWorstFit, Strategy::kFirstFit}) {
    std::vector<Report> per_seed;
    for (const auto& w : workloads) per_seed.push_back(run(cfg, w, s));
    means.push_back(mean_of(per_seed));
    if (cfg.seeds > 1) {
      std::printf("\n(mean over seeds %llu..%llu)\n",
                  static_cast<unsigned long long>(cfg.seed),
                  static_cast<unsigned long long>(cfg.seed + cfg.seeds - 1));
    } else {
      std::printf("\n");
    }
    print_report(means.back());
    std::fflush(stdout);  // progress for long multi-seed runs
  }
  std::printf("\n");
  print_table(means);
  return 0;
}
