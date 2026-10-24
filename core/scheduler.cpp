// Scheduler implementation: feasibility + scoring, preemption search, and
// the scheduling pass. See scheduler.h for the policy in plain words.

#include "scheduler.h"

#include <algorithm>
#include <chrono>
#include <tuple>
#include <utility>

namespace fleet {

std::string_view to_string(Strategy s) {
  switch (s) {
    case Strategy::kBestFit: return "best-fit";
    case Strategy::kWorstFit: return "worst-fit";
    case Strategy::kFirstFit: return "first-fit";
  }
  return "?";
}

std::optional<Strategy> parse_strategy(std::string_view name) {
  if (name == "best-fit" || name == "best") return Strategy::kBestFit;
  if (name == "worst-fit" || name == "worst") return Strategy::kWorstFit;
  if (name == "first-fit" || name == "first") return Strategy::kFirstFit;
  return std::nullopt;
}

namespace {

// An exact fraction num/den with den > 0. Scores are compared by
// cross-multiplying instead of dividing into doubles, so two nodes with
// truly equal scores compare equal and the tie-break (lowest id) is what
// decides, not floating-point rounding. ClusterState::kMaxCapacity keeps
// both factors below 2^31, so the products fit in int64.
struct Fraction {
  int64_t num;
  int64_t den;
};
bool less(const Fraction& a, const Fraction& b) {
  return a.num * b.den < b.num * a.den;
}

// Fraction of the node's capacity the task would leave free on the task's
// dominant resource: the dimension where the task asks for the larger share
// of THIS node (a 4-core/4 GB task is CPU-dominant on a 16-core/128 GB node
// and memory-dominant on a 64-core/8 GB one). If both shares are equal, CPU
// is used; any fixed rule would do, it just has to be fixed.
Fraction leftover_on_dominant(const Node& n, const Resources& req) {
  const Resources free = n.free();
  const bool cpu_dominant = !less(Fraction{req.milli_cpu, n.capacity.milli_cpu},
                                  Fraction{req.memory_mb, n.capacity.memory_mb});
  if (cpu_dominant) {
    return {free.milli_cpu - req.milli_cpu, n.capacity.milli_cpu};
  }
  return {free.memory_mb - req.memory_mb, n.capacity.memory_mb};
}

// Lexicographic preemption cost. Fewest victims first because each victim
// is lost work and a restart; then avoid touching higher bands (evicting
// two batch tasks beats evicting one batch and one production); then the
// total priority; node id last only so ties are deterministic.
using Cost = std::tuple<size_t, int, int, NodeId>;

struct Candidate {
  TaskId id;
  Resources request;
  int priority;
};

// Depth-first search for the cheapest victim subset on one node.
// Candidates are sorted cheapest-first (lowest priority, then largest), so
// the very first complete set the search reaches is the greedy answer
// "evict cheapest-first until it fits"; everything after that only looks
// for strictly better sets, pruned by count and by what is left to take.
class VictimSearch {
 public:
  VictimSearch(std::vector<Candidate> cands, Resources deficit, NodeId node,
               uint64_t& budget)
      : cands_(std::move(cands)), deficit_(deficit), node_(node),
        budget_(budget) {
    // suffix_[i] = total request of candidates i..end: if even taking all
    // of them cannot cover the deficit, the branch is dead.
    suffix_.assign(cands_.size() + 1, Resources{});
    for (size_t i = cands_.size(); i-- > 0;) {
      suffix_[i] = suffix_[i + 1] + cands_[i].request;
    }
  }

  // best is the cheapest cost seen so far across all nodes (shared, so
  // later nodes are pruned against earlier ones). Returns true if this node
  // produced a new best, stored in best/best_victims.
  bool run(std::optional<Cost>& best, std::vector<TaskId>& best_victims) {
    best_ = &best;
    best_victims_ = &best_victims;
    improved_ = false;
    dfs(0, Resources{}, /*max_prio=*/-1, /*prio_sum=*/0);
    return improved_;
  }

  bool truncated() const { return truncated_; }

 private:
  void dfs(size_t i, Resources freed, int max_prio, int prio_sum) {
    // The budget only stops the search once SOME plan exists. The first
    // descent (take candidates in order until the deficit is covered)
    // never backtracks, so it always completes and yields the greedy plan;
    // the budget bounds only the hunt for something better.
    if (budget_ == 0 && *best_) {
      truncated_ = true;
      return;
    }
    if (budget_ > 0) --budget_;
    if (deficit_.fits_in(freed)) {
      Cost c{chosen_.size(), max_prio, prio_sum, node_};
      if (!*best_ || c < **best_) {
        *best_ = c;
        *best_victims_ = chosen_;
        improved_ = true;
      }
      return;  // adding more victims only makes the cost worse
    }
    // Any completion needs at least one more victim.
    if (*best_ && chosen_.size() + 1 > std::get<0>(**best_)) return;
    if (!deficit_.fits_in(freed + suffix_[i])) return;
    for (size_t j = i; j < cands_.size(); ++j) {
      if (!deficit_.fits_in(freed + suffix_[j])) return;  // suffix shrinks
      chosen_.push_back(cands_[j].id);
      dfs(j + 1, freed + cands_[j].request,
          std::max(max_prio, cands_[j].priority), prio_sum + cands_[j].priority);
      chosen_.pop_back();
      if (truncated_) return;
    }
  }

  std::vector<Candidate> cands_;
  std::vector<Resources> suffix_;
  Resources deficit_;
  NodeId node_;
  uint64_t& budget_;
  std::vector<TaskId> chosen_;
  std::optional<Cost>* best_ = nullptr;
  std::vector<TaskId>* best_victims_ = nullptr;
  bool improved_ = false;
  bool truncated_ = false;
};

}  // namespace

std::optional<NodeId> Scheduler::pick_node(const ClusterState& state,
                                           const Task& task) const {
  std::optional<NodeId> best;
  Fraction best_score{0, 1};
  // Nodes are visited in id order and a node replaces the current best
  // only when it is STRICTLY better, so ties always go to the lowest id.
  for (const Node& n : state.nodes()) {
    if (!state.feasible(task, n)) continue;
    if (options_.strategy == Strategy::kFirstFit) return n.id;
    const Fraction score = leftover_on_dominant(n, task.request);
    const bool better = options_.strategy == Strategy::kBestFit
                            ? less(score, best_score)
                            : less(best_score, score);
    if (!best || better) {
      best = n.id;
      best_score = score;
    }
  }
  return best;
}

std::optional<PreemptionPlan> Scheduler::plan_preemption(
    const ClusterState& state, const Task& task) const {
  std::optional<Cost> best;
  std::vector<TaskId> best_victims;
  uint64_t budget = options_.preemption_search_budget;
  bool truncated = false;

  for (const Node& n : state.nodes()) {
    if (n.state != NodeState::kReady ||
        !task.request.fits_in(n.capacity) ||
        !ClusterState::labels_match(task.required_labels, n.labels)) {
      continue;
    }
    // Only strictly lower priority is ever a candidate: equal priority
    // would let two tasks evict each other forever, and higher priority
    // would invert the whole point of priorities.
    std::vector<Candidate> cands;
    Resources evictable;
    for (TaskId id : state.tasks_on(n.id)) {
      const Task& v = state.task(id);
      if (v.priority < task.priority) {
        cands.push_back({v.id, v.request, v.priority});
        evictable += v.request;
      }
    }
    const Resources free = n.free();
    // Deficit per dimension, never negative: the amount that must be freed.
    const Resources deficit{
        std::max<int64_t>(0, task.request.milli_cpu - free.milli_cpu),
        std::max<int64_t>(0, task.request.memory_mb - free.memory_mb)};
    if (!deficit.fits_in(evictable)) continue;  // cheap reject: no hope here

    // Cheapest-first order: lowest priority, then the largest share of
    // this node (bigger victims cover the deficit with fewer evictions),
    // then id. The share is a double because it only orders the search;
    // optimality comes from the exhaustive search, not from this order.
    auto share = [&](const Candidate& c) {
      return static_cast<double>(c.request.milli_cpu) /
                 static_cast<double>(n.capacity.milli_cpu) +
             static_cast<double>(c.request.memory_mb) /
                 static_cast<double>(n.capacity.memory_mb);
    };
    std::sort(cands.begin(), cands.end(),
              [&](const Candidate& a, const Candidate& b) {
                if (a.priority != b.priority) return a.priority < b.priority;
                const double sa = share(a), sb = share(b);
                if (sa != sb) return sa > sb;
                return a.id < b.id;
              });
    VictimSearch search(std::move(cands), deficit, n.id, budget);
    search.run(best, best_victims);
    truncated = truncated || search.truncated();
    if (budget == 0 && best) {
      // Out of budget with a plan in hand: remaining nodes are unexplored,
      // so the plan is valid but not proven cheapest.
      truncated = truncated || n.id + 1 < state.nodes().size();
      break;
    }
  }
  if (!best) return std::nullopt;
  PreemptionPlan plan;
  plan.node = std::get<3>(*best);
  plan.victims = std::move(best_victims);
  plan.max_victim_priority = std::get<1>(*best);
  plan.victim_priority_sum = std::get<2>(*best);
  plan.exhaustive = !truncated;
  return plan;
}

ScheduleResult Scheduler::schedule_pending(ClusterState& state) const {
  using Clock = std::chrono::steady_clock;
  ScheduleResult result;
  // Walk the pending queue by key rather than by iterator: placing a task
  // erases it from the set and preemption inserts victims, and both would
  // invalidate a plain iterator. upper_bound(previous key) always finds the
  // next task in order. Victims have strictly lower priority than their
  // preemptor, so their keys sort after the cursor and they get retried in
  // this same pass.
  //
  // Why the pass terminates, and visits each task at most once: a victim
  // is not running again until the cursor reaches its key, and every task
  // after that key has priority <= the victim's, so nothing later in the
  // pass can evict it a second time. Total visits <= pending at start +
  // tasks running at start.
  auto it = state.pending().begin();
  while (it != state.pending().end()) {
    const PendingKey key = *it;
    const Task& task = state.task(key.id);
    const auto start = Clock::now();
    ++result.decisions;

    if (auto node = pick_node(state, task)) {
      state.place(task.id, *node);
      result.placements.push_back({task.id, *node});
    } else if (auto plan = options_.enable_preemption
                               ? plan_preemption(state, task)
                               : std::nullopt) {
      if (!plan->exhaustive) ++result.truncated_searches;
      for (TaskId v : plan->victims) {
        state.evict(v);
        result.preemptions.push_back({v, task.id, plan->node});
      }
      // Cannot fail: the plan freed at least the deficit on a ready node
      // whose labels match. A false here would be a scheduler bug, and the
      // invariant checker in tests would report the task still pending.
      state.place(task.id, plan->node);
      result.placements.push_back({task.id, plan->node});
    } else {
      ++result.unschedulable;
    }

    if (options_.record_latency) {
      result.decision_ns.push_back(
          std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() -
                                                               start)
              .count());
    }
    it = state.pending().upper_bound(key);
  }
  return result;
}

}  // namespace fleet
