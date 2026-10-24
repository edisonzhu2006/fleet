#pragma once
// Scheduler: decides which node each pending task runs on (milestone 1).
//
// WHY THIS EXISTS. ClusterState knows what is legal; the Scheduler decides
// what is good. One pass of schedule_pending() visits every pending task in
// priority order (highest first, then oldest) and for each one:
//
//   1. Feasibility filter: nodes that accept tasks (ready and not
//      suspect), carry the task's required labels, and have enough free
//      CPU and memory right now.
//   2. Scoring: among feasible nodes, pick one by the configured Strategy.
//      The default, best-fit, packs tightly: it chooses the node that will
//      have the least room left on the task's dominant resource, so big
//      holes stay open for big tasks. Ties go to the lowest node id, so the
//      same input always produces the same placement.
//   3. Preemption, only if no node is feasible: find the cheapest set of
//      strictly lower-priority tasks on ONE node whose eviction makes the
//      task fit. Cheapest = fewest victims, then the lowest top victim
//      priority, then the lowest priority sum, then the lowest node id.
//      Victims go back to pending and are retried later in the same pass.
//
// PURE. The scheduler holds only its options; all state lives in the
// ClusterState passed in. That makes it trivially testable and lets the
// simulator run the three strategies on identical copies of a cluster.

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>
#include <vector>

#include "cluster_state.h"

namespace fleet {

enum class Strategy : uint8_t {
  kBestFit,   // least leftover on the dominant resource (pack)
  kWorstFit,  // most leftover on the dominant resource (spread)
  kFirstFit,  // lowest-id feasible node (the cheapest baseline)
};

std::string_view to_string(Strategy s);
std::optional<Strategy> parse_strategy(std::string_view name);

struct SchedulerOptions {
  Strategy strategy = Strategy::kBestFit;
  bool enable_preemption = true;
  // Finding the smallest victim set on a node is a two-dimensional covering
  // problem (NP-hard in general), so the exact search is capped at this many
  // search steps per preemption decision. The cap only applies once a plan
  // exists (the first plan found is the greedy one, which costs at most one
  // step per victim), so a feasible preemption is never missed; when the
  // cap is hit the best plan so far is used and ScheduleResult counts the
  // decision as truncated. Whether the m1 simulator hits it is in NOTES.md.
  uint64_t preemption_search_budget = 200'000;
  // Record wall time of every decision (for the simulator's percentiles).
  bool record_latency = false;
};

struct PreemptionPlan {
  NodeId node = 0;
  std::vector<TaskId> victims;  // in the order they were chosen
  int max_victim_priority = -1;
  int victim_priority_sum = 0;
  bool exhaustive = true;  // false if the search budget ran out
};

struct Placement {
  TaskId task;
  NodeId node;
};

struct PreemptionEvent {
  TaskId victim;
  TaskId preemptor;
  NodeId node;
};

struct ScheduleResult {
  size_t decisions = 0;      // pending tasks visited
  size_t unschedulable = 0;  // visits that left the task pending
  size_t truncated_searches = 0;
  std::vector<Placement> placements;
  std::vector<PreemptionEvent> preemptions;
  std::vector<int64_t> decision_ns;  // only if options.record_latency
};

class Scheduler {
 public:
  explicit Scheduler(SchedulerOptions options = {}) : options_(options) {}

  // One pass over the pending queue. Mutates state through its public
  // transitions only (place / evict), so invariants are enforced there.
  ScheduleResult schedule_pending(ClusterState& state) const;

  // Step 1 + 2 for a single task: the chosen feasible node, or nullopt.
  std::optional<NodeId> pick_node(const ClusterState& state,
                                  const Task& task) const;

  // Step 3 for a single task: the cheapest preemption plan, or nullopt if
  // evicting strictly lower-priority tasks cannot make room on any node.
  std::optional<PreemptionPlan> plan_preemption(const ClusterState& state,
                                                const Task& task) const;

  const SchedulerOptions& options() const { return options_; }

 private:
  SchedulerOptions options_;
};

}  // namespace fleet
