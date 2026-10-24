// Tests for preemption: the victim set is the cheapest one (fewest victims,
// then lowest priorities), it is on a single node, and equal or higher
// priority work is never touched. The last test checks optimality against
// a brute-force search over every subset on small random clusters.

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <tuple>
#include <vector>

#include "cluster_state.h"
#include "helpers.h"
#include "scheduler.h"

namespace fleet {
namespace {

using testing::submit_ok;

TaskId run_on(ClusterState& st, NodeId n, int prio, Resources req) {
  TaskId t = submit_ok(st, "j", prio, req);
  EXPECT_TRUE(st.place(t, n));
  return t;
}

TEST(Preemption, PrefersFewestVictims) {
  ClusterState st;
  NodeId a = st.add_node({4000, 4096});
  NodeId b = st.add_node({4000, 4096});
  // Node a: four 1-core batch tasks. Node b: one 3-core and one 1-core.
  for (int i = 0; i < 4; ++i) run_on(st, a, 0, {1000, 1024});
  TaskId big = run_on(st, b, 0, {3000, 3072});
  TaskId small = run_on(st, b, 0, {1000, 1024});
  TaskId prod = submit_ok(st, "p", 2, {3000, 3072});
  auto r = Scheduler().schedule_pending(st);
  // Evicting only `big` on b beats evicting three tasks on a.
  ASSERT_EQ(r.preemptions.size(), 1u);
  EXPECT_EQ(r.preemptions[0].victim, big);
  EXPECT_EQ(r.preemptions[0].preemptor, prod);
  EXPECT_EQ(*st.task(prod).node, b);
  EXPECT_EQ(st.task(small).state, TaskState::kRunning);
  EXPECT_EQ(st.task(big).state, TaskState::kPending);
  EXPECT_EQ(st.task(big).preempted_count, 1u);
  EXPECT_INVARIANTS(st);
}

TEST(Preemption, SameCountPrefersLowerPriority) {
  ClusterState st;
  NodeId a = st.add_node({4000, 4096});
  NodeId b = st.add_node({4000, 4096});
  TaskId normal = run_on(st, a, 1, {4000, 4096});
  TaskId batch = run_on(st, b, 0, {4000, 4096});
  TaskId crit = submit_ok(st, "c", 3, {2000, 2048});
  Scheduler().schedule_pending(st);
  // One victim either way; the batch one is cheaper even though node a
  // has the lower id.
  EXPECT_EQ(*st.task(crit).node, b);
  EXPECT_EQ(st.task(normal).state, TaskState::kRunning);
  EXPECT_EQ(st.task(batch).state, TaskState::kPending);
  EXPECT_INVARIANTS(st);
}

TEST(Preemption, SameCountPrefersLowerTopPriorityOverLowerSum) {
  // {1, 1} vs {0, 2}: equal sums, but {1, 1} never touches production.
  ClusterState st;
  NodeId a = st.add_node({4000, 4096});
  NodeId b = st.add_node({4000, 4096});
  run_on(st, a, 0, {2000, 2048});
  TaskId a_prod = run_on(st, a, 2, {2000, 2048});
  TaskId b1 = run_on(st, b, 1, {2000, 2048});
  TaskId b2 = run_on(st, b, 1, {2000, 2048});
  TaskId crit = submit_ok(st, "c", 3, {4000, 4096});
  // Check the plan itself: running the pass would also let b1 (normal)
  // cascade onto node a by evicting the batch task, which is correct but
  // not what this test is about.
  auto plan = Scheduler().plan_preemption(st, st.task(crit));
  ASSERT_TRUE(plan.has_value());
  EXPECT_EQ(plan->node, b);
  EXPECT_EQ(plan->victims, (std::vector<TaskId>{b1, b2}));
  EXPECT_EQ(plan->max_victim_priority, 1);
  Scheduler().schedule_pending(st);
  EXPECT_EQ(*st.task(crit).node, b);
  EXPECT_EQ(st.task(a_prod).state, TaskState::kRunning);  // never touched
  EXPECT_INVARIANTS(st);
}

TEST(Preemption, NeverTouchesEqualOrHigherPriority) {
  ClusterState st;
  NodeId a = st.add_node({4000, 4096});
  NodeId b = st.add_node({4000, 4096});
  TaskId same = run_on(st, a, 2, {4000, 4096});
  TaskId higher = run_on(st, b, 3, {4000, 4096});
  TaskId t = submit_ok(st, "p", 2, {1000, 1024});
  auto r = Scheduler().schedule_pending(st);
  EXPECT_TRUE(r.preemptions.empty());
  EXPECT_EQ(st.task(t).state, TaskState::kPending);
  EXPECT_EQ(st.task(same).state, TaskState::kRunning);
  EXPECT_EQ(st.task(higher).state, TaskState::kRunning);
  EXPECT_FALSE(Scheduler().plan_preemption(st, st.task(t)).has_value());
  EXPECT_INVARIANTS(st);
}

TEST(Preemption, DoesNotPreemptWhenTheTaskFitsSomewhere) {
  ClusterState st;
  NodeId full = st.add_node({4000, 4096});
  NodeId empty = st.add_node({4000, 4096});
  TaskId batch = run_on(st, full, 0, {4000, 4096});
  TaskId t = submit_ok(st, "p", 3, {1000, 1024});
  auto r = Scheduler().schedule_pending(st);
  EXPECT_TRUE(r.preemptions.empty());
  EXPECT_EQ(*st.task(t).node, empty);
  EXPECT_EQ(st.task(batch).state, TaskState::kRunning);
}

TEST(Preemption, VictimsAreRescheduledInTheSamePass) {
  // The critical task needs the gpu label, so it can only run on node a,
  // where it evicts the batch task; the batch task then lands on the
  // unlabeled node b later in the same pass.
  ClusterState st;
  NodeId a = st.add_node({4000, 4096}, {{"gpu", "yes"}});
  NodeId b = st.add_node({4000, 4096});
  TaskId batch = run_on(st, a, 0, {4000, 4096});
  TaskId crit = submit_ok(st, "c", 3, {4000, 4096}, {{"gpu", "yes"}});
  auto r = Scheduler().schedule_pending(st);
  EXPECT_EQ(*st.task(crit).node, a);
  EXPECT_EQ(*st.task(batch).node, b);
  ASSERT_EQ(r.preemptions.size(), 1u);
  EXPECT_EQ(r.placements.size(), 2u);
  EXPECT_INVARIANTS(st);
}

TEST(Preemption, CascadeWithinOnePass) {
  // crit (gpu only) evicts normal from a; normal then evicts batch from b;
  // batch has nothing lower to evict and stays pending.
  ClusterState st;
  NodeId a = st.add_node({4000, 4096}, {{"gpu", "yes"}});
  NodeId b = st.add_node({4000, 4096});
  TaskId normal = run_on(st, a, 1, {4000, 4096});
  TaskId batch = run_on(st, b, 0, {4000, 4096});
  TaskId crit = submit_ok(st, "c", 3, {4000, 4096}, {{"gpu", "yes"}});
  auto r = Scheduler().schedule_pending(st);
  EXPECT_EQ(*st.task(crit).node, a);
  EXPECT_EQ(*st.task(normal).node, b);
  EXPECT_EQ(st.task(batch).state, TaskState::kPending);
  ASSERT_EQ(r.preemptions.size(), 2u);
  EXPECT_EQ(r.preemptions[0].preemptor, crit);
  EXPECT_EQ(r.preemptions[1].preemptor, normal);
  EXPECT_EQ(r.decisions, 3u);  // crit, normal, batch: each visited once
  EXPECT_INVARIANTS(st);
}

TEST(Preemption, CanBeDisabled) {
  ClusterState st;
  NodeId a = st.add_node({4000, 4096});
  run_on(st, a, 0, {4000, 4096});
  TaskId t = submit_ok(st, "p", 3, {1000, 1024});
  SchedulerOptions o;
  o.enable_preemption = false;
  auto r = Scheduler(o).schedule_pending(st);
  EXPECT_TRUE(r.preemptions.empty());
  EXPECT_EQ(st.task(t).state, TaskState::kPending);
}

// ---- Optimality against brute force ------------------------------------

using Cost = std::tuple<size_t, int, int, NodeId>;

// Every subset of every node's lower-priority tasks; the minimum cost of
// the subsets that make the task fit. Exponential, fine for <= 10 tasks.
std::optional<Cost> brute_force(const ClusterState& st, const Task& task) {
  std::optional<Cost> best;
  for (const Node& n : st.nodes()) {
    if (n.state != NodeState::kReady || !task.request.fits_in(n.capacity) ||
        !ClusterState::labels_match(task.required_labels, n.labels)) {
      continue;
    }
    std::vector<const Task*> cands;
    for (TaskId id : st.tasks_on(n.id)) {
      if (st.task(id).priority < task.priority) cands.push_back(&st.task(id));
    }
    for (uint32_t mask = 0; mask < (1u << cands.size()); ++mask) {
      Resources freed;
      int max_p = -1, sum_p = 0;
      size_t count = 0;
      for (size_t i = 0; i < cands.size(); ++i) {
        if (mask & (1u << i)) {
          freed += cands[i]->request;
          max_p = std::max(max_p, cands[i]->priority);
          sum_p += cands[i]->priority;
          ++count;
        }
      }
      if (!task.request.fits_in(n.free() + freed)) continue;
      Cost c{count, max_p, sum_p, n.id};
      if (!best || c < *best) best = c;
    }
  }
  return best;
}

TEST(Preemption, MatchesBruteForceOnRandomClusters) {
  uint64_t x = 12345;
  auto next = [&x](uint64_t mod) {
    x = x * 6364136223846793005ULL + 1442695040888963407ULL;
    return (x >> 33) % mod;
  };
  int compared = 0, with_plan = 0;
  for (int trial = 0; trial < 400; ++trial) {
    ClusterState st;
    const int nodes = 2 + static_cast<int>(next(4));
    for (int i = 0; i < nodes; ++i) {
      st.add_node({static_cast<int64_t>(4000 + 1000 * next(5)),
                   static_cast<int64_t>(4096 + 2048 * next(5))});
    }
    // Fill nodes with up to 10 random tasks each (first-fit, any priority).
    for (int i = 0; i < nodes * 10; ++i) {
      TaskId t = submit_ok(st, "f", static_cast<int>(next(4)),
                           {static_cast<int64_t>(500 + 250 * next(8)),
                            static_cast<int64_t>(512 + 256 * next(12))});
      for (const Node& n : st.nodes()) {
        if (st.tasks_on(n.id).size() < 10 && st.place(t, n.id)) break;
      }
      if (st.task(t).state == TaskState::kPending) st.kill(t);
    }
    TaskId t = submit_ok(st, "p", 1 + static_cast<int>(next(3)),
                         {static_cast<int64_t>(1000 + 500 * next(8)),
                          static_cast<int64_t>(1024 + 1024 * next(8))});
    const Task& task = st.task(t);
    Scheduler sched;
    if (sched.pick_node(st, task)) continue;  // fits without preemption
    ++compared;
    auto plan = sched.plan_preemption(st, task);
    auto expect = brute_force(st, task);
    ASSERT_EQ(plan.has_value(), expect.has_value()) << "trial " << trial;
    if (!plan) continue;
    ++with_plan;
    EXPECT_TRUE(plan->exhaustive);
    Cost got{plan->victims.size(), plan->max_victim_priority,
             plan->victim_priority_sum, plan->node};
    EXPECT_EQ(got, *expect) << "trial " << trial;
    // Applying the plan must make the task fit, touching only lower bands.
    for (TaskId v : plan->victims) {
      EXPECT_LT(st.task(v).priority, task.priority);
      EXPECT_EQ(*st.task(v).node, plan->node);
      ASSERT_TRUE(st.evict(v));
    }
    EXPECT_TRUE(st.place(t, plan->node));
    EXPECT_INVARIANTS(st);
  }
  // The random instances must actually exercise preemption.
  EXPECT_GT(compared, 100);
  EXPECT_GT(with_plan, 50);
}

TEST(Preemption, TruncatedSearchStillReturnsAValidPlan) {
  ClusterState st;
  NodeId a = st.add_node({32000, 32768});
  for (int i = 0; i < 64; ++i) run_on(st, a, 0, {500, 512});
  TaskId t = submit_ok(st, "p", 3, {8000, 8192});
  SchedulerOptions o;
  o.preemption_search_budget = 5;  // far smaller than the 16-step greedy descent
  auto plan = Scheduler(o).plan_preemption(st, st.task(t));
  ASSERT_TRUE(plan.has_value());
  EXPECT_FALSE(plan->exhaustive);
  Resources freed;
  for (TaskId v : plan->victims) freed += st.task(v).request;
  EXPECT_TRUE(st.task(t).request.fits_in(st.node(a).free() + freed));
}

}  // namespace
}  // namespace fleet
