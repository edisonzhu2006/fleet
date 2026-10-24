// Tests for Scheduler placement: feasibility, the three strategies, the
// dominant-resource score, deterministic tie-breaking and priority order.

#include <gtest/gtest.h>

#include <cstdint>
#include <vector>

#include "cluster_state.h"
#include "helpers.h"
#include "scheduler.h"

namespace fleet {
namespace {

using testing::submit_ok;

Scheduler make(Strategy s) {
  SchedulerOptions o;
  o.strategy = s;
  return Scheduler(o);
}

// Three 8-core/8 GB nodes with 2, 6 and 4 cores already taken.
ClusterState three_partly_full_nodes() {
  ClusterState st;
  for (int64_t used : {2000, 6000, 4000}) {
    NodeId n = st.add_node({8000, 8192});
    TaskId filler = submit_ok(st, "filler", 3, {used, 1024});
    EXPECT_TRUE(st.place(filler, n));
  }
  return st;
}

TEST(Scheduler, BestFitPicksTightestNode) {
  ClusterState st = three_partly_full_nodes();
  TaskId t = submit_ok(st, "j", 1, {1000, 1024});
  auto r = make(Strategy::kBestFit).schedule_pending(st);
  ASSERT_EQ(r.placements.size(), 1u);
  EXPECT_EQ(*st.task(t).node, 1u);  // 6 of 8 cores used: least left over
  EXPECT_INVARIANTS(st);
}

TEST(Scheduler, WorstFitPicksEmptiestNode) {
  ClusterState st = three_partly_full_nodes();
  TaskId t = submit_ok(st, "j", 1, {1000, 1024});
  make(Strategy::kWorstFit).schedule_pending(st);
  EXPECT_EQ(*st.task(t).node, 0u);  // only 2 of 8 cores used
  EXPECT_INVARIANTS(st);
}

TEST(Scheduler, FirstFitPicksLowestFeasibleId) {
  ClusterState st = three_partly_full_nodes();
  TaskId t = submit_ok(st, "j", 1, {3000, 1024});  // fits nodes 0 and 2 only
  make(Strategy::kFirstFit).schedule_pending(st);
  EXPECT_EQ(*st.task(t).node, 0u);
  EXPECT_INVARIANTS(st);
}

TEST(Scheduler, BestFitSkipsInfeasibleTightNode) {
  ClusterState st = three_partly_full_nodes();
  TaskId t = submit_ok(st, "j", 1, {3000, 1024});  // node 1 has only 2000m
  make(Strategy::kBestFit).schedule_pending(st);
  EXPECT_EQ(*st.task(t).node, 2u);
  EXPECT_INVARIANTS(st);
}

TEST(Scheduler, ScoresOnTheDominantResource) {
  // Node 0: lots of CPU left, little memory left. Node 1: the opposite.
  ClusterState st;
  NodeId n0 = st.add_node({16000, 16384});
  NodeId n1 = st.add_node({16000, 16384});
  ASSERT_TRUE(st.place(submit_ok(st, "f", 3, {2000, 12288}), n0));
  ASSERT_TRUE(st.place(submit_ok(st, "f", 3, {12000, 2048}), n1));
  // Memory-dominant task (25% of memory, 6% of CPU): best-fit should judge
  // by memory, where node 0 is tightest (4096 free -> 0 left).
  TaskId mem_task = submit_ok(st, "j", 1, {1000, 4096});
  // CPU-dominant task (25% of CPU, 6% of memory): judged by CPU, where node
  // 1 is tightest (4000 free -> 0 left).
  TaskId cpu_task = submit_ok(st, "j", 1, {4000, 1024});
  make(Strategy::kBestFit).schedule_pending(st);
  EXPECT_EQ(*st.task(mem_task).node, n0);
  EXPECT_EQ(*st.task(cpu_task).node, n1);
  EXPECT_INVARIANTS(st);
}

TEST(Scheduler, TiesGoToLowestNodeId) {
  for (Strategy s : {Strategy::kBestFit, Strategy::kWorstFit, Strategy::kFirstFit}) {
    ClusterState st;
    for (int i = 0; i < 5; ++i) st.add_node({4000, 4096});
    TaskId t = submit_ok(st, "j", 1, {1000, 1024});
    make(s).schedule_pending(st);
    EXPECT_EQ(*st.task(t).node, 0u) << to_string(s);
  }
}

TEST(Scheduler, RespectsLabelsAndNodeState) {
  ClusterState st;
  NodeId plain = st.add_node({8000, 8192});
  NodeId ssd_draining = st.add_node({8000, 8192}, {{"disk", "ssd"}});
  NodeId ssd = st.add_node({8000, 8192}, {{"disk", "ssd"}});
  st.set_node_state(ssd_draining, NodeState::kDraining);
  TaskId t = submit_ok(st, "j", 1, {1000, 1024}, {{"disk", "ssd"}});
  TaskId any = submit_ok(st, "j", 1, {1000, 1024});
  make(Strategy::kBestFit).schedule_pending(st);
  EXPECT_EQ(*st.task(t).node, ssd);  // the only ready node with the label
  // The unlabeled task may go anywhere ready. t (older, same priority) was
  // placed first, so the ssd node is now the tighter one and best-fit
  // prefers it over the empty plain node.
  EXPECT_EQ(*st.task(any).node, ssd);
  EXPECT_TRUE(st.tasks_on(plain).empty());
  EXPECT_INVARIANTS(st);
}

TEST(Scheduler, UnplaceableTaskStaysPending) {
  ClusterState st;
  st.add_node({4000, 4096});
  TaskId big = submit_ok(st, "j", 1, {8000, 1024});
  auto r = make(Strategy::kBestFit).schedule_pending(st);
  EXPECT_EQ(st.task(big).state, TaskState::kPending);
  EXPECT_EQ(r.unschedulable, 1u);
  EXPECT_TRUE(r.placements.empty());
  EXPECT_INVARIANTS(st);
}

TEST(Scheduler, HigherPriorityIsPlacedFirstUnderContention) {
  ClusterState st;
  st.add_node({4000, 4096});
  // Submitted first, but lower priority.
  TaskId low = submit_ok(st, "j", 0, {3000, 1024});
  TaskId high = submit_ok(st, "j", 2, {3000, 1024});
  SchedulerOptions o;
  o.enable_preemption = false;  // isolate ordering from preemption
  Scheduler(o).schedule_pending(st);
  EXPECT_EQ(st.task(high).state, TaskState::kRunning);
  EXPECT_EQ(st.task(low).state, TaskState::kPending);
  EXPECT_INVARIANTS(st);
}

// Builds a random but seeded cluster and workload, schedules it, and
// returns the placement sequence. Used to show runs are reproducible.
std::vector<std::pair<TaskId, NodeId>> run_seeded(Strategy s, uint64_t seed) {
  uint64_t x = seed;
  auto next = [&x](uint64_t mod) {
    x = x * 6364136223846793005ULL + 1442695040888963407ULL;
    return (x >> 33) % mod;
  };
  ClusterState st;
  // Many identical nodes so ties are common: the case tie-breaking is for.
  for (int i = 0; i < 40; ++i) st.add_node({8000, 16384});
  for (int i = 0; i < 400; ++i) {
    submit_ok(st, "j", static_cast<int>(next(4)),
              {static_cast<int64_t>(250 * (1 + next(8))),
               static_cast<int64_t>(512 * (1 + next(8)))});
  }
  std::vector<std::pair<TaskId, NodeId>> out;
  SchedulerOptions o;
  o.strategy = s;
  Scheduler sched(o);
  for (int round = 0; round < 3; ++round) {
    auto r = sched.schedule_pending(st);
    for (const auto& p : r.placements) out.emplace_back(p.task, p.node);
    EXPECT_INVARIANTS(st);
    // Finish every third running task so later rounds see churn.
    for (const Task& t : st.tasks()) {
      if (t.state == TaskState::kRunning && t.id % 3 == 0) st.finish(t.id, true);
    }
    for (int i = 0; i < 100; ++i) {
      submit_ok(st, "j", static_cast<int>(next(4)),
                {static_cast<int64_t>(250 * (1 + next(8))),
                 static_cast<int64_t>(512 * (1 + next(8)))});
    }
  }
  return out;
}

TEST(Scheduler, SameInputSamePlacements) {
  for (Strategy s : {Strategy::kBestFit, Strategy::kWorstFit, Strategy::kFirstFit}) {
    auto a = run_seeded(s, 7);
    auto b = run_seeded(s, 7);
    EXPECT_FALSE(a.empty());
    EXPECT_EQ(a, b) << to_string(s);
  }
  // Sanity: the strategies really differ on this input.
  EXPECT_NE(run_seeded(Strategy::kBestFit, 7), run_seeded(Strategy::kWorstFit, 7));
}

TEST(Scheduler, ParseStrategy) {
  EXPECT_EQ(parse_strategy("best-fit"), Strategy::kBestFit);
  EXPECT_EQ(parse_strategy("worst"), Strategy::kWorstFit);
  EXPECT_EQ(parse_strategy("first-fit"), Strategy::kFirstFit);
  EXPECT_FALSE(parse_strategy("random").has_value());
}

}  // namespace
}  // namespace fleet
