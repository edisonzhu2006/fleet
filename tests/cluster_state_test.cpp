// Tests for ClusterState: admission control, quota accounting, legal and
// illegal transitions, node state changes. Every test ends (and most steps
// are followed) by EXPECT_INVARIANTS, so bookkeeping drift fails loudly.

#include <gtest/gtest.h>

#include <stdexcept>

#include "cluster_state.h"
#include "helpers.h"

namespace fleet {
namespace {

using testing::submit_ok;

TEST(ClusterState, AddNodeRejectsBadCapacity) {
  ClusterState st;
  EXPECT_THROW(st.add_node({0, 1024}), std::invalid_argument);
  EXPECT_THROW(st.add_node({1000, -1}), std::invalid_argument);
  EXPECT_THROW(st.add_node({ClusterState::kMaxCapacity + 1, 1}),
               std::invalid_argument);
  EXPECT_EQ(st.add_node({1000, 1024}), 0u);
  EXPECT_EQ(st.add_node({1000, 1024}), 1u);
  EXPECT_INVARIANTS(st);
}

TEST(ClusterState, SubmitValidatesSpec) {
  ClusterState st;
  EXPECT_FALSE(st.submit({"", 1, {100, 100}, {}}).ok);
  EXPECT_FALSE(st.submit({"j", -1, {100, 100}, {}}).ok);
  EXPECT_FALSE(st.submit({"j", 4, {100, 100}, {}}).ok);
  EXPECT_FALSE(st.submit({"j", 1, {0, 100}, {}}).ok);
  EXPECT_FALSE(st.submit({"j", 1, {100, 0}, {}}).ok);
  SubmitResult r = st.submit({"j", 1, {100, 100}, {}});
  ASSERT_TRUE(r.ok) << r.reason;
  EXPECT_EQ(st.task(r.id).state, TaskState::kPending);
  EXPECT_EQ(st.pending().size(), 1u);
  EXPECT_INVARIANTS(st);
}

TEST(ClusterState, QuotaRejectsOverQuotaWithReason) {
  ClusterState st;
  st.set_quota("web", {2000, 4096});
  submit_ok(st, "web", 1, {1500, 1024});
  // CPU would go to 2500 > 2000.
  SubmitResult r = st.submit({"web", 1, {1000, 1024}, {}});
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.reason.find("over quota"), std::string::npos) << r.reason;
  EXPECT_NE(r.reason.find("web"), std::string::npos) << r.reason;
  // Memory would go to 5120 > 4096, even though CPU fits.
  EXPECT_FALSE(st.submit({"web", 1, {100, 4000}, {}}).ok);
  // Exactly at the limit is allowed.
  EXPECT_TRUE(st.submit({"web", 1, {500, 3072}, {}}).ok);
  // Other jobs are unaffected; jobs without a quota are unlimited.
  EXPECT_TRUE(st.submit({"batch", 0, {1'000'000, 1'000'000}, {}}).ok);
  EXPECT_EQ(st.quotas().at("web").used, (Resources{2000, 4096}));
  EXPECT_INVARIANTS(st);
}

TEST(ClusterState, QuotaIsReleasedOnlyWhenTaskEnds) {
  ClusterState st;
  NodeId n = st.add_node({4000, 8192});
  st.set_quota("j", {1000, 1000});
  TaskId t = submit_ok(st, "j", 1, {1000, 1000});
  EXPECT_FALSE(st.submit({"j", 1, {1, 1}, {}}).ok);
  ASSERT_TRUE(st.place(t, n));
  // Eviction puts the task back to pending; it is still alive, so it keeps
  // its quota (otherwise a preempted job could overfill its quota).
  ASSERT_TRUE(st.evict(t));
  EXPECT_FALSE(st.submit({"j", 1, {1, 1}, {}}).ok);
  ASSERT_TRUE(st.place(t, n));
  ASSERT_TRUE(st.finish(t, true));
  EXPECT_EQ(st.task(t).state, TaskState::kSucceeded);
  EXPECT_TRUE(st.submit({"j", 1, {1000, 1000}, {}}).ok);
  EXPECT_INVARIANTS(st);
}

TEST(ClusterState, LoweringQuotaBlocksOnlyNewSubmissions) {
  ClusterState st;
  st.set_quota("j", {2000, 2000});
  TaskId t = submit_ok(st, "j", 1, {1500, 1500});
  st.set_quota("j", {1000, 1000});
  EXPECT_EQ(st.task(t).state, TaskState::kPending);  // not killed
  EXPECT_FALSE(st.submit({"j", 1, {1, 1}, {}}).ok);
  EXPECT_INVARIANTS(st);
}

TEST(ClusterState, PlaceRejectsIllegalPlacements) {
  ClusterState st;
  NodeId small = st.add_node({1000, 1024});
  NodeId ssd = st.add_node({8000, 8192}, {{"disk", "ssd"}});
  NodeId big = st.add_node({8000, 8192});
  TaskId t = submit_ok(st, "j", 1, {2000, 512});
  TaskId needs_ssd = submit_ok(st, "j", 1, {100, 100}, {{"disk", "ssd"}});

  EXPECT_FALSE(st.place(t, small));        // does not fit
  EXPECT_FALSE(st.place(needs_ssd, big));  // label missing
  EXPECT_FALSE(st.place(t, 99));           // no such node
  EXPECT_FALSE(st.place(99, big));         // no such task
  st.set_node_state(big, NodeState::kDraining);
  EXPECT_FALSE(st.place(t, big));  // not ready
  st.set_node_state(big, NodeState::kReady);
  EXPECT_TRUE(st.place(t, big));
  EXPECT_FALSE(st.place(t, big));  // already running
  EXPECT_TRUE(st.place(needs_ssd, ssd));
  EXPECT_EQ(st.node(big).allocated, (Resources{2000, 512}));
  EXPECT_TRUE(st.pending().empty());
  EXPECT_INVARIANTS(st);
}

TEST(ClusterState, LabelValueMustMatch) {
  EXPECT_TRUE(ClusterState::labels_match({}, {{"a", "1"}}));
  EXPECT_TRUE(ClusterState::labels_match({{"a", "1"}}, {{"a", "1"}, {"b", "2"}}));
  EXPECT_FALSE(ClusterState::labels_match({{"a", "1"}}, {{"a", "2"}}));
  EXPECT_FALSE(ClusterState::labels_match({{"a", "1"}, {"c", "3"}}, {{"a", "1"}}));
}

TEST(ClusterState, DeadNodeReturnsTasksToPendingDrainingKeepsThem) {
  ClusterState st;
  NodeId a = st.add_node({4000, 4096});
  NodeId b = st.add_node({4000, 4096});
  TaskId t1 = submit_ok(st, "j", 1, {1000, 1000});
  TaskId t2 = submit_ok(st, "j", 1, {1000, 1000});
  TaskId t3 = submit_ok(st, "j", 1, {1000, 1000});
  ASSERT_TRUE(st.place(t1, a));
  ASSERT_TRUE(st.place(t2, a));
  ASSERT_TRUE(st.place(t3, b));
  st.set_node_state(a, NodeState::kDead);
  EXPECT_EQ(st.task(t1).state, TaskState::kPending);
  EXPECT_EQ(st.task(t2).state, TaskState::kPending);
  EXPECT_EQ(st.task(t3).state, TaskState::kRunning);
  EXPECT_EQ(st.task(t1).preempted_count, 0u);  // node loss is not preemption
  EXPECT_EQ(st.node(a).allocated, (Resources{}));
  EXPECT_TRUE(st.tasks_on(a).empty());
  EXPECT_INVARIANTS(st);
  // Draining (m3) leaves the task where it runs: the master moves it with
  // requeue() once the agent reports it stopped. No new placements there.
  st.set_node_state(b, NodeState::kDraining);
  EXPECT_EQ(st.task(t3).state, TaskState::kRunning);
  EXPECT_FALSE(st.place(t1, b));
  EXPECT_INVARIANTS(st);
  EXPECT_TRUE(st.requeue(t3));
  EXPECT_FALSE(st.requeue(t3));  // not running any more
  EXPECT_EQ(st.task(t3).preempted_count, 0u);  // a move is not a preemption
  EXPECT_EQ(st.pending().size(), 3u);
  EXPECT_INVARIANTS(st);
}

TEST(ClusterState, KillAndFinishTransitions) {
  ClusterState st;
  NodeId n = st.add_node({4000, 4096});
  TaskId pending = submit_ok(st, "j", 1, {1000, 1000});
  TaskId running = submit_ok(st, "j", 1, {1000, 1000});
  TaskId failing = submit_ok(st, "j", 1, {1000, 1000});
  ASSERT_TRUE(st.place(running, n));
  ASSERT_TRUE(st.place(failing, n));
  EXPECT_FALSE(st.finish(pending, true));  // only running tasks finish
  EXPECT_TRUE(st.kill(pending));
  EXPECT_TRUE(st.kill(running));
  EXPECT_FALSE(st.kill(running));  // already terminal
  EXPECT_TRUE(st.finish(failing, false));
  EXPECT_EQ(st.task(pending).state, TaskState::kKilled);
  EXPECT_EQ(st.task(running).state, TaskState::kKilled);
  EXPECT_EQ(st.task(failing).state, TaskState::kFailed);
  EXPECT_EQ(st.node(n).allocated, (Resources{}));
  EXPECT_EQ(st.quotas().at("j").used, (Resources{}));
  EXPECT_FALSE(st.evict(failing));
  EXPECT_INVARIANTS(st);
}

TEST(ClusterState, PendingQueueOrdersByPriorityThenAge) {
  ClusterState st;
  TaskId a = submit_ok(st, "j", 0, {1, 1});
  TaskId b = submit_ok(st, "j", 2, {1, 1});
  TaskId c = submit_ok(st, "j", 2, {1, 1});
  TaskId d = submit_ok(st, "j", 3, {1, 1});
  std::vector<TaskId> order;
  for (const auto& k : st.pending()) order.push_back(k.id);
  EXPECT_EQ(order, (std::vector<TaskId>{d, b, c, a}));
}

}  // namespace
}  // namespace fleet
