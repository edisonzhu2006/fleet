// Tests for m3 in MasterCore, with no network and a fake clock: a silent
// node becomes suspect then dead and its tasks are rescheduled; fencing by
// epoch (stale and revoked epochs are refused, their reports ignored);
// restart policies with exponential backoff; graceful drain's edge cases.
// Every test steps time by hand and checks check_invariants() as it goes.

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "master_core.h"
#include "master_test_helpers.h"

namespace fleet {
namespace {

using namespace fleet::testing;
using namespace std::chrono_literals;

// Fast backoff so the schedule is easy to read: 100, 200, 400, 800, 800...
MasterOptions fast_backoff() {
  MasterOptions o;
  o.backoff = {100ms, 800ms, 10s};
  return o;
}

// ------------------------------------------------- detection and rescheduling

TEST(Failover, SilentNodeIsSuspectThenDeadAndItsTasksMove) {
  MasterCore core({}, 1);  // 500 ms heartbeats, suspect 3, dead 6
  FakeAgent a = agent_for(register_node(core, "a", 2000, 2048));
  FakeAgent b = agent_for(register_node(core, "b", 2000, 2048));
  // Two tasks that fill one node each, so where each lands is forced.
  auto ids = submit(core, "svc", 1, 2000, 512, 2);
  core.tick(kT0);
  const NodeId first_node = *core.state().task(ids[0]).node;
  ASSERT_NE(first_node, *core.state().task(ids[1]).node);
  const TaskId on_b = first_node == b.node_id ? ids[0] : ids[1];
  const TaskId on_a = on_b == ids[0] ? ids[1] : ids[0];

  // a heartbeats every 500 ms; b falls silent at t0. Step the fake clock
  // in 100 ms ticks (the master's default) and note when b changes.
  Clock::time_point suspect_at{}, dead_at{};
  for (auto t = kT0 + 100ms; t <= kT0 + 4000ms; t += 100ms) {
    if ((t - kT0) % 500ms == 0ms) {
      a.running(on_a, core.attempt(on_a), a.epoch);
      a.beat(core, t);
    }
    const TickResult r = core.tick(t);
    if (!r.suspected.empty()) suspect_at = t;
    if (!r.died.empty()) {
      dead_at = t;
      EXPECT_EQ(r.died, std::vector<NodeId>{b.node_id});
    }
    EXPECT_CORE_OK(core);
  }
  EXPECT_EQ(suspect_at, kT0 + 1500ms);
  EXPECT_EQ(dead_at, kT0 + 3000ms);
  EXPECT_EQ(core.state().node(b.node_id).state, NodeState::kDead);
  EXPECT_EQ(core.epoch(b.node_id), 0u);  // revoked
  EXPECT_EQ(core.state().node(a.node_id).state, NodeState::kReady);
  // b's task went back to pending; it cannot fit on a (full), so it waits.
  EXPECT_EQ(core.state().task(on_b).state, TaskState::kPending);
  EXPECT_EQ(core.state().task(on_b).preempted_count, 0u);  // not a preemption
  EXPECT_EQ(core.restarts(on_b), 0u);                      // not a restart either

  // Room appears on a: the next tick places b's task there, at attempt 2,
  // and a's next heartbeat carries the start.
  a.exited(on_a, 1, a.epoch, 0);
  a.beat(core, kT0 + 4100ms);
  EXPECT_EQ(core.state().task(on_b).node, a.node_id);
  EXPECT_EQ(core.attempt(on_b), 2u);
  auto resp = a.beat(core, kT0 + 4600ms);
  ASSERT_EQ(resp.start_size(), 1);
  EXPECT_EQ(resp.start(0).instance().task_id(), on_b);
  EXPECT_EQ(resp.start(0).instance().attempt(), 2u);
  EXPECT_CORE_OK(core);
}

TEST(Failover, SuspectNodeKeepsItsTasksButGetsNoNewOnesUntilItIsHeardFrom) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  FakeAgent b = agent_for(register_node(core, "b"));
  // Keep b healthy; let a go quiet for 2 s (suspect, not dead).
  b.beat(core, kT0 + 1000ms);
  b.beat(core, kT0 + 1500ms);
  core.tick(kT0 + 1600ms);
  ASSERT_TRUE(core.state().node(a.node_id).suspect);
  // Best-fit would pick either empty node by id; a is suspect, so b.
  auto ids = submit(core, "web", 1, 1000, 512, 3);
  core.tick(kT0 + 1700ms);
  for (TaskId id : ids) EXPECT_EQ(core.state().task(id).node, b.node_id);
  // a speaks again before 3 s: no longer suspect, and takes work again.
  a.beat(core, kT0 + 2000ms);
  EXPECT_FALSE(core.state().node(a.node_id).suspect);
  EXPECT_EQ(core.state().node(a.node_id).state, NodeState::kReady);
  auto more = submit(core, "big", 1, 3000, 512);
  core.tick(kT0 + 2100ms);
  EXPECT_EQ(core.state().task(more[0]).node, a.node_id);
  EXPECT_CORE_OK(core);
}

TEST(Failover, ATickStallDeclaresDeathOnceNotOnEveryTick) {
  MasterCore core({}, 1);
  register_node(core, "a");
  const TickResult r = core.tick(kT0 + 60s);  // nothing ran for a minute
  EXPECT_EQ(r.died.size(), 1u);
  EXPECT_TRUE(core.tick(kT0 + 61s).died.empty());
  EXPECT_CORE_OK(core);
}

// ----------------------------------------------------------------- fencing

TEST(Fencing, EpochsOnlyGoUp) {
  MasterCore core({}, 1000);
  uint64_t last = 0;
  for (int i = 0; i < 5; ++i) {
    for (const char* n : {"a", "b"}) {
      const uint64_t e = register_node(core, n).epoch();
      EXPECT_GT(e, last);
      last = e;
    }
  }
  // A master started later begins above everything an earlier one issued
  // (the default seed is the wall clock in microseconds), so a restarted
  // master's epochs cannot collide with ones its predecessor handed out.
  MasterCore before;
  const uint64_t old_epoch = register_node(before, "a").epoch();
  std::this_thread::sleep_for(2ms);
  MasterCore after;
  EXPECT_GT(register_node(after, "a").epoch(), old_epoch);
}

TEST(Fencing, DeadNodeIsRefusedAndItsReportIgnoredUntilItRegistersAgain) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  FakeAgent b = agent_for(register_node(core, "b"));
  auto ids = submit(core, "svc", 1, 3000, 512);
  core.tick(kT0);
  const NodeId first = *core.state().task(ids[0]).node;
  FakeAgent& gone = first == a.node_id ? a : b;  // will be partitioned
  FakeAgent& live = first == a.node_id ? b : a;
  const uint64_t old_epoch = gone.epoch;
  gone.running(ids[0], 1, gone.epoch);
  gone.beat(core, kT0);

  // gone is cut off for 3 s; live keeps beating. gone is declared dead and
  // its task restarts on live at attempt 2.
  for (auto t = kT0 + 500ms; t <= kT0 + 3000ms; t += 500ms) {
    live.beat(core, t);
    core.tick(t);
  }
  ASSERT_EQ(core.state().node(gone.node_id).state, NodeState::kDead);
  ASSERT_EQ(core.state().task(ids[0]).node, live.node_id);
  ASSERT_EQ(core.attempt(ids[0]), 2u);
  live.running(ids[0], 2, live.epoch);
  live.beat(core, kT0 + 3500ms);

  // The partition heals. gone still runs attempt 1 and reports it, plus an
  // exit of something else. The whole heartbeat is refused with the
  // fencing status, and nothing in it is applied: the exit does not
  // finish anything and the heartbeat does not revive the node.
  gone.running(ids[0], 1, old_epoch);
  gone.exited(ids[0], 1, old_epoch, 0);
  v1::HeartbeatResponse resp;
  grpc::Status s = gone.try_beat(core, &resp, kT0 + 3600ms);
  EXPECT_EQ(s.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
  EXPECT_NE(s.error_message().find("declared dead"), std::string::npos) << s.error_message();
  EXPECT_NE(s.error_message().find("kill all tasks"), std::string::npos);
  EXPECT_EQ(resp.start_size() + resp.stop_size() + resp.forget_size(), 0);
  EXPECT_EQ(core.state().task(ids[0]).state, TaskState::kRunning);  // still attempt 2 on live
  EXPECT_EQ(core.state().task(ids[0]).node, live.node_id);
  EXPECT_EQ(core.state().node(gone.node_id).state, NodeState::kDead);

  // The agent kills everything and registers again: a new, larger epoch,
  // and the node is ready again (empty).
  auto again = register_node(core, gone.node_id == a.node_id ? "a" : "b", 4000, 4096,
                             kT0 + 3700ms);
  EXPECT_EQ(again.node_id(), gone.node_id);
  EXPECT_GT(again.epoch(), old_epoch);
  EXPECT_GT(again.epoch(), live.epoch);
  EXPECT_EQ(core.state().node(gone.node_id).state, NodeState::kReady);
  EXPECT_TRUE(core.state().tasks_on(gone.node_id).empty());

  // Had any old process survived the kill, its report under the old
  // epoch would get a stop, and its exit would be acknowledged but never
  // applied.
  FakeAgent back = agent_for(again);
  back.running(ids[0], 1, old_epoch);
  resp = back.beat(core, kT0 + 3800ms);
  ASSERT_EQ(resp.stop_size(), 1);
  EXPECT_EQ(resp.stop(0).epoch(), old_epoch);
  back.exited(ids[0], 1, old_epoch, 1);
  resp = back.beat(core, kT0 + 3900ms);
  EXPECT_EQ(resp.forget_size(), 1);
  EXPECT_EQ(core.state().task(ids[0]).state, TaskState::kRunning);
  EXPECT_EQ(core.state().task(ids[0]).node, live.node_id);
  EXPECT_CORE_OK(core);
}

TEST(Fencing, AHeartbeatWithAnOlderEpochIsRefusedEvenWhileTheNodeIsAlive) {
  MasterCore core({}, 1);
  auto first = register_node(core, "a");
  auto second = register_node(core, "a");  // e.g. a second agent with the same name
  FakeAgent old_agent = agent_for(first);
  v1::HeartbeatResponse resp;
  grpc::Status s = old_agent.try_beat(core, &resp);
  EXPECT_EQ(s.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
  EXPECT_NE(s.error_message().find("stale epoch"), std::string::npos) << s.error_message();
  FakeAgent new_agent = agent_for(second);
  EXPECT_TRUE(new_agent.try_beat(core, &resp).ok());
  EXPECT_CORE_OK(core);
}

// ---------------------------------------------------------- restart policies

TEST(Restart, OnFailureBacksOffExponentiallyAndStopsOnSuccess) {
  MasterCore core(fast_backoff(), 1);
  FakeAgent ag = agent_for(register_node(core, "a"));
  auto ids = submit(core, "flaky", 1, 1000, 512, 1, v1::RESTART_POLICY_ON_FAILURE);
  const TaskId id = ids[0];
  Clock::time_point t = kT0;
  core.tick(t);

  // Fail five times in a row, each right after starting: the delays are
  // 100, 200, 400, 800, 800 ms (capped).
  const std::vector<std::chrono::milliseconds> want = {100ms, 200ms, 400ms, 800ms, 800ms};
  for (size_t i = 0; i < want.size(); ++i) {
    ASSERT_EQ(core.state().task(id).state, TaskState::kRunning) << i;
    const uint32_t att = core.attempt(id);
    EXPECT_EQ(att, i + 1);
    t += 10ms;
    ag.exited(id, att, ag.epoch, 1);
    ag.beat(core, t);
    ASSERT_EQ(core.state().task(id).state, TaskState::kBackoff) << i;
    EXPECT_EQ(core.restart_at(id) - t, want[i]) << i;
    EXPECT_EQ(core.restarts(id), i + 1);
    EXPECT_CORE_OK(core);
    // Not one millisecond early...
    core.tick(t + want[i] - 1ms);
    EXPECT_EQ(core.state().task(id).state, TaskState::kBackoff) << i;
    // ...and placed on the tick at which the delay is over.
    const TickResult r = core.tick(t + want[i]);
    EXPECT_EQ(r.restarted, std::vector<TaskId>{id});
    EXPECT_EQ(core.state().task(id).state, TaskState::kRunning);
    t += want[i];
  }
  // Exit 0: on-failure is done with it.
  ag.exited(id, core.attempt(id), ag.epoch, 0);
  ag.beat(core, t + 10ms);
  EXPECT_EQ(core.state().task(id).state, TaskState::kSucceeded);
  EXPECT_EQ(core.attempt(id), 6u);
  EXPECT_EQ(core.restarts(id), 5u);
  EXPECT_CORE_OK(core);
}

TEST(Restart, BackoffResetsAfterAnAttemptThatStayedUp) {
  MasterCore core(fast_backoff(), 1);  // reset_after 10 s
  FakeAgent ag = agent_for(register_node(core, "a"));
  const TaskId id = submit(core, "svc", 1, 1000, 512, 1, v1::RESTART_POLICY_ALWAYS)[0];
  Clock::time_point t = kT0;
  core.tick(t);
  // Three quick exits: 100, 200, 400.
  for (int i = 0; i < 3; ++i) {
    ag.exited(id, core.attempt(id), ag.epoch, 0);  // even exit 0 restarts under always
    ag.beat(core, t);
    t = core.restart_at(id);
    core.tick(t);
  }
  EXPECT_EQ(core.restarts(id), 3u);
  // The fourth attempt runs for 10 s before exiting: the loop is over, so
  // the delay is back to the base instead of 800 ms.
  t += 10s;
  ag.exited(id, core.attempt(id), ag.epoch, 1);
  ag.beat(core, t);
  EXPECT_EQ(core.restart_at(id) - t, 100ms);
  EXPECT_CORE_OK(core);
}

TEST(Restart, NeverMeansTheFirstExitIsFinal) {
  MasterCore core(fast_backoff(), 1);
  FakeAgent ag = agent_for(register_node(core, "a"));
  auto unset = submit(core, "m2client", 1, 500, 64);  // UNSPECIFIED: never
  auto never = submit(core, "batch", 1, 500, 64, 1, v1::RESTART_POLICY_NEVER);
  core.tick(kT0);
  ag.exited(unset[0], 1, ag.epoch, 3);
  ag.exited(never[0], 1, ag.epoch, 3);
  ag.beat(core, kT0 + 10ms);
  EXPECT_EQ(core.state().task(unset[0]).state, TaskState::kFailed);
  EXPECT_EQ(core.state().task(never[0]).state, TaskState::kFailed);
  EXPECT_EQ(core.restarts(never[0]), 0u);
  v1::SubmitRequest bad;
  bad.set_job("x");
  bad.mutable_request()->set_milli_cpu(100);
  bad.mutable_request()->set_memory_mb(64);
  bad.add_argv("true");
  bad.set_restart_policy(static_cast<v1::RestartPolicy>(42));  // from a newer client
  v1::SubmitResponse resp;
  EXPECT_EQ(core.submit(bad, &resp).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_CORE_OK(core);
}

TEST(Restart, StartFailureCountsAsAFailure) {
  MasterCore core(fast_backoff(), 1);
  FakeAgent ag = agent_for(register_node(core, "a"));
  const TaskId id = submit(core, "typo", 1, 500, 64, 1, v1::RESTART_POLICY_ON_FAILURE)[0];
  core.tick(kT0);
  auto* st = ag.req.add_tasks();
  st->mutable_instance()->set_task_id(id);
  st->mutable_instance()->set_attempt(1);
  st->mutable_instance()->set_epoch(ag.epoch);
  st->set_phase(v1::TaskStatus::PHASE_START_FAILED);  // exit code 0, but it never ran
  st->set_message("command not found: tpyo");
  ag.beat(core, kT0 + 10ms);
  EXPECT_EQ(core.state().task(id).state, TaskState::kBackoff);
  EXPECT_CORE_OK(core);
}

TEST(Restart, KillDuringBackoffIsFinalAndCancelsTheTimer) {
  MasterCore core(fast_backoff(), 1);
  FakeAgent ag = agent_for(register_node(core, "a"));
  const TaskId id = submit(core, "svc", 1, 1000, 512, 1, v1::RESTART_POLICY_ALWAYS)[0];
  core.tick(kT0);
  ag.exited(id, 1, ag.epoch, 1);
  ag.beat(core, kT0);
  ASSERT_EQ(core.state().task(id).state, TaskState::kBackoff);
  v1::ListResponse list;
  ASSERT_TRUE(core.list({}, &list, kT0 + 40ms).ok());
  EXPECT_EQ(list.tasks(0).state(), v1::TASK_STATE_BACKOFF);
  EXPECT_EQ(list.tasks(0).restart_in_ms(), 60);
  EXPECT_EQ(list.tasks(0).restarts(), 1u);
  EXPECT_EQ(list.tasks(0).restart_policy(), v1::RESTART_POLICY_ALWAYS);

  v1::KillRequest kreq;
  kreq.set_task_id(id);
  v1::KillResponse kresp;
  ASSERT_TRUE(core.kill(kreq, &kresp).ok());
  EXPECT_EQ(core.state().task(id).state, TaskState::kKilled);
  EXPECT_CORE_OK(core);  // includes: no timer left for it
  EXPECT_TRUE(core.tick(kT0 + 1s).restarted.empty());
  EXPECT_EQ(core.state().task(id).state, TaskState::kKilled);
  // Quota is released only now; while in backoff it stayed charged.
  EXPECT_EQ(core.state().quotas().at("svc").used, Resources{});
}

TEST(Restart, NodeDeathIsNotTheTasksFaultAndSkipsBackoff) {
  MasterCore core(fast_backoff(), 1);
  register_node(core, "a");
  FakeAgent b = agent_for(register_node(core, "b"));
  const TaskId id = submit(core, "svc", 1, 3000, 512, 1, v1::RESTART_POLICY_ON_FAILURE)[0];
  core.tick(kT0);
  // Whichever node got it, let it die while the other stays alive.
  const NodeId victim = *core.state().task(id).node;
  FakeAgent other = victim == b.node_id ? FakeAgent{0, core.epoch(0), {}} : b;
  for (auto t = kT0 + 500ms; t <= kT0 + 3000ms; t += 500ms) {
    other.beat(core, t);
    core.tick(t);
  }
  // Rescheduled at once in the same tick (no backoff), not counted.
  EXPECT_EQ(core.state().task(id).state, TaskState::kRunning);
  EXPECT_EQ(core.state().task(id).node, other.node_id);
  EXPECT_EQ(core.restarts(id), 0u);
  EXPECT_CORE_OK(core);
}

// ------------------------------------------------------------------ drain

TEST(Drain, TaskPlacedButNeverStartedMovesAtTheNextHeartbeat) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a", 4000, 4096));
  FakeAgent b = agent_for(register_node(core, "b", 2000, 2048));
  const TaskId id = submit(core, "web", 1, 2000, 512)[0];
  core.tick(kT0);
  ASSERT_EQ(core.state().task(id).node, b.node_id);  // best-fit: the tighter node
  v1::DrainRequest dreq;
  dreq.set_node("b");
  v1::DrainResponse dresp;
  ASSERT_TRUE(core.drain(dreq, &dresp).ok());
  // b's first heartbeat after the drain does not report the task: the
  // start never reached it, and no start is in flight (none is sent to a
  // draining node). It moves now, with no stop.
  auto resp = b.beat(core);
  EXPECT_EQ(resp.start_size(), 0);
  EXPECT_EQ(resp.stop_size(), 0);
  EXPECT_EQ(core.state().task(id).state, TaskState::kPending);
  EXPECT_TRUE(core.needs_schedule());
  core.tick(kT0);
  EXPECT_EQ(core.state().task(id).node, a.node_id);
  EXPECT_EQ(core.attempt(id), 2u);
  EXPECT_CORE_OK(core);
}

TEST(Drain, AnExitBeforeTheStopWasSentIsTheTasksOwn) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  register_node(core, "b");
  const TaskId id = submit(core, "batch", 0, 3000, 512)[0];
  core.tick(kT0);
  ASSERT_EQ(core.state().task(id).node, a.node_id);
  v1::DrainRequest dreq;
  dreq.set_node("a");
  v1::DrainResponse dresp;
  ASSERT_TRUE(core.drain(dreq, &dresp).ok());
  // The batch task finished on its own before any heartbeat carried a
  // stop: it succeeded, and a policy-never task is not rerun.
  a.exited(id, 1, a.epoch, 0);
  a.beat(core);
  EXPECT_EQ(core.state().task(id).state, TaskState::kSucceeded);
  EXPECT_CORE_OK(core);
}

TEST(Drain, ADrainingNodeThatDiesHandsItsTasksOverAtOnce) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  FakeAgent b = agent_for(register_node(core, "b"));
  const TaskId id = submit(core, "web", 1, 3000, 512)[0];
  core.tick(kT0);
  FakeAgent& on = *core.state().task(id).node == a.node_id ? a : b;
  FakeAgent& other = &on == &a ? b : a;
  on.running(id, 1, on.epoch);
  on.beat(core);
  v1::DrainRequest dreq;
  dreq.set_node(&on == &a ? "a" : "b");
  v1::DrainResponse dresp;
  ASSERT_TRUE(core.drain(dreq, &dresp).ok());
  on.running(id, 1, on.epoch);
  ASSERT_EQ(on.beat(core).stop_size(), 1);  // stop sent: the task is moving
  // The node never reports the exit; it goes silent and is declared dead.
  for (auto t = kT0 + 500ms; t <= kT0 + 3000ms; t += 500ms) {
    other.beat(core, t);
    core.tick(t);
  }
  EXPECT_EQ(core.state().node(on.node_id).state, NodeState::kDead);
  EXPECT_EQ(core.state().task(id).node, other.node_id);
  EXPECT_EQ(core.attempt(id), 2u);
  // A dead node cannot be drained; it has nothing left to move.
  EXPECT_EQ(core.drain(dreq, &dresp).error_code(), grpc::StatusCode::FAILED_PRECONDITION);
  // When it comes back, the operator's drain still holds.
  register_node(core, &on == &a ? "a" : "b", 4000, 4096, kT0 + 3100ms);
  EXPECT_EQ(core.state().node(on.node_id).state, NodeState::kDraining);
  EXPECT_CORE_OK(core);
}

TEST(Drain, DrainingTwiceIsANoOpAndADrainedNodeStaysDrainedAcrossReRegistration) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  register_node(core, "b");
  const TaskId id = submit(core, "web", 1, 3000, 512)[0];
  core.tick(kT0);
  const std::string name = *core.state().task(id).node == a.node_id ? "a" : "b";
  v1::DrainRequest dreq;
  dreq.set_node(name);
  v1::DrainResponse r1, r2;
  ASSERT_TRUE(core.drain(dreq, &r1).ok());
  ASSERT_TRUE(core.drain(dreq, &r2).ok());
  EXPECT_EQ(r1.moving_size(), 1);
  EXPECT_EQ(r2.moving_size(), 1);  // still there: nothing has reported yet
  // Its agent restarts: the task is requeued (the old process is gone or
  // fenced), and the node stays draining.
  register_node(core, name);
  EXPECT_EQ(core.state().node(name == "a" ? 0 : 1).state, NodeState::kDraining);
  EXPECT_EQ(core.state().task(id).state, TaskState::kPending);
  core.tick(kT0);
  EXPECT_NE(core.state().task(id).node, std::optional<NodeId>(name == "a" ? 0u : 1u));
  EXPECT_CORE_OK(core);
}

}  // namespace
}  // namespace fleet
