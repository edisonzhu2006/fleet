// Tests for MasterCore with no network and no threads (m2): registration and
// epochs, heartbeat reconciliation (start until reported, stop what
// is not current, apply and acknowledge exits exactly once), admission,
// kill and drain. Each test drives the core call by call with an explicit
// clock and checks MasterCore::check_invariants() along the way.

#include <gtest/gtest.h>

#include <string>

#include "master_core.h"
#include "master_test_helpers.h"

namespace fleet {
namespace {

using namespace fleet::testing;

TEST(MasterCore, RegisterAssignsIdsAndEpochs) {
  MasterCore core({}, 1);
  auto a = register_node(core, "a");
  auto b = register_node(core, "b");
  EXPECT_EQ(a.node_id(), 0u);
  EXPECT_EQ(b.node_id(), 1u);
  EXPECT_NE(a.epoch(), 0u);
  EXPECT_NE(a.epoch(), b.epoch());
  EXPECT_EQ(a.heartbeat_interval_ms(), 500u);

  v1::RegisterNodeRequest bad;
  v1::RegisterNodeResponse resp;
  EXPECT_EQ(core.register_node(bad, &resp, kT0).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);  // no name
  bad.set_name("c");
  EXPECT_EQ(core.register_node(bad, &resp, kT0).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);  // zero capacity
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, StartIsResentUntilReportedThenNotAgain) {
  MasterCore core({}, 1);
  FakeAgent ag = agent_for(register_node(core, "a"));
  auto ids = submit(core, "web", 1, 1000, 512);
  core.schedule(kT0);
  ASSERT_EQ(core.state().task(ids[0]).state, TaskState::kRunning);
  EXPECT_EQ(core.attempt(ids[0]), 1u);

  // A lost reply is repaired: the next heartbeat carries the start again.
  for (int i = 0; i < 2; ++i) {
    auto resp = ag.beat(core);
    ASSERT_EQ(resp.start_size(), 1);
    EXPECT_EQ(resp.start(0).instance().task_id(), ids[0]);
    EXPECT_EQ(resp.start(0).instance().attempt(), 1u);
    EXPECT_EQ(resp.start(0).instance().epoch(), ag.epoch);
    EXPECT_EQ(resp.start(0).job(), "web");
    ASSERT_EQ(resp.start(0).argv_size(), 1);
    EXPECT_EQ(resp.start(0).limits().milli_cpu(), 1000);
  }
  ag.running(ids[0], 1, ag.epoch);
  auto resp = ag.beat(core);
  EXPECT_EQ(resp.start_size(), 0);
  EXPECT_EQ(resp.stop_size(), 0);
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, ExitIsAppliedOnceAndAcknowledged) {
  MasterCore core({}, 1);
  FakeAgent ag = agent_for(register_node(core, "a"));
  auto ids = submit(core, "batch", 0, 1000, 512, 2);
  core.schedule(kT0);
  ag.exited(ids[0], 1, ag.epoch, 0);
  ag.exited(ids[1], 1, ag.epoch, 7);
  auto resp = ag.beat(core);
  EXPECT_EQ(resp.forget_size(), 2);
  EXPECT_EQ(resp.start_size(), 0);  // an exited task is not restarted
  EXPECT_EQ(core.state().task(ids[0]).state, TaskState::kSucceeded);
  EXPECT_EQ(core.state().task(ids[1]).state, TaskState::kFailed);

  // The agent did not see the forget (lost reply) and reports again: still
  // acknowledged, nothing changes.
  ag.exited(ids[1], 1, ag.epoch, 7);
  resp = ag.beat(core);
  EXPECT_EQ(resp.forget_size(), 1);
  EXPECT_EQ(core.state().task(ids[1]).state, TaskState::kFailed);
  EXPECT_EQ(core.state().node(0).allocated, Resources{});
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, ExitFreesRoomAndTheSameReplyStartsTheNextTask) {
  MasterCore core({}, 1);
  FakeAgent ag = agent_for(register_node(core, "a", 1000, 1024));
  auto first = submit(core, "batch", 0, 1000, 512);
  auto second = submit(core, "batch", 0, 1000, 512);
  core.schedule(kT0);
  ASSERT_EQ(core.state().task(second[0]).state, TaskState::kPending);  // no room
  ag.running(first[0], 1, ag.epoch);
  ag.beat(core);

  // The heartbeat that reports the exit also carries the start for the
  // task that now fits, without waiting for the scheduler thread.
  ag.exited(first[0], 1, ag.epoch, 0);
  auto resp = ag.beat(core);
  ASSERT_EQ(resp.start_size(), 1);
  EXPECT_EQ(resp.start(0).instance().task_id(), second[0]);
  EXPECT_EQ(resp.start(0).instance().attempt(), 1u);
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, StopsKilledStaleAttemptAndUnknownInstances) {
  MasterCore core({}, 1);
  FakeAgent ag = agent_for(register_node(core, "a"));
  auto ids = submit(core, "web", 1, 1000, 512, 2);
  core.schedule(kT0);

  v1::KillRequest kreq;
  kreq.set_task_id(ids[0]);
  v1::KillResponse kresp;
  ASSERT_TRUE(core.kill(kreq, &kresp).ok());
  ASSERT_EQ(kresp.killed_size(), 1);

  ag.running(ids[0], 1, ag.epoch);       // killed: stop
  ag.running(ids[1], 99, ag.epoch);      // wrong attempt: stop
  ag.running(12345, 1, ag.epoch);        // unknown task: stop
  ag.running(ids[1], 1, ag.epoch + 1);   // wrong epoch: stop
  auto resp = ag.beat(core);
  EXPECT_EQ(resp.stop_size(), 4);
  // The current instance of ids[1] was never reported, so it is started.
  ASSERT_EQ(resp.start_size(), 1);
  EXPECT_EQ(resp.start(0).instance().task_id(), ids[1]);

  // An exit from the stale attempt must not finish the current one.
  ag.exited(ids[1], 99, ag.epoch, 0);
  resp = ag.beat(core);
  EXPECT_EQ(resp.forget_size(), 1);
  EXPECT_EQ(core.state().task(ids[1]).state, TaskState::kRunning);
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, HeartbeatRejectsUnknownNodeAndStaleEpoch) {
  MasterCore core({}, 1);
  auto first = register_node(core, "a");
  v1::HeartbeatRequest req;
  v1::HeartbeatResponse resp;
  req.set_node_id(7);
  EXPECT_EQ(core.heartbeat(req, &resp, kT0).error_code(), grpc::StatusCode::NOT_FOUND);

  auto second = register_node(core, "a");  // the agent restarted
  EXPECT_EQ(second.node_id(), first.node_id());
  EXPECT_NE(second.epoch(), first.epoch());
  req.set_node_id(first.node_id());
  req.set_epoch(first.epoch());
  EXPECT_EQ(core.heartbeat(req, &resp, kT0).error_code(),
            grpc::StatusCode::FAILED_PRECONDITION);
  req.set_epoch(second.epoch());
  EXPECT_TRUE(core.heartbeat(req, &resp, kT0).ok());
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, ReRegisterRequeuesTasksAtANewAttempt) {
  MasterCore core({}, 1);
  auto first = register_node(core, "a");
  auto ids = submit(core, "web", 1, 1000, 512);
  core.schedule(kT0);
  EXPECT_EQ(core.attempt(ids[0]), 1u);

  auto second = register_node(core, "a");
  EXPECT_EQ(core.state().task(ids[0]).state, TaskState::kPending);
  core.schedule(kT0);
  EXPECT_EQ(core.attempt(ids[0]), 2u);

  // A survivor of the old epoch is stopped; the new attempt starts.
  FakeAgent ag = agent_for(second);
  ag.running(ids[0], 1, first.epoch());
  auto resp = ag.beat(core);
  ASSERT_EQ(resp.stop_size(), 1);
  EXPECT_EQ(resp.stop(0).epoch(), first.epoch());
  ASSERT_EQ(resp.start_size(), 1);
  EXPECT_EQ(resp.start(0).instance().attempt(), 2u);

  // Changing shape on re-registration is refused.
  v1::RegisterNodeRequest req;
  req.set_name("a");
  req.mutable_capacity()->set_milli_cpu(8000);
  req.mutable_capacity()->set_memory_mb(4096);
  v1::RegisterNodeResponse r;
  EXPECT_EQ(core.register_node(req, &r, kT0).error_code(),
            grpc::StatusCode::FAILED_PRECONDITION);
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, SubmitValidation) {
  MasterCore core({}, 1);
  v1::SubmitRequest req;
  v1::SubmitResponse resp;
  req.mutable_request()->set_milli_cpu(100);
  req.mutable_request()->set_memory_mb(100);
  req.add_argv("true");
  for (const char* bad : {"", "../etc", "a/b", ".", "..", "has space"}) {
    req.set_job(bad);
    EXPECT_EQ(core.submit(req, &resp).error_code(), grpc::StatusCode::INVALID_ARGUMENT)
        << bad;
  }
  req.set_job("ok.job-1_x");
  req.set_priority(9);
  EXPECT_EQ(core.submit(req, &resp).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  req.set_priority(1);
  req.clear_argv();
  EXPECT_EQ(core.submit(req, &resp).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  req.add_argv("true");
  req.set_count(100000);
  EXPECT_EQ(core.submit(req, &resp).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  EXPECT_EQ(core.state().tasks().size(), 0u);
  req.set_count(3);
  ASSERT_TRUE(core.submit(req, &resp).ok());
  EXPECT_EQ(resp.task_ids_size(), 3);
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, DrainStopsTheTaskFirstAndMovesItAfterItsExit) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  FakeAgent b = agent_for(register_node(core, "b"));
  auto ids = submit(core, "web", 1, 1000, 512);
  core.schedule(kT0);
  const NodeId first = *core.state().task(ids[0]).node;
  FakeAgent& on = first == a.node_id ? a : b;
  FakeAgent& other = first == a.node_id ? b : a;
  on.running(ids[0], 1, on.epoch);
  on.beat(core);

  v1::DrainRequest dreq;
  dreq.set_node(first == a.node_id ? "a" : "b");
  v1::DrainResponse dresp;
  ASSERT_TRUE(core.drain(dreq, &dresp).ok());
  ASSERT_EQ(dresp.moving_size(), 1);
  // Graceful: the task is still placed where it runs, and a scheduling
  // pass does not start a second copy elsewhere.
  core.schedule(kT0);
  EXPECT_EQ(core.state().task(ids[0]).node, first);
  EXPECT_EQ(core.attempt(ids[0]), 1u);
  EXPECT_EQ(other.beat(core).start_size(), 0);

  // Every heartbeat that still reports it running is told to stop it.
  for (int i = 0; i < 2; ++i) {
    on.running(ids[0], 1, on.epoch);
    auto resp = on.beat(core);
    ASSERT_EQ(resp.stop_size(), 1);
    EXPECT_EQ(resp.stop(0).attempt(), 1u);
    EXPECT_EQ(resp.start_size(), 0);
  }
  EXPECT_CORE_OK(core);

  // Its exit (SIGTERM from the stop) is the move completing: not a
  // failure, whatever the policy, and the task is placed on the other node
  // at a new attempt.
  on.exited(ids[0], 1, on.epoch, 143);
  on.beat(core);
  EXPECT_EQ(core.restarts(ids[0]), 0u);
  core.schedule(kT0);
  EXPECT_EQ(core.state().task(ids[0]).state, TaskState::kRunning);
  EXPECT_EQ(core.state().task(ids[0]).node, other.node_id);
  EXPECT_EQ(core.attempt(ids[0]), 2u);
  auto resp = other.beat(core);
  ASSERT_EQ(resp.start_size(), 1);
  EXPECT_EQ(resp.start(0).instance().attempt(), 2u);

  // The drained node takes nothing new.
  auto more = submit(core, "web", 1, 3500, 512);
  core.schedule(kT0);
  EXPECT_EQ(core.state().task(more[0]).state, TaskState::kPending);

  dreq.set_node("nope");
  EXPECT_EQ(core.drain(dreq, &dresp).error_code(), grpc::StatusCode::NOT_FOUND);
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, PreemptionStopsVictimAndStartsPreemptor) {
  MasterCore core({}, 1);
  FakeAgent ag = agent_for(register_node(core, "a", 2000, 2048));
  auto batch = submit(core, "batch", 0, 1000, 512, 2);
  core.schedule(kT0);
  ag.running(batch[0], 1, ag.epoch);
  ag.running(batch[1], 1, ag.epoch);
  ag.beat(core);

  auto prod = submit(core, "prod", 2, 1000, 512);
  core.schedule(kT0);
  ASSERT_EQ(core.state().task(prod[0]).state, TaskState::kRunning);
  ag.running(batch[0], 1, ag.epoch);
  ag.running(batch[1], 1, ag.epoch);
  auto resp = ag.beat(core);
  EXPECT_EQ(resp.stop_size(), 1);  // exactly one victim
  ASSERT_EQ(resp.start_size(), 1);
  EXPECT_EQ(resp.start(0).instance().task_id(), prod[0]);
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, KillByJobAndErrors) {
  MasterCore core({}, 1);
  register_node(core, "a");
  submit(core, "j", 1, 1000, 512, 3);
  v1::KillRequest req;
  v1::KillResponse resp;
  EXPECT_EQ(core.kill(req, &resp).error_code(), grpc::StatusCode::INVALID_ARGUMENT);
  req.set_job("j");
  ASSERT_TRUE(core.kill(req, &resp).ok());
  EXPECT_EQ(resp.killed_size(), 3);
  req.set_task_id(0);
  EXPECT_EQ(core.kill(req, &resp).error_code(), grpc::StatusCode::FAILED_PRECONDITION);
  req.set_task_id(99);
  EXPECT_EQ(core.kill(req, &resp).error_code(), grpc::StatusCode::NOT_FOUND);
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, ImageIsValidatedStoredAndSentWithTheStart) {
  MasterCore core({}, 1);
  FakeAgent ag = agent_for(register_node(core, "a"));
  v1::SubmitRequest req;
  req.set_job("box");
  req.mutable_request()->set_milli_cpu(500);
  req.mutable_request()->set_memory_mb(64);
  req.add_argv("true");
  v1::SubmitResponse resp;
  // An image that docker would parse as a flag never gets admitted.
  for (const char* bad : {"--privileged", "-v", "a b"}) {
    req.set_image(bad);
    EXPECT_EQ(core.submit(req, &resp).error_code(), grpc::StatusCode::INVALID_ARGUMENT) << bad;
  }
  EXPECT_EQ(resp.task_ids_size(), 0);
  req.set_image("alpine:3.20");
  ASSERT_TRUE(core.submit(req, &resp).ok());
  req.clear_image();  // empty: the node's default
  ASSERT_TRUE(core.submit(req, &resp).ok());
  core.schedule(kT0);
  auto hb = ag.beat(core);
  ASSERT_EQ(hb.start_size(), 2);
  EXPECT_EQ(hb.start(0).image(), "alpine:3.20");
  EXPECT_EQ(hb.start(1).image(), "");
  v1::ListResponse list;
  ASSERT_TRUE(core.list({}, &list, kT0).ok());
  EXPECT_EQ(list.tasks(0).image(), "alpine:3.20");
  EXPECT_CORE_OK(core);
}

TEST(MasterCore, OomKillIsRecordedAndListed) {
  MasterCore core({}, 1);
  FakeAgent ag = agent_for(register_node(core, "a"));
  auto ids = submit(core, "hog", 1, 500, 32, 2);
  core.schedule(kT0);
  // Task 0 is OOM-killed (137 + flag); task 1 exits 137 from a plain
  // SIGKILL. Both fail; only the first is an OOM.
  ag.exited(ids[0], 1, ag.epoch, 137);
  ag.req.mutable_tasks(0)->set_oom_killed(true);
  ag.req.mutable_tasks(0)->set_message("OOM-killed: exceeded its 32 MB memory limit");
  ag.exited(ids[1], 1, ag.epoch, 137);
  ag.beat(core);
  EXPECT_EQ(core.state().task(ids[0]).state, TaskState::kFailed);
  EXPECT_EQ(core.state().task(ids[1]).state, TaskState::kFailed);
  v1::ListResponse list;
  ASSERT_TRUE(core.list({}, &list, kT0).ok());
  EXPECT_TRUE(list.tasks(0).oom_killed());
  EXPECT_EQ(list.tasks(0).exit_code(), 137);
  EXPECT_NE(list.tasks(0).message().find("32 MB"), std::string::npos);
  EXPECT_FALSE(list.tasks(1).oom_killed());
  EXPECT_CORE_OK(core);
}

}  // namespace
}  // namespace fleet
