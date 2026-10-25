// Tests for the master side of stateful tasks (m5), in MasterCore with a
// fake clock and no network: admission rules for a checkpoint directory,
// the launch carrying the directory and resume command, durability of both
// through the log and snapshots, and the attempt rules that keep two
// attempts from running at once (never preempted; placed again only after
// the old node is dead, at a higher attempt; the old node's epoch refused).

#include <gtest/gtest.h>

#include <chrono>

#include "master_core.h"
#include "master_test_helpers.h"

namespace fleet {
namespace {

using namespace fleet::testing;
using namespace std::chrono_literals;

v1::SubmitRequest stateful_req(const std::string& job, const std::string& dir,
                               int prio = 1, int64_t mcpu = 1000) {
  v1::SubmitRequest req;
  req.set_job(job);
  req.set_priority(prio);
  req.mutable_request()->set_milli_cpu(mcpu);
  req.mutable_request()->set_memory_mb(256);
  req.add_argv("streamkit");
  req.add_argv("--ckpt-dir");
  req.add_argv(dir);
  req.set_checkpoint_dir(dir);
  *req.mutable_resume_argv() = req.argv();
  req.add_resume_argv("--resume");
  return req;
}

grpc::Status try_submit(MasterCore& core, const v1::SubmitRequest& req,
                        v1::SubmitResponse* resp) {
  return core.submit(req, resp);
}

TaskId submit_stateful(MasterCore& core, const std::string& job, const std::string& dir,
                       int prio = 1, int64_t mcpu = 1000) {
  v1::SubmitResponse resp;
  const grpc::Status s = try_submit(core, stateful_req(job, dir, prio, mcpu), &resp);
  EXPECT_TRUE(s.ok()) << s.error_message();
  return resp.task_ids_size() ? resp.task_ids(0) : 0;
}

// ---------------------------------------------------------------- admission

TEST(StatefulAdmission, RefusesBadCheckpointSpecs) {
  MasterCore core({}, 1);
  struct Case {
    const char* why;
    std::function<void(v1::SubmitRequest*)> mutate;
  };
  const std::vector<Case> cases = {
      {"relative path", [](v1::SubmitRequest* r) { r->set_checkpoint_dir("ckpt/a"); }},
      {"root", [](v1::SubmitRequest* r) { r->set_checkpoint_dir("/"); }},
      {"dot-dot", [](v1::SubmitRequest* r) { r->set_checkpoint_dir("/a/../b"); }},
      {"dot", [](v1::SubmitRequest* r) { r->set_checkpoint_dir("/a/./b"); }},
      {"trailing slash", [](v1::SubmitRequest* r) { r->set_checkpoint_dir("/a/b/"); }},
      {"no resume command", [](v1::SubmitRequest* r) { r->clear_resume_argv(); }},
      {"count 2", [](v1::SubmitRequest* r) { r->set_count(2); }},
      {"resume without dir", [](v1::SubmitRequest* r) { r->clear_checkpoint_dir(); }},
  };
  for (const Case& c : cases) {
    v1::SubmitRequest req = stateful_req("sk", "/shared/ckpt/sk");
    c.mutate(&req);
    v1::SubmitResponse resp;
    const grpc::Status s = try_submit(core, req, &resp);
    EXPECT_EQ(s.error_code(), grpc::StatusCode::INVALID_ARGUMENT) << c.why;
  }
  EXPECT_TRUE(core.take_records().empty());  // nothing refused was logged
  EXPECT_TRUE(core.state().tasks().empty());
}

TEST(StatefulAdmission, ACheckpointDirBelongsToOneLiveTask) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  const TaskId first = submit_stateful(core, "sk1", "/shared/ckpt/x");
  v1::SubmitResponse resp;
  // A second task on the same directory while the first is live (pending
  // here) would write checkpoints into the first one's: refused.
  grpc::Status s = try_submit(core, stateful_req("sk2", "/shared/ckpt/x"), &resp);
  EXPECT_EQ(s.error_code(), grpc::StatusCode::ALREADY_EXISTS) << s.error_message();
  // Another directory is fine.
  submit_stateful(core, "sk3", "/shared/ckpt/y");
  // Once the first task has finished, its directory is free again.
  core.tick(kT0);
  ASSERT_EQ(core.state().task(first).state, TaskState::kRunning);
  a.running(first + 1, core.attempt(first + 1), a.epoch);
  a.exited(first, core.attempt(first), a.epoch, 0);
  a.beat(core, kT0 + 500ms);
  ASSERT_EQ(core.state().task(first).state, TaskState::kSucceeded);
  s = try_submit(core, stateful_req("sk2", "/shared/ckpt/x"), &resp);
  EXPECT_TRUE(s.ok()) << s.error_message();
  EXPECT_CORE_OK(core);
}

TEST(StatefulAdmission, ARetriedSubmissionIsAnsweredNotRefusedAsADuplicateDir) {
  MasterCore core({}, 1);
  v1::SubmitRequest req = stateful_req("sk", "/shared/ckpt/x");
  req.set_request_id("r-1");
  v1::SubmitResponse a, b;
  ASSERT_TRUE(try_submit(core, req, &a).ok());
  // The same request again (the client lost the reply): the first answer,
  // not "directory in use by task <its own id>".
  const grpc::Status s = try_submit(core, req, &b);
  EXPECT_TRUE(s.ok()) << s.error_message();
  EXPECT_EQ(a.task_ids(0), b.task_ids(0));
  EXPECT_EQ(core.state().tasks().size(), 1u);
}

// ---------------------------------------------------------------- the launch

TEST(StatefulLaunch, StartCarriesTheCheckpointDirAndResumeCommand) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  const TaskId id = submit_stateful(core, "sk", "/shared/ckpt/x");
  core.tick(kT0);
  const v1::HeartbeatResponse resp = a.beat(core, kT0 + 500ms);
  ASSERT_EQ(resp.start_size(), 1);
  const v1::TaskLaunch& l = resp.start(0);
  EXPECT_EQ(l.instance().task_id(), id);
  EXPECT_EQ(l.instance().attempt(), 1u);
  EXPECT_EQ(l.checkpoint_dir(), "/shared/ckpt/x");
  ASSERT_EQ(l.resume_argv_size(), l.argv_size() + 1);
  EXPECT_EQ(l.resume_argv(l.resume_argv_size() - 1), "--resume");
  // A stateless task's launch has neither.
  submit(core, "plain", 1, 500, 64);
  core.tick(kT0 + 600ms);
  a.running(id, 1, a.epoch);
  const v1::HeartbeatResponse r2 = a.beat(core, kT0 + 1000ms);
  ASSERT_EQ(r2.start_size(), 1);
  EXPECT_EQ(r2.start(0).checkpoint_dir(), "");
  EXPECT_EQ(r2.start(0).resume_argv_size(), 0);
}

TEST(StatefulLaunch, CheckpointDirAndResumeCommandSurviveReplayAndSnapshot) {
  MasterCore core({}, 1);
  register_node(core, "a");
  const TaskId id = submit_stateful(core, "sk", "/shared/ckpt/x");
  core.tick(kT0);
  MasterState replayed;
  for (const v1::WalRecord& r : core.take_records()) ASSERT_EQ(replayed.apply(r), "");
  EXPECT_EQ(replayed.task(id).checkpoint_dir, "/shared/ckpt/x");
  EXPECT_EQ(replayed.task(id).resume_argv, core.durable_state().task(id).resume_argv);
  EXPECT_FALSE(replayed.cluster().task(id).preemptible);

  v1::Snapshot snap;
  replayed.to_snapshot(&snap);
  std::string err;
  std::optional<MasterState> back = MasterState::from_snapshot(snap, &err);
  ASSERT_TRUE(back) << err;
  EXPECT_EQ(back->task(id).checkpoint_dir, "/shared/ckpt/x");
  EXPECT_EQ(back->task(id).resume_argv, replayed.task(id).resume_argv);
  EXPECT_FALSE(back->cluster().task(id).preemptible);
  EXPECT_EQ(back->task(id).attempt, 1u);
}

// ---------------------------------------------------------------- one attempt at a time

TEST(StatefulAttempts, AStatefulTaskIsNeverAPreemptionVictim) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a", 2000, 2048));
  // Fill the node with a batch-priority stateful task and a batch-priority
  // stateless one, then ask for room that only evicting both would make.
  const TaskId sk = submit_stateful(core, "sk", "/shared/ckpt/x", 0, 1000);
  const auto plain = submit(core, "plain", 0, 1000, 256);
  core.tick(kT0);
  ASSERT_EQ(core.state().task(sk).state, TaskState::kRunning);
  ASSERT_EQ(core.state().task(plain[0]).state, TaskState::kRunning);
  const auto big = submit(core, "prod", 2, 2000, 256);
  core.tick(kT0 + 100ms);
  // The stateless victim alone does not make room, and the stateful one
  // may not be evicted: the high-priority task waits, nothing is evicted.
  EXPECT_EQ(core.state().task(big[0]).state, TaskState::kPending);
  EXPECT_EQ(core.state().task(sk).state, TaskState::kRunning);
  EXPECT_EQ(core.attempt(sk), 1u);
  EXPECT_EQ(core.state().task(sk).preempted_count, 0u);
  // A task that fits by evicting only the stateless one does so.
  const auto mid = submit(core, "prod2", 2, 1000, 256);
  core.tick(kT0 + 200ms);
  EXPECT_EQ(core.state().task(mid[0]).state, TaskState::kRunning);
  EXPECT_EQ(core.state().task(plain[0]).preempted_count, 1u);
  EXPECT_EQ(core.state().task(sk).state, TaskState::kRunning);
  EXPECT_CORE_OK(core);
  (void)a;
}

TEST(StatefulAttempts, PlacedAgainOnlyAfterTheOldNodeIsDeadAndAtAHigherAttempt) {
  MasterCore core({}, 1);  // 500 ms beats, suspect after 1.5 s, dead after 3 s
  FakeAgent a = agent_for(register_node(core, "a"));
  FakeAgent b = agent_for(register_node(core, "b"));
  const TaskId id = submit_stateful(core, "sk", "/shared/ckpt/x", 1, 3000);
  core.tick(kT0);
  const NodeId first = *core.state().task(id).node;
  FakeAgent& old_node = first == a.node_id ? a : b;
  FakeAgent& other = first == a.node_id ? b : a;
  old_node.running(id, 1, old_node.epoch);
  old_node.beat(core, kT0 + 100ms);

  // The old node falls silent at 100 ms. Through suspect and until it is
  // declared dead, the task stays assigned to it: being slow or cut off is
  // not proof its copy has stopped.
  for (auto t = kT0 + 200ms; t < kT0 + 3100ms; t += 100ms) {
    if ((t - kT0) % 500ms == 0ms) other.beat(core, t);
    const TickResult r = core.tick(t);
    EXPECT_TRUE(r.died.empty()) << "at " << (t - kT0).count();
    ASSERT_EQ(core.state().task(id).node, first) << "moved before the node was dead";
    EXPECT_EQ(core.attempt(id), 1u);
  }
  EXPECT_TRUE(core.state().node(first).suspect);

  // Dead at 3.1 s: the epoch is revoked and the task is placed on the
  // other node at attempt 2, which that node's next heartbeat starts.
  const TickResult r = core.tick(kT0 + 3100ms);
  ASSERT_EQ(r.died, std::vector<NodeId>{first});
  EXPECT_EQ(core.state().task(id).node, other.node_id);
  EXPECT_EQ(core.attempt(id), 2u);
  const v1::HeartbeatResponse start = other.beat(core, kT0 + 3500ms);
  ASSERT_EQ(start.start_size(), 1);
  EXPECT_EQ(start.start(0).instance().attempt(), 2u);
  EXPECT_EQ(start.start(0).checkpoint_dir(), "/shared/ckpt/x");

  // The old node comes back still running attempt 1: its heartbeat is
  // refused (the agent then kills everything), and nothing it says about
  // attempt 1 changes the task.
  old_node.running(id, 1, old_node.epoch);
  v1::HeartbeatResponse refused;
  const grpc::Status s = old_node.try_beat(core, &refused, kT0 + 3600ms);
  EXPECT_EQ(s.error_code(), grpc::StatusCode::FAILED_PRECONDITION);
  EXPECT_EQ(core.state().task(id).node, other.node_id);
  EXPECT_EQ(core.attempt(id), 2u);
  EXPECT_CORE_OK(core);
}

TEST(StatefulAttempts, AReportOfAnOlderAttemptIsStoppedNotAdopted) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  const TaskId id = submit_stateful(core, "sk", "/shared/ckpt/x");
  core.tick(kT0);
  a.running(id, 1, a.epoch);
  a.beat(core, kT0 + 100ms);
  // The agent restarts (new epoch): the old epoch's task is requeued and
  // placed again at attempt 2, here.
  a = agent_for(register_node(core, "a", 4000, 4096, kT0 + 200ms));
  core.tick(kT0 + 300ms);
  ASSERT_EQ(core.attempt(id), 2u);
  // A report naming attempt 1 under the new epoch (it cannot be current)
  // is told to stop; attempt 2 is still started.
  a.running(id, 1, a.epoch);
  const v1::HeartbeatResponse resp = a.beat(core, kT0 + 400ms);
  ASSERT_EQ(resp.stop_size(), 1);
  EXPECT_EQ(resp.stop(0).attempt(), 1u);
  ASSERT_EQ(resp.start_size(), 1);
  EXPECT_EQ(resp.start(0).instance().attempt(), 2u);
  EXPECT_CORE_OK(core);
}

TEST(StatefulAttempts, DrainMovesAStatefulTaskOnlyAfterItsExit) {
  MasterCore core({}, 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  FakeAgent b = agent_for(register_node(core, "b"));
  const TaskId id = submit_stateful(core, "sk", "/shared/ckpt/x", 1, 3000);
  core.tick(kT0);
  FakeAgent& on = *core.state().task(id).node == a.node_id ? a : b;
  FakeAgent& off = &on == &a ? b : a;
  on.running(id, 1, on.epoch);
  on.beat(core, kT0 + 100ms);
  v1::DrainRequest dreq;
  dreq.set_node(core.durable_state().node(on.node_id).name);
  v1::DrainResponse dresp;
  ASSERT_TRUE(core.drain(dreq, &dresp).ok());
  // Still running there: told to stop, not placed elsewhere yet.
  on.running(id, 1, on.epoch);
  const v1::HeartbeatResponse r1 = on.beat(core, kT0 + 200ms);
  ASSERT_EQ(r1.stop_size(), 1);
  core.tick(kT0 + 300ms);
  EXPECT_EQ(core.state().task(id).node, on.node_id);
  EXPECT_EQ(core.attempt(id), 1u);
  // Its exit arrives: now it moves, at attempt 2.
  on.exited(id, 1, on.epoch, 143);
  on.beat(core, kT0 + 400ms);
  core.tick(kT0 + 500ms);
  EXPECT_EQ(core.state().task(id).node, off.node_id);
  EXPECT_EQ(core.attempt(id), 2u);
  EXPECT_CORE_OK(core);
}

}  // namespace
}  // namespace fleet
