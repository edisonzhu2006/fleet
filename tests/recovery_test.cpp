// Tests for deterministic replay and master crash recovery (m4).
//
// Replay: a randomized driver runs a master through thousands of
// operations (submissions, heartbeats with exits, kills, drains, silent
// and restarting agents, time passing, and master restarts from the log)
// while a "follower" MasterState applies every record the master makes.
// After every single operation the follower's state must equal the live
// one, field for field. That is the property recovery relies on, and the
// one m4b's Raft followers will rely on.
//
// Recovery: scenario tests of the report-before-schedule window and the
// reconciliation after a restart, all with a fake clock (no sleeps).

#include <gtest/gtest.h>

#include <map>
#include <tuple>

#include "master_core.h"
#include "wal_test_helpers.h"

namespace fleet {
namespace {

using namespace std::chrono_literals;
using namespace fleet::testing;

class Rand {
 public:
  explicit Rand(uint64_t seed) : x_(seed * 0x9E3779B97F4A7C15ULL + 1) {}
  uint64_t operator()(uint64_t mod) {
    x_ = x_ * 6364136223846793005ULL + 1442695040888963407ULL;
    return (x_ >> 33) % mod;
  }

 private:
  uint64_t x_;
};

// An agent as the master sees it over the wire: what it runs, what has
// exited and not been forgotten, and how it reacts to each reply. It
// behaves like agent.cpp (start what it is told unless it already has the
// instance, stop by marking exited with SIGTERM, forget acknowledged
// exits, kill everything and re-register when fenced).
struct SimAgent {
  using Key = std::tuple<uint32_t, uint32_t, uint64_t>;  // task, attempt, epoch
  struct Inst {
    bool exited = false;
    int code = 0;
    int signal = 0;
  };
  std::string name;
  int64_t cpu = 0, mem = 0;
  bool registered = false;
  bool silent = false;  // stops heartbeating (a crashed machine or a partition)
  uint32_t node_id = 0;
  uint64_t epoch = 0;
  std::map<Key, Inst> insts;

  void do_register(MasterCore& core, Clock::time_point now) {
    insts.clear();  // a fresh agent process: leftovers were killed (m3)
    v1::RegisterNodeRequest req;
    req.set_name(name);
    req.mutable_capacity()->set_milli_cpu(cpu);
    req.mutable_capacity()->set_memory_mb(mem);
    v1::RegisterNodeResponse resp;
    ASSERT_TRUE(core.register_node(req, &resp, now).ok());
    registered = true;
    node_id = resp.node_id();
    epoch = resp.epoch();
  }

  void beat(MasterCore& core, Clock::time_point now, Rand& rnd) {
    if (silent) return;
    if (!registered) {
      do_register(core, now);
      return;
    }
    // Some running tasks exit on their own since the last beat.
    for (auto& [key, inst] : insts) {
      if (!inst.exited && rnd(6) == 0) {
        inst.exited = true;
        inst.code = static_cast<int>(rnd(3));  // 0, 1 or 2
      }
    }
    v1::HeartbeatRequest req;
    req.set_node_id(node_id);
    req.set_epoch(epoch);
    for (const auto& [key, inst] : insts) {
      v1::TaskStatus* st = req.add_tasks();
      st->mutable_instance()->set_task_id(std::get<0>(key));
      st->mutable_instance()->set_attempt(std::get<1>(key));
      st->mutable_instance()->set_epoch(std::get<2>(key));
      st->set_phase(inst.exited ? v1::TaskStatus::PHASE_EXITED : v1::TaskStatus::PHASE_RUNNING);
      st->set_exit_code(inst.code);
      st->set_signal(inst.signal);
    }
    v1::HeartbeatResponse resp;
    const grpc::Status s = core.heartbeat(req, &resp, now);
    if (s.error_code() == grpc::StatusCode::FAILED_PRECONDITION) {
      insts.clear();  // fenced: kill everything, register again next beat
      registered = false;
      return;
    }
    ASSERT_TRUE(s.ok()) << s.error_message();
    for (const auto& f : resp.forget()) {
      auto it = insts.find({f.task_id(), f.attempt(), f.epoch()});
      if (it != insts.end() && it->second.exited) insts.erase(it);
    }
    for (const auto& st : resp.stop()) {
      auto it = insts.find({st.task_id(), st.attempt(), st.epoch()});
      if (it != insts.end() && !it->second.exited) {
        it->second.exited = true;
        it->second.signal = 15;
      }
    }
    for (const auto& l : resp.start()) {
      insts.try_emplace({l.instance().task_id(), l.instance().attempt(), l.instance().epoch()});
    }
  }
};

struct ReplayCounts {
  int ops = 0, restarts = 0, records = 0;
  std::map<int, int> by_op;  // WalRecord::OpCase -> count
};

// One randomized run; returns what it did so the test can check coverage.
ReplayCounts run_replay(uint64_t seed, int ops) {
  Rand rnd(seed);
  const MasterOptions opts = durable_test_options();
  auto core = std::make_unique<MasterCore>(opts, 1);
  MasterState follower;  // applies every record the live master makes
  std::vector<SimAgent> agents;
  Clock::time_point now = kT0;
  ReplayCounts c;
  const char* jobs[] = {"web", "batch", "etl", "cron"};
  const v1::RestartPolicy policies[] = {v1::RESTART_POLICY_NEVER, v1::RESTART_POLICY_ON_FAILURE,
                                        v1::RESTART_POLICY_ALWAYS};

  auto add_agent = [&] {
    SimAgent a;
    a.name = "n" + std::to_string(agents.size());
    a.cpu = static_cast<int64_t>(2000 + 2000 * rnd(3));
    a.mem = static_cast<int64_t>(2048 * (1 + rnd(3)));
    a.do_register(*core, now);
    agents.push_back(std::move(a));
  };
  for (int i = 0; i < 4; ++i) add_agent();

  for (int op = 0; op < ops; ++op) {
    const uint64_t pick = rnd(100);
    if (pick < 15) {  // submit
      v1::SubmitRequest req;
      req.set_job(jobs[rnd(4)]);
      req.set_priority(static_cast<int32_t>(rnd(4)));
      req.mutable_request()->set_milli_cpu(static_cast<int64_t>(250 * (1 + rnd(8))));
      req.mutable_request()->set_memory_mb(static_cast<int64_t>(128 * (1 + rnd(8))));
      req.add_argv("true");
      req.set_count(static_cast<uint32_t>(1 + rnd(3)));
      req.set_restart_policy(policies[rnd(3)]);
      v1::SubmitResponse resp;
      EXPECT_TRUE(core->submit(req, &resp).ok());
    } else if (pick < 52) {  // one agent heartbeats
      agents[rnd(agents.size())].beat(*core, now, rnd);
    } else if (pick < 75) {  // time passes: every agent beats, then a tick
      now += std::chrono::milliseconds(100 + 100 * rnd(4));
      for (SimAgent& a : agents) a.beat(*core, now, rnd);
      core->tick(now);
    } else if (pick < 79) {  // kill a task, or now and then a whole job
      v1::KillRequest req;
      if (rnd(8) == 0) {
        req.set_job(jobs[rnd(4)]);
      } else if (!core->state().tasks().empty()) {
        req.set_task_id(static_cast<uint32_t>(rnd(core->state().tasks().size())));
      } else {
        req.set_job("web");
      }
      v1::KillResponse resp;
      core->kill(req, &resp);  // may be refused (already terminal): fine
    } else if (pick < 83 && rnd(5) == 0) {  // now and then, drain a node
      // Rare: a drained node never takes work again (there is no undrain),
      // so frequent drains would leave the cluster with nowhere to place.
      v1::DrainRequest req;
      req.set_node(agents[rnd(agents.size())].name);
      v1::DrainResponse resp;
      core->drain(req, &resp);  // refused for a dead node: fine
    } else if (pick < 83) {
      // (the drain's dice said no)
    } else if (pick < 86) {  // an agent goes silent, or comes back
      // Biased toward coming back, so most of the cluster is usually up.
      SimAgent& a = agents[rnd(agents.size())];
      a.silent = a.silent ? false : rnd(2) == 0;
    } else if (pick < 92) {  // an agent process restarts
      SimAgent& a = agents[rnd(agents.size())];
      if (!a.silent) a.do_register(*core, now);
    } else if (pick < 95) {
      if (agents.size() < 6) add_agent();
    } else if (pick < 96) {  // the master crashes and restarts from its log
      // Every record so far was committed (MasterServer commits at the end
      // of each call), so the follower holds exactly the durable state.
      core = std::make_unique<MasterCore>(opts, follower, now, 1);
      ++c.restarts;
    } else {  // a long pause: the master's tick stalls
      now += std::chrono::milliseconds(500 * (1 + rnd(4)));
      core->tick(now);
    }
    if (::testing::Test::HasFatalFailure()) return c;

    for (const v1::WalRecord& r : core->take_records()) {
      const std::string err = follower.apply(r);
      EXPECT_EQ(err, "") << "seed " << seed << " op " << op;
      if (!err.empty()) return c;
      ++c.records;
      ++c.by_op[r.op_case()];
    }
    const std::string diff = state_diff(follower, core->durable_state());
    EXPECT_EQ(diff, "") << "seed " << seed << " op " << op;
    const std::string inv = core->check_invariants();
    EXPECT_EQ(inv, "") << "seed " << seed << " op " << op;
    if (!diff.empty() || !inv.empty()) return c;
    if (op % 97 == 0) {
      // A snapshot of the follower rebuilds the same state too.
      v1::Snapshot snap;
      follower.to_snapshot(&snap);
      std::string err;
      auto back = MasterState::from_snapshot(snap, &err);
      EXPECT_TRUE(back) << err;
      if (back) EXPECT_EQ(state_diff(*back, follower), "");
    }
    ++c.ops;
  }
  return c;
}

TEST(Replay, EqualsLiveStateForRandomizedOperationSequences) {
  std::map<int, int> total;
  for (uint64_t seed : {1, 2, 3, 4, 5}) {
    const ReplayCounts c = run_replay(seed, 3000);
    ASSERT_EQ(c.ops, 3000) << "seed " << seed << " stopped early";
    EXPECT_GT(c.restarts, 0) << "seed " << seed;
    std::string summary;
    for (const auto& [op, n] : c.by_op) {
      summary += " " + std::to_string(op) + ":" + std::to_string(n);
      total[op] += n;
    }
    std::printf("seed %llu: %d ops, %d master restarts, %d records (by op case:%s)\n",
                static_cast<unsigned long long>(seed), c.ops, c.restarts, c.records,
                summary.c_str());
  }
  // The driver must actually exercise every kind of record, or the
  // equality above proves little.
  for (int op = v1::WalRecord::kNodeRegistered; op <= v1::WalRecord::kTaskKilled; ++op) {
    EXPECT_GT(total[op], 0) << "no record of op case " << op << " in any seed";
  }
}

// ------------------------------------------------------------ recovery

// A master with two nodes, each running one task, plus one pending task
// that does not fit; then a "crash": the state is whatever the log holds.
struct Crashed {
  MasterState state;
  uint64_t epoch_a = 0, epoch_b = 0;
  uint32_t node_a = 0, node_b = 0;
};

Crashed crash_with_running_tasks() {
  MasterCore core(durable_test_options(), 1);
  FakeAgent a = agent_for(register_node(core, "a", 2000, 2048));
  FakeAgent b = agent_for(register_node(core, "b", 2000, 2048));
  submit(core, "svc", 1, 2000, 256, 2);  // 0 -> a, 1 -> b
  core.schedule(kT0);
  a.running(0, 1, a.epoch);
  a.beat(core);
  b.running(1, 1, b.epoch);
  b.beat(core);
  submit(core, "later", 1, 1000, 128);  // 2: pending, no room
  core.schedule(kT0);
  Crashed c;
  EXPECT_EQ(apply_all(c.state, core.take_records()), "");
  c.epoch_a = a.epoch;
  c.epoch_b = b.epoch;
  c.node_a = a.node_id;
  c.node_b = b.node_id;
  return c;
}

TEST(Recovery, NoSchedulingUntilEveryLiveNodeHasReported) {
  Crashed c = crash_with_running_tasks();
  const Clock::time_point t = kT0 + 10s;
  MasterCore core(durable_test_options(), c.state, t, 1);
  EXPECT_TRUE(core.recovering());
  // Unreported nodes are suspect: they keep their tasks, get no new ones.
  EXPECT_TRUE(core.state().node(c.node_a).suspect);
  EXPECT_TRUE(core.state().node(c.node_b).suspect);
  // Kill b's task: room for task 2 on b, but no pass may place it yet.
  v1::KillRequest kq;
  kq.set_task_id(1);
  v1::KillResponse kr;
  ASSERT_TRUE(core.kill(kq, &kr).ok());
  EXPECT_TRUE(core.needs_schedule());
  EXPECT_TRUE(core.tick(t + 100ms).schedule.placements.empty());
  // And the tick does not ask to run again at once: MasterServer's tick
  // thread would otherwise spin with the lock held and starve heartbeats.
  EXPECT_FALSE(core.needs_schedule());
  FakeAgent a{c.node_a, c.epoch_a, {}};
  a.running(0, 1, c.epoch_a);
  v1::HeartbeatResponse ra = a.beat(core, t + 150ms);  // same epoch: accepted, no re-register
  EXPECT_EQ(ra.start_size(), 0);
  EXPECT_EQ(ra.stop_size(), 0);
  EXPECT_TRUE(core.recovering());  // b has not reported
  EXPECT_TRUE(core.tick(t + 200ms).schedule.placements.empty());
  FakeAgent b{c.node_b, c.epoch_b, {}};
  b.running(1, 1, c.epoch_b);  // still running: it was killed after the crash
  v1::HeartbeatResponse rb = b.beat(core, t + 250ms);
  ASSERT_EQ(rb.stop_size(), 1);  // the kill reaches it now
  EXPECT_EQ(rb.stop(0).task_id(), 1u);
  EXPECT_TRUE(core.needs_schedule());  // the last report wakes the tick
  // Every live node has reported: the next tick ends the window early
  // (well before its 1 s) and places task 2.
  const TickResult r = core.tick(t + 300ms);
  EXPECT_FALSE(core.recovering());
  ASSERT_EQ(r.schedule.placements.size(), 1u);
  EXPECT_EQ(r.schedule.placements[0].task, 2u);
  EXPECT_EQ(core.attempt(0), 1u);  // a's task was never disturbed
  EXPECT_CORE_OK(core);
}

TEST(Recovery, ATaskTheNodeDoesNotReportGoesBackToPending) {
  // The log says task 1 runs on b, but b's first report after the restart
  // does not list it: the reply that carried its start was lost.
  Crashed c = crash_with_running_tasks();
  const Clock::time_point t = kT0 + 10s;
  MasterCore core(durable_test_options(), c.state, t, 1);
  FakeAgent b{c.node_b, c.epoch_b, {}};
  v1::HeartbeatResponse rb = b.beat(core, t + 100ms);  // reports nothing
  EXPECT_EQ(rb.start_size(), 0);  // not re-sent: requeued instead
  EXPECT_EQ(core.state().task(1).state, TaskState::kPending);
  std::vector<v1::WalRecord> recs = core.take_records();
  ASSERT_EQ(recs.size(), 1u);
  EXPECT_EQ(recs[0].task_requeued().reason(), v1::TaskRequeued::REASON_UNREPORTED_AFTER_RESTART);
  FakeAgent a{c.node_a, c.epoch_a, {}};
  a.running(0, 1, c.epoch_a);
  a.beat(core, t + 100ms);
  // Placed again (b is the only room) as a new attempt, and started by b's
  // next heartbeat.
  core.tick(t + 200ms);
  EXPECT_EQ(core.state().task(1).state, TaskState::kRunning);
  EXPECT_EQ(core.attempt(1), 2u);
  rb = b.beat(core, t + 300ms);
  ASSERT_EQ(rb.start_size(), 1);
  EXPECT_EQ(rb.start(0).instance().attempt(), 2u);
  EXPECT_CORE_OK(core);
}

TEST(Recovery, AnInstanceTheLogDoesNotKnowIsStopped) {
  Crashed c = crash_with_running_tasks();
  const Clock::time_point t = kT0 + 10s;
  MasterCore core(durable_test_options(), c.state, t, 1);
  FakeAgent a{c.node_a, c.epoch_a, {}};
  a.running(0, 1, c.epoch_a);
  a.running(0, 7, c.epoch_a);   // an attempt the log never made
  a.running(42, 1, c.epoch_a);  // a task id the log never made
  v1::HeartbeatResponse ra = a.beat(core, t + 100ms);
  ASSERT_EQ(ra.stop_size(), 2);
  EXPECT_EQ(core.state().task(0).state, TaskState::kRunning);  // the real one stays
  EXPECT_CORE_OK(core);
}

TEST(Recovery, ALateNodeStaysSuspectAfterTheWindowThenRejoins) {
  Crashed c = crash_with_running_tasks();
  const Clock::time_point t = kT0 + 10s;
  MasterCore core(durable_test_options(), c.state, t, 1);
  FakeAgent a{c.node_a, c.epoch_a, {}};
  a.running(0, 1, c.epoch_a);
  a.beat(core, t + 100ms);
  EXPECT_TRUE(core.tick(t + 999ms).schedule.placements.empty());
  EXPECT_TRUE(core.recovering());
  core.tick(t + 1000ms);  // window over with b still silent
  EXPECT_FALSE(core.recovering());
  EXPECT_TRUE(core.state().node(c.node_b).suspect);
  // b's task stays where the log says, and nothing new goes to b.
  EXPECT_EQ(core.state().task(1).node, c.node_b);
  EXPECT_EQ(core.state().task(2).state, TaskState::kPending);
  FakeAgent b{c.node_b, c.epoch_b, {}};
  b.running(1, 1, c.epoch_b);
  b.beat(core, t + 1200ms);
  EXPECT_FALSE(core.state().node(c.node_b).suspect);
  EXPECT_EQ(core.attempt(1), 1u);  // confirmed, untouched
  EXPECT_CORE_OK(core);
}

TEST(Recovery, ANodeThatNeverReportsIsDeclaredDeadOnTheUsualClock) {
  Crashed c = crash_with_running_tasks();
  const Clock::time_point t = kT0 + 10s;
  MasterCore core(durable_test_options(), c.state, t, 1);
  FakeAgent a{c.node_a, c.epoch_a, {}};
  for (int i = 0; i <= 6; ++i) {  // a keeps beating, b never does
    a.running(0, 1, c.epoch_a);
    a.beat(core, t + std::chrono::milliseconds(500 * i));
    const TickResult r = core.tick(t + std::chrono::milliseconds(500 * i + 1));
    // Silence is counted from the restart, not from before the crash: b
    // is dead only after 6 x 500 ms of silence since t.
    EXPECT_EQ(r.died.empty(), i < 6) << i;
  }
  EXPECT_EQ(core.state().node(c.node_b).state, NodeState::kDead);
  EXPECT_EQ(core.epoch(c.node_b), 0u);
  EXPECT_NE(core.state().task(1).node, std::optional<NodeId>(c.node_b));
  EXPECT_CORE_OK(core);
}

TEST(Recovery, BackoffDeadlinesSurviveARestart) {
  MasterCore core(durable_test_options(), 1);  // backoff base 100 ms
  FakeAgent a = agent_for(register_node(core, "a"));
  submit(core, "flaky", 1, 1000, 128, 1, v1::RESTART_POLICY_ON_FAILURE);
  core.schedule(kT0);
  a.exited(0, 1, a.epoch, 1);
  a.beat(core, kT0 + 50ms);  // backs off until 150 ms
  MasterState log;
  ASSERT_EQ(apply_all(log, core.take_records()), "");
  MasterCore after(durable_test_options(), log, kT0 + 60ms, 1);
  EXPECT_EQ(after.restart_at(0), kT0 + 150ms);
  EXPECT_TRUE(after.tick(kT0 + 149ms).restarted.empty());
  EXPECT_EQ(after.tick(kT0 + 150ms).restarted, std::vector<TaskId>{0});
  EXPECT_CORE_OK(after);
}

TEST(Recovery, EpochsContinueFromTheLogNotTheClock) {
  // A floor of 1 means the wall clock does not help: only the durable
  // counter keeps a restarted master from reissuing an epoch.
  MasterCore core(durable_test_options(), 1);
  const uint64_t e1 = register_node(core, "a").epoch();
  const uint64_t e2 = register_node(core, "b").epoch();
  MasterState log;
  ASSERT_EQ(apply_all(log, core.take_records()), "");
  MasterCore after(durable_test_options(), log, kT0 + 1s, 1);
  const uint64_t e3 = register_node(after, "c", 4000, 4096, kT0 + 1s).epoch();
  EXPECT_GT(e3, std::max(e1, e2));
  EXPECT_CORE_OK(after);
}

TEST(Recovery, AnExitLostWithAnUncommittedBatchIsReportedAgainAndAppliedOnce) {
  // The ordering rule end to end: the master applies an exit, but crashes
  // before the batch is durable, so the reply (with its forget) was never
  // sent either. The agent still holds the exit and reports it to the new
  // master, which applies it exactly once.
  MasterCore core(durable_test_options(), 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  submit(core, "job", 1, 1000, 128);
  core.schedule(kT0);
  a.running(0, 1, a.epoch);
  a.beat(core);
  MasterState durable;
  ASSERT_EQ(apply_all(durable, core.take_records()), "");  // committed
  a.exited(0, 1, a.epoch, 0);
  a.beat(core, kT0 + 100ms);
  EXPECT_EQ(core.state().task(0).state, TaskState::kSucceeded);
  core.take_records();  // ...and lost: never written

  MasterCore after(durable_test_options(), durable, kT0 + 200ms, 1);
  EXPECT_EQ(after.state().task(0).state, TaskState::kRunning);
  a.exited(0, 1, a.epoch, 0);
  v1::HeartbeatResponse r = a.beat(after, kT0 + 300ms);
  EXPECT_EQ(after.state().task(0).state, TaskState::kSucceeded);
  ASSERT_EQ(r.forget_size(), 1);
  a.exited(0, 1, a.epoch, 0);  // a duplicate report changes nothing
  after.take_records();
  a.beat(after, kT0 + 400ms);
  EXPECT_TRUE(after.take_records().empty());
  EXPECT_CORE_OK(after);
}

TEST(Recovery, ADrainInProgressStillTreatsTheExitAsAMove) {
  MasterCore core(durable_test_options(), 1);
  FakeAgent a = agent_for(register_node(core, "a"));
  FakeAgent b = agent_for(register_node(core, "b"));
  submit(core, "web", 1, 1000, 128, 1, v1::RESTART_POLICY_NEVER);
  core.schedule(kT0);
  const NodeId on = *core.state().task(0).node;
  FakeAgent& host = on == a.node_id ? a : b;
  v1::DrainRequest dq;
  dq.set_node(on == a.node_id ? "a" : "b");
  v1::DrainResponse dr;
  ASSERT_TRUE(core.drain(dq, &dr).ok());
  host.running(0, 1, host.epoch);
  ASSERT_EQ(host.beat(core, kT0 + 100ms).stop_size(), 1);  // moving
  MasterState log;
  ASSERT_EQ(apply_all(log, core.take_records()), "");
  MasterCore after(durable_test_options(), log, kT0 + 200ms, 1);
  // The stop took effect while the master was down: SIGTERM, 143. A
  // restart that forgot the move would finish this never-restart task as
  // failed; it must move it instead.
  host.exited(0, 1, host.epoch, 143);
  host.beat(after, kT0 + 300ms);
  EXPECT_EQ(after.state().task(0).state, TaskState::kPending);
  EXPECT_CORE_OK(after);
}

}  // namespace
}  // namespace fleet
