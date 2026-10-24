// End-to-end tests over real loopback gRPC: an in-process MasterServer on
// an ephemeral port, in-process Agents with ProcessRuntime running real
// processes, and a generated client stub standing in for fleetctl. These
// cover the m2 "done when": submit, run to completion, kill; plus drain,
// preemption, agent restart (incarnation fencing) and deadlines.
//
// Timing: the master tells agents to heartbeat every 100 ms here (500 ms
// in production) so each test finishes in about a second. Waits poll List
// with a generous limit rather than sleeping fixed amounts, so a loaded
// machine makes them slower, not flaky.

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <sstream>
#include <thread>
#include <unistd.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "agent.h"
#include "fleet.grpc.pb.h"
#include "master_server.h"
#include "process_runtime.h"

namespace fleet {
namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class ClusterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = fs::path("/private/tmp/fleet") /
            ("it-" + std::to_string(::getpid()) + "-" +
             ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(root_);
    fs::create_directories(root_);
    MasterServerOptions mo;
    mo.listen_address = "127.0.0.1:0";
    mo.schedule_interval = 50ms;
    mo.core.heartbeat_interval_ms = 100;
    master_ = std::make_unique<MasterServer>(mo);
    std::string err;
    ASSERT_TRUE(master_->start(&err)) << err;
    address_ = "127.0.0.1:" + std::to_string(master_->port());
    stub_ = v1::Master::NewStub(
        grpc::CreateChannel(address_, grpc::InsecureChannelCredentials()));
  }

  void TearDown() override {
    for (auto& a : agents_) a->request_stop();
    agents_.clear();  // joins; each agent kills its tasks
    if (master_) {
      master_->with_core([](MasterCore& c) {
        const std::string e = c.check_invariants();
        EXPECT_TRUE(e.empty()) << e;
      });
      master_->shutdown();
    }
    fs::remove_all(root_);
  }

  Agent& add_agent(const std::string& name, int64_t mcpu = 4000, int64_t mem = 4096) {
    AgentOptions o;
    o.master_address = address_;
    o.name = name;
    o.capacity = {mcpu, mem};
    o.work_dir = root_ / name;
    ProcessRuntimeOptions ro;
    ro.stop_grace = 500ms;
    agents_.push_back(std::make_unique<Agent>(o, std::make_unique<ProcessRuntime>(ro)));
    agents_.back()->start_background();
    return *agents_.back();
  }

  std::unique_ptr<grpc::ClientContext> ctx() {
    auto c = std::make_unique<grpc::ClientContext>();
    c->set_deadline(std::chrono::system_clock::now() + 2s);
    return c;
  }

  std::vector<uint32_t> submit(const std::string& job, int prio, int64_t mcpu,
                               std::vector<std::string> argv, uint32_t count = 1) {
    v1::SubmitRequest req;
    req.set_job(job);
    req.set_priority(prio);
    req.mutable_request()->set_milli_cpu(mcpu);
    req.mutable_request()->set_memory_mb(64);
    for (auto& a : argv) req.add_argv(a);
    req.set_count(count);
    v1::SubmitResponse resp;
    grpc::Status s = stub_->Submit(ctx().get(), req, &resp);
    EXPECT_TRUE(s.ok()) << s.error_message();
    return {resp.task_ids().begin(), resp.task_ids().end()};
  }

  v1::ListResponse list() {
    v1::ListResponse resp;
    grpc::Status s = stub_->List(ctx().get(), v1::ListRequest{}, &resp);
    EXPECT_TRUE(s.ok()) << s.error_message();
    return resp;
  }

  v1::TaskInfo task(uint32_t id) {
    // Keep the response alive: in C++20, `for (x : list().tasks())`
    // destroys the temporary response before the loop body runs.
    const v1::ListResponse all = list();
    for (const auto& t : all.tasks()) {
      if (t.id() == id) return t;
    }
    ADD_FAILURE() << "no task " << id;
    return {};
  }

  // Polls until pred() holds; fails the test after `limit`.
  bool wait_for(const std::function<bool()>& pred, std::chrono::milliseconds limit = 10s) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < end) {
      if (pred()) return true;
      std::this_thread::sleep_for(20ms);
    }
    return pred();
  }

  bool wait_state(uint32_t id, v1::TaskState s) {
    return wait_for([&] { return task(id).state() == s; });
  }

  size_t running_processes() {
    size_t n = 0;
    for (auto& a : agents_) n += a->stats().running;
    return n;
  }

  static std::string slurp(const fs::path& p) {
    std::ifstream in(p);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
  }

  fs::path root_;
  std::unique_ptr<MasterServer> master_;
  std::string address_;
  std::unique_ptr<v1::Master::Stub> stub_;
  std::vector<std::unique_ptr<Agent>> agents_;
};

TEST_F(ClusterTest, SubmitRunsToCompletionAcrossAgents) {
  add_agent("a");
  add_agent("b");
  add_agent("c");
  ASSERT_TRUE(wait_for([&] { return list().nodes_size() == 3; }));

  // Six 1.5-core tasks on three 4-core nodes: at most two fit per node, so
  // placement must use all three.
  auto ok = submit("hello", 1, 1500, {"/bin/sh", "-c", "echo hello from task; sleep 0.3"}, 6);
  auto bad = submit("bad", 1, 100, {"/bin/sh", "-c", "exit 3"});
  auto missing = submit("missing", 1, 100, {"no-such-command-fleet"});

  for (uint32_t id : ok) ASSERT_TRUE(wait_state(id, v1::TASK_STATE_SUCCEEDED)) << id;
  ASSERT_TRUE(wait_state(bad[0], v1::TASK_STATE_FAILED));
  ASSERT_TRUE(wait_state(missing[0], v1::TASK_STATE_FAILED));

  std::set<std::string> nodes;
  for (uint32_t id : ok) {
    v1::TaskInfo t = task(id);
    EXPECT_TRUE(t.has_exit());
    EXPECT_EQ(t.exit_code(), 0);
    EXPECT_EQ(t.attempt(), 1u);
    nodes.insert(t.node());
    const fs::path log = root_ / t.node() / ("hello.t" + std::to_string(id) + ".a1.log");
    EXPECT_NE(slurp(log).find("hello from task"), std::string::npos) << log;
  }
  EXPECT_EQ(nodes.size(), 3u);
  EXPECT_EQ(task(bad[0]).exit_code(), 3);
  EXPECT_NE(task(missing[0]).message().find("command not found"), std::string::npos);

  // Everything finished: resources released, agents idle.
  ASSERT_TRUE(wait_for([&] { return running_processes() == 0; }));
  const v1::ListResponse final_list = list();
  for (const auto& n : final_list.nodes()) EXPECT_EQ(n.allocated().milli_cpu(), 0) << n.name();
}

TEST_F(ClusterTest, KillStopsTheProcess) {
  add_agent("a");
  auto ids = submit("sleeper", 1, 500, {"/bin/sh", "-c", "echo $$; exec sleep 30"});
  ASSERT_TRUE(wait_state(ids[0], v1::TASK_STATE_RUNNING));
  ASSERT_TRUE(wait_for([&] { return running_processes() == 1; }));
  const fs::path log = root_ / "a" / ("sleeper.t" + std::to_string(ids[0]) + ".a1.log");
  pid_t pid = 0;
  ASSERT_TRUE(wait_for([&] {
    // On macOS the log starts with ProcessRuntime's "memory limit not
    // enforced" line, so look for the line that is just the pid.
    std::istringstream in(slurp(log));
    for (std::string line; std::getline(in, line);) {
      if (!line.empty() && line.find_first_not_of("0123456789") == std::string::npos) {
        pid = static_cast<pid_t>(std::stol(line));
      }
    }
    return pid > 0;
  }));
  ASSERT_EQ(::kill(pid, 0), 0);  // really running

  v1::KillRequest req;
  req.set_task_id(ids[0]);
  v1::KillResponse resp;
  ASSERT_TRUE(stub_->Kill(ctx().get(), req, &resp).ok());
  EXPECT_EQ(resp.killed_size(), 1);
  EXPECT_EQ(task(ids[0]).state(), v1::TASK_STATE_KILLED);  // desired state: at once
  // Actual state follows within a heartbeat or two.
  ASSERT_TRUE(wait_for([&] { return running_processes() == 0; }));
  EXPECT_NE(::kill(pid, 0), 0) << "process " << pid << " still alive";
}

TEST_F(ClusterTest, DrainMovesTaskToAnotherNode) {
  add_agent("a");
  add_agent("b");
  ASSERT_TRUE(wait_for([&] { return list().nodes_size() == 2; }));
  auto ids = submit("svc", 2, 1000, {"sleep", "30"});
  ASSERT_TRUE(wait_state(ids[0], v1::TASK_STATE_RUNNING));
  ASSERT_TRUE(wait_for([&] { return running_processes() == 1; }));
  const std::string first = task(ids[0]).node();

  v1::DrainRequest req;
  req.set_node(first);
  v1::DrainResponse resp;
  ASSERT_TRUE(stub_->Drain(ctx().get(), req, &resp).ok());
  ASSERT_EQ(resp.evicted_size(), 1);

  ASSERT_TRUE(wait_for([&] {
    v1::TaskInfo t = task(ids[0]);
    return t.state() == v1::TASK_STATE_RUNNING && t.node() != first && t.attempt() == 2;
  }));
  // The old copy is stopped and the new one started: one process total.
  ASSERT_TRUE(wait_for([&] {
    const v1::ListResponse l = list();
    size_t on_first = 0, on_second = 0;
    for (auto& a : agents_) {
      const Agent::Stats st = a->stats();
      if (l.nodes(static_cast<int>(st.node_id)).name() == first) {
        on_first += st.running;
      } else {
        on_second += st.running;
      }
    }
    return on_first == 0 && on_second == 1;
  }));
}

TEST_F(ClusterTest, PreemptionOverTheWire) {
  add_agent("a", 1000, 1024);
  auto batch = submit("batch", 0, 1000, {"sleep", "30"});
  ASSERT_TRUE(wait_state(batch[0], v1::TASK_STATE_RUNNING));
  ASSERT_TRUE(wait_for([&] { return running_processes() == 1; }));

  auto prod = submit("prod", 2, 1000, {"/bin/sh", "-c", "sleep 0.2"});
  ASSERT_TRUE(wait_state(prod[0], v1::TASK_STATE_SUCCEEDED));
  // The victim went back to pending and, once prod finished, ran again as
  // a new attempt.
  ASSERT_TRUE(wait_for([&] {
    v1::TaskInfo t = task(batch[0]);
    return t.state() == v1::TASK_STATE_RUNNING && t.attempt() == 2;
  }));
  EXPECT_EQ(task(batch[0]).preempted_count(), 1u);
  ASSERT_TRUE(wait_for([&] { return running_processes() == 1; }));
}

TEST_F(ClusterTest, AgentRestartRequeuesUnderNewIncarnation) {
  add_agent("a");
  auto ids = submit("svc", 1, 1000, {"sleep", "30"});
  ASSERT_TRUE(wait_state(ids[0], v1::TASK_STATE_RUNNING));
  ASSERT_TRUE(wait_for([&] { return running_processes() == 1; }));

  // Stop the agent (it kills its tasks) and start a new one with the same
  // name, as after a fleetlet restart.
  agents_.front()->request_stop();
  agents_.front()->join();
  agents_.clear();
  add_agent("a");
  ASSERT_TRUE(wait_for([&] {
    v1::TaskInfo t = task(ids[0]);
    return t.state() == v1::TASK_STATE_RUNNING && t.attempt() == 2;
  }));
  ASSERT_TRUE(wait_for([&] { return running_processes() == 1; }));
  EXPECT_EQ(list().nodes_size(), 1);  // same node, not a new one
}

TEST_F(ClusterTest, DeadlinesBoundCallsToADeadMaster) {
  // Port 1 on loopback: nothing listens there.
  auto dead = v1::Master::NewStub(
      grpc::CreateChannel("127.0.0.1:1", grpc::InsecureChannelCredentials()));
  grpc::ClientContext c;
  c.set_deadline(std::chrono::system_clock::now() + 300ms);
  v1::ListResponse resp;
  const auto t0 = std::chrono::steady_clock::now();
  grpc::Status s = dead->List(&c, v1::ListRequest{}, &resp);
  EXPECT_FALSE(s.ok());
  EXPECT_TRUE(s.error_code() == grpc::StatusCode::UNAVAILABLE ||
              s.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED)
      << s.error_code();
  EXPECT_LT(std::chrono::steady_clock::now() - t0, 2s);

  // An agent pointed at nothing keeps trying, and still stops promptly.
  AgentOptions o;
  o.master_address = "127.0.0.1:1";
  o.name = "lost";
  o.capacity = {1000, 1024};
  o.work_dir = root_ / "lost";
  o.heartbeat_interval = 100ms;
  Agent lost(o, std::make_unique<ProcessRuntime>());
  lost.start_background();
  ASSERT_TRUE(wait_for([&] { return lost.stats().heartbeats_failed >= 2; }, 5s));
  EXPECT_FALSE(lost.stats().registered);
  const auto t1 = std::chrono::steady_clock::now();
  lost.request_stop();
  lost.join();
  EXPECT_LT(std::chrono::steady_clock::now() - t1, 1s);
}

}  // namespace
}  // namespace fleet
