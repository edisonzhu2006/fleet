// End-to-end master restarts over real loopback gRPC (m4): an in-process
// MasterServer with a state directory, in-process Agents running real
// processes, and the master stopped and started again on the same port and
// directory while the agents and their tasks keep running.
//
// Stopping a MasterServer writes nothing beyond what every RPC already
// committed, so for the files on disk a shutdown here is a crash; the
// SIGKILL version of this, 20 times, is bench/master_failure.sh.
//
// Timing as in integration_test.cpp: 100 ms heartbeats, waits poll with a
// generous limit instead of sleeping fixed amounts.

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <map>
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

class MasterRestartTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = fs::path("/private/tmp/fleet") /
            ("mr-" + std::to_string(::getpid()) + "-" +
             ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(root_);
    fs::create_directories(root_);
    start_master(0);
  }

  void TearDown() override {
    for (auto& a : agents_) a->request_stop();
    agents_.clear();
    if (master_) {
      master_->with_core([](MasterCore& c) {
        const std::string e = c.check_invariants();
        EXPECT_TRUE(e.empty()) << e;
      });
      master_->shutdown();
    }
    fs::remove_all(root_);
  }

  // Port 0 picks one; a restart passes the port the first master got, so
  // the agents' channels reconnect to it.
  void start_master(int port, uint64_t snapshot_every = 10000) {
    MasterServerOptions mo;
    mo.listen_address = "127.0.0.1:" + std::to_string(port);
    mo.tick_interval = 50ms;
    mo.core.heartbeat_interval_ms = 100;
    mo.core.suspect_after_missed = 10;  // as in integration_test.cpp (TSan)
    mo.core.dead_after_missed = 30;
    mo.core.recovery_window = 2s;
    mo.state_dir = root_ / "state";
    mo.storage.snapshot_every = snapshot_every;  // real F_FULLFSYNC syncs
    master_ = std::make_unique<MasterServer>(mo);
    std::string err;
    ASSERT_TRUE(master_->start(&err)) << err;
    address_ = "127.0.0.1:" + std::to_string(master_->port());
    stub_ = v1::Master::NewStub(grpc::CreateChannel(address_, grpc::InsecureChannelCredentials()));
  }

  void restart_master(uint64_t snapshot_every = 10000) {
    const int port = master_->port();
    master_->shutdown();
    master_.reset();
    std::this_thread::sleep_for(300ms);  // agents' heartbeats fail meanwhile
    start_master(port, snapshot_every);
  }

  Agent& add_agent(const std::string& name) {
    AgentOptions o;
    o.master_address = address_;
    o.name = name;
    o.capacity = {4000, 4096};
    o.work_dir = root_ / name;
    o.event_log = root_ / (name + ".events");
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

  std::vector<uint32_t> submit(const std::string& job, std::vector<std::string> argv,
                               uint32_t count) {
    v1::SubmitRequest req;
    req.set_job(job);
    req.set_priority(1);
    req.mutable_request()->set_milli_cpu(1000);
    req.mutable_request()->set_memory_mb(64);
    for (auto& a : argv) req.add_argv(a);
    req.set_count(count);
    v1::SubmitResponse resp;
    grpc::Status s = stub_->Submit(ctx().get(), req, &resp);
    EXPECT_TRUE(s.ok()) << s.error_message();
    return {resp.task_ids().begin(), resp.task_ids().end()};
  }

  std::map<uint32_t, v1::TaskInfo> tasks() {
    v1::ListResponse resp;
    if (!stub_->List(ctx().get(), v1::ListRequest{}, &resp).ok()) return {};
    std::map<uint32_t, v1::TaskInfo> out;
    for (const auto& t : resp.tasks()) out[t.id()] = t;
    return out;
  }

  bool wait_for(const std::function<bool()>& pred, std::chrono::milliseconds limit = 15s) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < end) {
      if (pred()) return true;
      std::this_thread::sleep_for(20ms);
    }
    return pred();
  }

  bool all_in(const std::vector<uint32_t>& ids, v1::TaskState s) {
    auto t = tasks();
    for (uint32_t id : ids) {
      if (!t.count(id) || t[id].state() != s) return false;
    }
    return true;
  }

  // "start" lines per task id across every agent's event log.
  std::map<uint32_t, int> starts_per_task() {
    std::map<uint32_t, int> n;
    for (const auto& e : fs::directory_iterator(root_)) {
      if (e.path().extension() != ".events") continue;
      std::ifstream in(e.path());
      long long ts = 0;
      std::string what, node;
      uint32_t task = 0, attempt = 0;
      unsigned long long epoch = 0;
      while (in >> ts >> what >> task >> attempt >> epoch >> node) {
        if (what == "start") ++n[task];
      }
    }
    return n;
  }

  fs::path root_;
  std::unique_ptr<MasterServer> master_;
  std::string address_;
  std::unique_ptr<v1::Master::Stub> stub_;
  std::vector<std::unique_ptr<Agent>> agents_;
};

TEST_F(MasterRestartTest, RunningTasksSurviveAndNewWorkIsScheduledAfterRestart) {
  add_agent("a");
  add_agent("b");
  const auto longs = submit("long", {"/bin/sh", "-c", "sleep 30"}, 3);
  ASSERT_TRUE(wait_for([&] { return all_in(longs, v1::TASK_STATE_RUNNING); }));
  // Every agent has started its share (the reply carrying each start was
  // sent, i.e. its record was durable first).
  ASSERT_TRUE(wait_for([&] { return agents_[0]->stats().running + agents_[1]->stats().running == 3; }));
  const auto before = tasks();
  const uint64_t fenced_before = agents_[0]->stats().fenced + agents_[1]->stats().fenced;

  restart_master();
  EXPECT_GT(master_->recovery_info().lsn, 0u);
  EXPECT_EQ(master_->recovery_info().torn_bytes, 0u);
  // The agents are back within the report window: it closes early.
  ASSERT_TRUE(wait_for([&] {
    bool rec = true;
    master_->with_core([&](MasterCore& c) { rec = c.recovering(); });
    return !rec;
  }));
  // Same tasks, same nodes, same attempts: nothing was restarted, and the
  // agents kept their epochs (no fencing, no re-registration).
  const auto after = tasks();
  for (uint32_t id : longs) {
    EXPECT_EQ(after.at(id).state(), v1::TASK_STATE_RUNNING);
    EXPECT_EQ(after.at(id).attempt(), before.at(id).attempt());
    EXPECT_EQ(after.at(id).node(), before.at(id).node());
  }
  EXPECT_EQ(agents_[0]->stats().fenced + agents_[1]->stats().fenced, fenced_before);
  // New work runs, and ids continue after the recovered ones.
  const auto shorts = submit("short", {"/bin/sh", "-c", "exit 0"}, 2);
  ASSERT_EQ(shorts.size(), 2u);
  EXPECT_EQ(shorts[0], longs.back() + 1);
  ASSERT_TRUE(wait_for([&] { return all_in(shorts, v1::TASK_STATE_SUCCEEDED); }));
  // Each task's process was started exactly once, across both masters.
  const auto starts = starts_per_task();
  for (uint32_t id : longs) EXPECT_EQ(starts.count(id) ? starts.at(id) : 0, 1) << id;
  for (uint32_t id : shorts) EXPECT_EQ(starts.count(id) ? starts.at(id) : 0, 1) << id;
}

TEST_F(MasterRestartTest, ExitsDuringTheOutageAreAppliedOnceAfterIt) {
  add_agent("a");
  // Tasks that finish while the master is down: their agents keep the
  // exits (unacknowledged) and report them to the restarted master.
  const auto ids = submit("brief", {"/bin/sh", "-c", "sleep 0.5"}, 2);
  ASSERT_TRUE(wait_for([&] { return all_in(ids, v1::TASK_STATE_RUNNING); }));
  ASSERT_TRUE(wait_for([&] { return agents_[0]->stats().running == 2; }));
  const int port = master_->port();
  master_->shutdown();
  master_.reset();
  std::this_thread::sleep_for(900ms);  // both exit meanwhile
  start_master(port);
  ASSERT_TRUE(wait_for([&] { return all_in(ids, v1::TASK_STATE_SUCCEEDED); }));
  const auto t = tasks();
  for (uint32_t id : ids) {
    EXPECT_EQ(t.at(id).attempt(), 1u);
    EXPECT_EQ(t.at(id).exit_code(), 0);
  }
}

TEST_F(MasterRestartTest, RecoversFromASnapshotPlusTheLogAfterIt) {
  restart_master(5);  // a master that snapshots every 5 records
  add_agent("a");
  add_agent("b");
  std::vector<uint32_t> all;
  for (int i = 0; i < 4; ++i) {
    auto ids = submit("s" + std::to_string(i), {"/bin/sh", "-c", "exit 0"}, 2);
    all.insert(all.end(), ids.begin(), ids.end());
  }
  ASSERT_TRUE(wait_for([&] { return all_in(all, v1::TASK_STATE_SUCCEEDED); }));
  EXPECT_GT(master_->storage_stats().snapshots, 0u);
  restart_master(5);
  EXPECT_TRUE(master_->recovery_info().had_snapshot);
  EXPECT_GT(master_->recovery_info().snapshot_lsn, 0u);
  EXPECT_TRUE(all_in(all, v1::TASK_STATE_SUCCEEDED));
  const auto more = submit("more", {"/bin/sh", "-c", "exit 0"}, 1);
  ASSERT_TRUE(wait_for([&] { return all_in(more, v1::TASK_STATE_SUCCEEDED); }));
}

TEST_F(MasterRestartTest, ACorruptLogStopsTheMasterFromStarting) {
  add_agent("a");
  submit("x", {"/bin/sh", "-c", "exit 0"}, 3);
  const int port = master_->port();
  for (auto& a : agents_) a->request_stop();
  agents_.clear();
  master_->shutdown();
  master_.reset();
  // Flip one byte in the first record: damage in the middle of the log.
  const fs::path wal = root_ / "state" / "wal";
  std::fstream f(wal, std::ios::in | std::ios::out | std::ios::binary);
  f.seekp(10);
  f.put('\x7f');
  f.close();
  MasterServerOptions mo;
  mo.listen_address = "127.0.0.1:" + std::to_string(port);
  mo.state_dir = root_ / "state";
  MasterServer m(mo);
  std::string err;
  EXPECT_FALSE(m.start(&err));
  EXPECT_NE(err.find("corrupt record at offset 0"), std::string::npos) << err;
}

}  // namespace
}  // namespace fleet
