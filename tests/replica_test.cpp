// End-to-end tests of the Raft-replicated master (m4b) over real loopback
// gRPC: three in-process ReplicaServers on fixed ports, in-process Agents
// running real processes and given all three addresses, and a client that
// talks to whichever replica it is pointed at.
//
// Covered: one leader is elected and the others redirect; tasks keep
// running untouched when the leader is stopped (for the files a shutdown
// is a crash) and the new leader serves after its no-op and the report
// window; a stopped replica rejoins and catches up, by log or by snapshot;
// a leader cut off from the others cannot commit or even answer, the
// majority elects a new leader, and on healing the old one steps down and
// its uncommitted entries vanish; after each test every replica's applied
// state is byte-identical, and the leader's speculative state equals its
// applied state once nothing is uncommitted. The SIGKILL versions, 20
// times over, are in bench/leader_failover.sh.
//
// Timing: 100 ms agent heartbeats; Raft elections 300 to 600 ms with 50 ms
// heartbeats (wider than production's 150 to 300 so TSan's slowdown does
// not cause spurious elections); waits poll with generous limits.

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <chrono>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <netinet/in.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "agent.h"
#include "fleet.grpc.pb.h"
#include "leader_redirect.h"
#include "process_runtime.h"
#include "replica_server.h"

namespace fleet {
namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

// A port nobody is listening on right now. (The OS could hand it to
// someone else before the replica binds it; on a test machine it does not.)
int free_port() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  a.sin_port = 0;
  ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
  socklen_t len = sizeof(a);
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
  const int port = ntohs(a.sin_port);
  ::close(fd);
  return port;
}

bool eventually(const std::function<bool()>& pred, std::chrono::milliseconds limit = 15s) {
  const auto end = std::chrono::steady_clock::now() + limit;
  while (std::chrono::steady_clock::now() < end) {
    if (pred()) return true;
    std::this_thread::sleep_for(20ms);
  }
  return pred();
}

class ReplicaTest : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = fs::path("/private/tmp/fleet") /
            ("rp-" + std::to_string(::getpid()) + "-" +
             ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(root_);
    fs::create_directories(root_);
  }

  void TearDown() override {
    for (auto& a : agents_) a->request_stop();
    agents_.clear();
    check_replicas_agree();
    for (auto& [id, r] : replicas_) {
      if (r) r->shutdown();
    }
    replicas_.clear();
    fs::remove_all(root_);
  }

  void start_cluster(int n, uint64_t snapshot_every = 10000) {
    snapshot_every_ = snapshot_every;
    for (int i = 1; i <= n; ++i) {
      members_[static_cast<uint32_t>(i)] = "127.0.0.1:" + std::to_string(free_port());
    }
    for (const auto& [id, addr] : members_) start_replica(id);
  }

  void start_replica(uint32_t id) {
    ReplicaOptions o;
    o.id = id;
    o.members = members_;
    o.state_dir = root_ / ("r" + std::to_string(id));
    o.storage.sync = SyncMode::kFsync;  // real syncs, cheaper than F_FULLFSYNC
    o.election_timeout_min = 300ms;
    o.election_timeout_max = 600ms;
    o.raft_heartbeat = 50ms;
    o.tick_interval = 50ms;
    o.snapshot_every = snapshot_every_;
    o.core.heartbeat_interval_ms = 100;
    o.core.suspect_after_missed = 10;  // as in integration_test.cpp (TSan)
    o.core.dead_after_missed = 30;
    o.core.recovery_window = 2s;
    auto r = std::make_unique<ReplicaServer>(o);
    std::string err;
    ASSERT_TRUE(r->start(&err)) << err;
    replicas_[id] = std::move(r);
  }

  // A crash, as far as the files and the peers can tell.
  void stop_replica(uint32_t id) {
    replicas_.at(id)->shutdown();
    replicas_.at(id).reset();
  }

  std::string all_addresses() const {
    std::string s;
    for (const auto& [id, addr] : members_) s += (s.empty() ? "" : ",") + addr;
    return s;
  }

  // The replica serving as leader (waits for one), 0 if none in time.
  uint32_t leader(std::chrono::milliseconds limit = 15s) {
    uint32_t found = 0;
    eventually(
        [&] {
          for (auto& [id, r] : replicas_) {
            if (r && r->status().serving) {
              found = id;
              return true;
            }
          }
          return false;
        },
        limit);
    return found;
  }

  v1::Master::Stub& stub(uint32_t id) {
    auto& s = stubs_[id];
    if (!s) {
      s = v1::Master::NewStub(
          grpc::CreateChannel(members_.at(id), grpc::InsecureChannelCredentials()));
    }
    return *s;
  }

  grpc::Status submit_at(uint32_t id, const std::string& job, std::vector<std::string> argv,
                         uint32_t count = 1, std::vector<uint32_t>* ids = nullptr,
                         std::chrono::milliseconds deadline = 3s) {
    v1::SubmitRequest req;
    req.set_job(job);
    req.set_priority(1);
    req.mutable_request()->set_milli_cpu(250);
    req.mutable_request()->set_memory_mb(32);
    for (auto& a : argv) req.add_argv(a);
    req.set_count(count);
    v1::SubmitResponse resp;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + deadline);
    grpc::Status s = stub(id).Submit(&ctx, req, &resp);
    if (ids) ids->assign(resp.task_ids().begin(), resp.task_ids().end());
    return s;
  }

  // List at the current leader (retrying across a leader change).
  v1::ListResponse list() {
    v1::ListResponse resp;
    eventually([&] {
      const uint32_t l = leader(5s);
      if (!l) return false;
      resp.Clear();
      grpc::ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + 2s);
      return stub(l).List(&ctx, v1::ListRequest{}, &resp).ok();
    });
    return resp;
  }

  bool tasks_running(const std::vector<uint32_t>& ids, uint32_t attempt = 0) {
    const v1::ListResponse r = list();
    for (uint32_t id : ids) {
      bool ok = false;
      for (const auto& t : r.tasks()) {
        if (t.id() == id && t.state() == v1::TASK_STATE_RUNNING &&
            (attempt == 0 || t.attempt() == attempt)) {
          ok = true;
        }
      }
      if (!ok) return false;
    }
    return true;
  }

  // Processes the agents really run (the master's RUNNING means placed;
  // the start reaches the agent with its next heartbeat reply).
  size_t agents_running() const {
    size_t n = 0;
    for (const auto& a : agents_) n += a->stats().running;
    return n;
  }
  uint64_t agents_started() const {
    uint64_t n = 0;
    for (const auto& a : agents_) n += a->stats().started;
    return n;
  }
  uint64_t agents_fenced() const {
    uint64_t n = 0;
    for (const auto& a : agents_) n += a->stats().fenced;
    return n;
  }

  Agent& add_agent(const std::string& name) {
    AgentOptions o;
    o.master_address = all_addresses();
    o.name = name;
    o.capacity = {4000, 4096};
    o.work_dir = root_ / name;
    ProcessRuntimeOptions ro;
    ro.stop_grace = 500ms;
    agents_.push_back(std::make_unique<Agent>(o, std::make_unique<ProcessRuntime>(ro)));
    agents_.back()->start_background();
    return *agents_.back();
  }

  // State Machine Safety, end to end: every running replica reaches the
  // same applied index with byte-identical state, and the leader's
  // speculative state equals its applied state.
  void check_replicas_agree() {
    std::string why;
    const bool ok = eventually([&] {
      why.clear();
      std::string first;
      uint64_t first_index = 0;
      bool have = false;
      for (auto& [id, r] : replicas_) {
        if (!r) continue;
        uint64_t idx = 0;
        const std::string s = r->applied_state(&idx);
        if (!have) {
          first = s;
          first_index = idx;
          have = true;
        } else if (idx != first_index || s != first) {
          why = "replica " + std::to_string(id) + " at index " + std::to_string(idx) +
                " differs from index " + std::to_string(first_index);
          return false;
        }
        const std::string m = r->check_leader_matches_applied();
        if (!m.empty()) {
          why = m;
          return false;
        }
      }
      return true;
    }, 10s);
    EXPECT_TRUE(ok) << why;
  }

  fs::path root_;
  uint64_t snapshot_every_ = 10000;
  std::map<uint32_t, std::string> members_;
  std::map<uint32_t, std::unique_ptr<ReplicaServer>> replicas_;
  std::map<uint32_t, std::unique_ptr<v1::Master::Stub>> stubs_;
  std::vector<std::unique_ptr<Agent>> agents_;
};

TEST_F(ReplicaTest, OneLeaderIsElectedAndTheOthersRedirectToIt) {
  start_cluster(3);
  const uint32_t l = leader();
  ASSERT_NE(l, 0u);
  int serving = 0;
  for (auto& [id, r] : replicas_) serving += r->status().serving ? 1 : 0;
  EXPECT_EQ(serving, 1);
  const uint32_t f = l == 1 ? 2 : 1;
  ASSERT_TRUE(eventually([&] { return replicas_.at(f)->status().leader_id == l; }));
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + 2s);
  v1::ListResponse resp;
  const grpc::Status s = stub(f).List(&ctx, v1::ListRequest{}, &resp);
  EXPECT_EQ(s.error_code(), grpc::StatusCode::UNAVAILABLE);
  EXPECT_EQ(leader_hint(ctx), members_.at(l));  // where to go instead
  ASSERT_TRUE(submit_at(l, "hello", {"true"}).ok());
}

TEST_F(ReplicaTest, TasksSurviveLeaderFailoverAndTheNewLeaderSchedules) {
  start_cluster(3);
  const uint32_t first = leader();
  ASSERT_NE(first, 0u);
  add_agent("a1");
  add_agent("a2");
  std::vector<uint32_t> svc;
  ASSERT_TRUE(eventually([&] { return submit_at(leader(), "svc", {"sleep", "30"}, 4, &svc).ok(); }));
  ASSERT_TRUE(eventually([&] { return tasks_running(svc, 1) && agents_running() == 4; }));
  const uint64_t started = agents_started();
  ASSERT_EQ(started, 4u);

  stop_replica(first);
  const uint32_t second = leader();
  ASSERT_NE(second, 0u);
  EXPECT_NE(second, first);
  // The services were never disturbed: same attempt, no extra starts, no
  // fencing, and the new leader accepted the agents' epochs as they were.
  ASSERT_TRUE(eventually([&] { return tasks_running(svc, 1); }));
  // The new leader schedules once the report window ends.
  std::vector<uint32_t> more;
  ASSERT_TRUE(eventually([&] { return submit_at(leader(), "more", {"sleep", "30"}, 2, &more).ok(); }));
  ASSERT_TRUE(eventually([&] { return tasks_running(more) && agents_running() == 6; }));
  EXPECT_TRUE(tasks_running(svc, 1));
  EXPECT_EQ(agents_started(), started + 2);  // the 4 services were never restarted
  EXPECT_EQ(agents_fenced(), 0u);

  // The old leader comes back as a follower and catches up.
  start_replica(first);
  ASSERT_TRUE(eventually([&] {
    const auto s = replicas_.at(first)->status();
    return s.role == raft::Role::kFollower &&
           s.applied_index == replicas_.at(second)->status().applied_index;
  }));
}

TEST_F(ReplicaTest, APartitionedLeaderCannotCommitAndStepsDownOnHeal) {
  start_cluster(3);
  const uint32_t old_leader = leader();
  ASSERT_NE(old_leader, 0u);
  add_agent("a1");
  std::vector<uint32_t> before;
  ASSERT_TRUE(eventually([&] { return submit_at(leader(), "before", {"sleep", "30"}, 1, &before).ok(); }));
  ASSERT_TRUE(eventually([&] { return tasks_running(before, 1) && agents_running() == 1; }));
  const ReplicaStatusInfo s0 = replicas_.at(old_leader)->status();

  replicas_.at(old_leader)->set_isolated(true);
  // It still believes it leads (nothing told it otherwise), but it can
  // neither commit nor confirm its leadership, so it answers nothing: a
  // submission is logged and then reported as of unknown outcome.
  const grpc::Status cut = submit_at(old_leader, "split", {"true"});
  EXPECT_FALSE(cut.ok());
  EXPECT_TRUE(cut.error_code() == grpc::StatusCode::ABORTED ||
              cut.error_code() == grpc::StatusCode::UNAVAILABLE)
      << cut.error_message();
  // The majority elects a new leader of a higher term.
  uint32_t new_leader = 0;
  ASSERT_TRUE(eventually([&] {
    for (auto& [id, r] : replicas_) {
      const auto st = r->status();
      if (id != old_leader && st.serving && st.term > s0.term) new_leader = id;
    }
    return new_leader != 0;
  }));
  const ReplicaStatusInfo stale = replicas_.at(old_leader)->status();
  EXPECT_EQ(stale.role, raft::Role::kLeader);  // still thinks so...
  EXPECT_EQ(stale.term, s0.term);
  EXPECT_GT(stale.last_index, s0.commit_index);  // ...holds the "split" entries...
  EXPECT_LE(stale.commit_index, s0.commit_index + 1);  // ...and cannot commit them
  std::vector<uint32_t> after;
  ASSERT_TRUE(eventually([&] { return submit_at(new_leader, "after", {"sleep", "30"}, 1, &after).ok(); }));

  replicas_.at(old_leader)->set_isolated(false);
  ASSERT_TRUE(eventually([&] {
    const auto st = replicas_.at(old_leader)->status();
    return st.role == raft::Role::kFollower && st.term >= replicas_.at(new_leader)->status().term &&
           st.applied_index == replicas_.at(new_leader)->status().applied_index;
  }));
  // The split submission never existed; the others did, once each.
  const v1::ListResponse r = list();
  int split = 0;
  for (const auto& t : r.tasks()) split += t.job() == "split" ? 1 : 0;
  EXPECT_EQ(split, 0);
  // `before` kept running through it all, `after` started once, nobody fenced.
  ASSERT_TRUE(eventually([&] {
    return tasks_running(before, 1) && tasks_running(after) && agents_running() == 2;
  }));
  EXPECT_EQ(agents_started(), 2u);
  EXPECT_EQ(agents_fenced(), 0u);
}

TEST_F(ReplicaTest, ALaggingReplicaIsCaughtUpWithASnapshot) {
  start_cluster(3, /*snapshot_every=*/20);
  ASSERT_NE(leader(), 0u);
  stop_replica(3);
  // Enough entries that the others compact past everything replica 3 has.
  for (int i = 0; i < 30; ++i) {
    ASSERT_TRUE(eventually([&] {
      const uint32_t l = leader();
      return l && submit_at(l, "j" + std::to_string(i), {"true"}).ok();
    }));
  }
  uint32_t l = leader();
  ASSERT_TRUE(eventually([&] { return replicas_.at(l)->status().snapshot_index >= 20; }));
  start_replica(3);
  ASSERT_TRUE(eventually([&] {
    l = leader();
    const auto s = replicas_.at(3)->status();
    return l && s.snapshot_index >= 20 && s.applied_index == replicas_.at(l)->status().applied_index;
  }));
}

TEST_F(ReplicaTest, AOneReplicaGroupElectsItselfAndServes) {
  start_cluster(1);
  ASSERT_EQ(leader(), 1u);
  ASSERT_TRUE(submit_at(1, "solo", {"true"}).ok());
  stop_replica(1);
  start_replica(1);  // recovers its log and serves again
  ASSERT_EQ(leader(), 1u);
  const v1::ListResponse r = list();
  ASSERT_EQ(r.tasks_size(), 1);
  EXPECT_EQ(r.tasks(0).job(), "solo");
}

}  // namespace
}  // namespace fleet
