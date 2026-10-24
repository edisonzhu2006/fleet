#pragma once
// Helpers shared by the MasterCore tests (master_core_test.cpp, m2, and
// failover_test.cpp, m3): register a node, submit tasks, and a FakeAgent
// that builds heartbeats the way a real agent would. Every call takes an
// explicit time (default: the fake clock's origin), so tests that care
// about failure detection and backoff step time by hand.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "master_core.h"

#define EXPECT_CORE_OK(core)                                   \
  do {                                                         \
    const std::string fleet_err = (core).check_invariants();   \
    EXPECT_TRUE(fleet_err.empty()) << fleet_err;               \
  } while (0)

namespace fleet::testing {

inline const Clock::time_point kT0{};  // the fake clock's origin

inline v1::RegisterNodeResponse register_node(MasterCore& core, const std::string& name,
                                              int64_t mcpu = 4000, int64_t mem = 4096,
                                              Clock::time_point now = kT0) {
  v1::RegisterNodeRequest req;
  req.set_name(name);
  req.mutable_capacity()->set_milli_cpu(mcpu);
  req.mutable_capacity()->set_memory_mb(mem);
  v1::RegisterNodeResponse resp;
  grpc::Status s = core.register_node(req, &resp, now);
  EXPECT_TRUE(s.ok()) << s.error_message();
  return resp;
}

inline std::vector<uint32_t> submit(
    MasterCore& core, const std::string& job, int prio, int64_t mcpu, int64_t mem,
    uint32_t count = 1, v1::RestartPolicy policy = v1::RESTART_POLICY_UNSPECIFIED) {
  v1::SubmitRequest req;
  req.set_job(job);
  req.set_priority(prio);
  req.mutable_request()->set_milli_cpu(mcpu);
  req.mutable_request()->set_memory_mb(mem);
  req.add_argv("true");
  req.set_count(count);
  req.set_restart_policy(policy);
  v1::SubmitResponse resp;
  grpc::Status s = core.submit(req, &resp);
  EXPECT_TRUE(s.ok()) << s.error_message();
  return {resp.task_ids().begin(), resp.task_ids().end()};
}

// Builds heartbeats the way an agent would: add what it runs and what
// exited, then beat().
struct FakeAgent {
  uint32_t node_id;
  uint64_t epoch;
  v1::HeartbeatRequest req;

  void running(uint32_t task, uint32_t attempt, uint64_t epoch_tag) {
    auto* st = req.add_tasks();
    st->mutable_instance()->set_task_id(task);
    st->mutable_instance()->set_attempt(attempt);
    st->mutable_instance()->set_epoch(epoch_tag);
    st->set_phase(v1::TaskStatus::PHASE_RUNNING);
  }
  void exited(uint32_t task, uint32_t attempt, uint64_t epoch_tag, int code) {
    auto* st = req.add_tasks();
    st->mutable_instance()->set_task_id(task);
    st->mutable_instance()->set_attempt(attempt);
    st->mutable_instance()->set_epoch(epoch_tag);
    st->set_phase(v1::TaskStatus::PHASE_EXITED);
    st->set_exit_code(code);
  }
  // Sends the heartbeat and expects it to be accepted.
  v1::HeartbeatResponse beat(MasterCore& core, Clock::time_point now = kT0) {
    v1::HeartbeatResponse resp;
    grpc::Status s = try_beat(core, &resp, now);
    EXPECT_TRUE(s.ok()) << s.error_message();
    return resp;
  }
  // Sends the heartbeat and returns the status, for tests of refusals.
  grpc::Status try_beat(MasterCore& core, v1::HeartbeatResponse* resp,
                        Clock::time_point now = kT0) {
    req.set_node_id(node_id);
    req.set_epoch(epoch);
    grpc::Status s = core.heartbeat(req, resp, now);
    req.clear_tasks();
    return s;
  }
};

inline FakeAgent agent_for(const v1::RegisterNodeResponse& r) {
  return FakeAgent{r.node_id(), r.epoch(), {}};
}

}  // namespace fleet::testing
