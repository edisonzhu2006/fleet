#pragma once
// MasterCore: the master's logic with no threads and no network (m2).
//
// WHY THIS EXISTS. fleetmaster is a gRPC server, but nothing it decides
// depends on gRPC: what to start or stop on a node given its heartbeat,
// whether a submission is admitted, what a drain evicts. Putting all of
// that in one single-threaded class that takes request messages and fills
// in replies means it can be unit-tested call by call (tests/
// master_core_test.cpp) with an explicit clock, and the server
// (master_server.h) is only locking and plumbing.
//
// WHAT IT ADDS TO ClusterState. ClusterState (core/) knows nodes, tasks and
// resources. The master also needs what only matters once real processes
// exist: node names and incarnations, the last heartbeat time, each task's
// command line, its attempt number and how it exited. That lives here, in
// vectors parallel to ClusterState's, indexed by the same dense ids.
//
// RECONCILIATION. heartbeat() compares what the master wants on a node
// (the tasks ClusterState has placed there, at their current attempt) with
// what the agent reports, and replies with the difference: start what is
// missing, stop what should not run, forget exits already recorded. The
// reply depends only on current state, never on what was sent before, so
// a lost reply is repaired by the next heartbeat. Plan: this is the
// reconciler m3 builds restarts and rescheduling on. When the report
// contains exits, heartbeat() runs a scheduling pass before building the
// reply, so work placed into the freed room starts in the same round trip.
//
// NOT THREAD-SAFE. MasterServer holds one mutex around every call.

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include <grpcpp/support/status.h>

#include "cluster_state.h"
#include "fleet.pb.h"
#include "scheduler.h"

namespace fleet {

using Clock = std::chrono::steady_clock;

struct MasterOptions {
  SchedulerOptions scheduler;
  // Told to agents at registration so the cadence is set in one place.
  uint32_t heartbeat_interval_ms = 500;
  // Upper bound on SubmitRequest.count, so one typo cannot create a
  // million tasks.
  uint32_t max_tasks_per_submit = 1000;
  // Print placements, exits, kills and drains to stderr (fleetmaster sets
  // this; tests leave it off).
  bool log_events = false;
};

class MasterCore {
 public:
  // seed feeds the incarnation generator; tests pass a fixed one.
  explicit MasterCore(MasterOptions options = {},
                      uint64_t seed = std::random_device{}());

  grpc::Status register_node(const v1::RegisterNodeRequest& req,
                             v1::RegisterNodeResponse* resp, Clock::time_point now);
  grpc::Status heartbeat(const v1::HeartbeatRequest& req,
                         v1::HeartbeatResponse* resp, Clock::time_point now);
  grpc::Status submit(const v1::SubmitRequest& req, v1::SubmitResponse* resp);
  grpc::Status list(const v1::ListRequest& req, v1::ListResponse* resp,
                    Clock::time_point now) const;
  grpc::Status kill(const v1::KillRequest& req, v1::KillResponse* resp);
  grpc::Status drain(const v1::DrainRequest& req, v1::DrainResponse* resp);

  // One scheduling pass. Every placement bumps the task's attempt, which is
  // what makes the next heartbeat from that node carry a start for it.
  ScheduleResult schedule();

  // True when something happened since the last schedule() that could let
  // a pending task fit (a submission, a kill, a drain, a new node).
  // MasterServer uses it to run a pass right away instead of waiting for
  // the next timer tick. Exits do not set it: heartbeat() schedules
  // inline when it applies one.
  bool needs_schedule() const { return needs_schedule_; }

  const ClusterState& state() const { return state_; }
  uint32_t attempt(TaskId id) const { return tasks_.at(id).attempt; }
  uint64_t incarnation(NodeId id) const { return nodes_.at(id).incarnation; }

  // ClusterState's invariants plus the master's own: metadata vectors are
  // parallel to ClusterState's, every node name maps back to its id, every
  // placed task has attempt >= 1.
  std::string check_invariants() const;

 private:
  struct NodeMeta {
    std::string name;
    uint64_t incarnation = 0;
    Clock::time_point last_heartbeat;
  };
  struct TaskMeta {
    std::vector<std::string> argv;
    std::string image;  // empty: the agent runtime's default
    uint32_t attempt = 0;  // 0 = never placed
    std::optional<NodeId> last_node;
    bool has_exit = false;
    int32_t exit_code = 0;
    int32_t signal = 0;
    bool oom_killed = false;
    std::string message;
  };

  // Is this reported instance the one the master currently wants on node?
  bool is_current(NodeId node, const v1::TaskInstance& inst) const;
  uint64_t new_incarnation();
  void log(const std::string& line) const;

  MasterOptions options_;
  ClusterState state_;
  Scheduler scheduler_;
  std::vector<NodeMeta> nodes_;  // parallel to state_.nodes()
  std::vector<TaskMeta> tasks_;  // parallel to state_.tasks()
  std::map<std::string, NodeId> node_by_name_;
  std::mt19937_64 rng_;
  bool needs_schedule_ = false;
};

}  // namespace fleet
