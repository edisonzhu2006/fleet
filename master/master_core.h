#pragma once
// MasterCore: the master's logic with no threads and no network (m2, m3).
//
// WHY THIS EXISTS. fleetmaster is a gRPC server, but nothing it decides
// depends on gRPC: what to start or stop on a node given its heartbeat,
// whether a submission is admitted, when a silent node is dead, when a
// crashed task runs again. Putting all of that in one single-threaded
// class that takes request messages and an explicit `now` means it can be
// unit-tested call by call with a fake clock (tests/master_core_test.cpp,
// tests/failover_test.cpp), and the server (master_server.h) is only
// locking, plumbing and a timer.
//
// WHAT IT ADDS TO ClusterState. ClusterState (core/) knows nodes, tasks and
// resources. The master also needs what only matters once real processes
// exist: node names and epochs, heartbeat health (FailureDetector), each
// task's command line, attempt number, restart policy, backoff timer and
// how it last exited. That lives here, in vectors parallel to
// ClusterState's, indexed by the same dense ids.
//
// THE RECONCILER (m3). Desired state is ClusterState: which tasks should
// run, and where. Observed state comes from two places: what agents report
// in heartbeats, and silence (the failure detector). The reconciler is the
// one loop that converges the two, in two halves:
//   - tick(now), on a timer (100 ms by default): the cluster half. Turns
//     silence into decisions (suspect: no new tasks there; dead: revoke the
//     node's epoch and send its tasks back to pending), turns finished
//     backoff timers into pending tasks, and runs a scheduling pass that
//     gives every pending task a node.
//   - heartbeat(), per node: the node half. Applies the node's report
//     (exits go through the restart policy) and replies with the diff
//     between desired and reported: start what should run here and is not
//     reported, stop what is reported and should not run (killed,
//     preempted, stale epoch, or being moved off a draining node), forget
//     exits already recorded.
// Restarts, rescheduling after node death, and drain are therefore not
// separate mechanisms: each one only changes desired state, and the same
// diff carries it out. The reply depends only on current state, never on
// what was sent before, so a lost reply is repaired by the next heartbeat.
//
// FENCING (m3). Every registration gets an epoch from one counter that
// only goes up. Instances are tagged with the epoch they started under;
// anything reported under another epoch is stopped, a heartbeat with an
// old epoch is refused (FAILED_PRECONDITION, and the agent kills all its
// tasks and registers again), and a node declared dead has its epoch
// revoked (set to 0, which is never issued). NOTES.md m3 says what this
// prevents and what it cannot.
//
// NOT THREAD-SAFE. MasterServer holds one mutex around every call.

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/support/status.h>

#include "cluster_state.h"
#include "failure_detector.h"
#include "fleet.pb.h"
#include "restart_policy.h"
#include "scheduler.h"

namespace fleet {

struct MasterOptions {
  SchedulerOptions scheduler;
  // Told to agents at registration so the cadence is set in one place; the
  // failure detector counts missed beats against the same value.
  uint32_t heartbeat_interval_ms = 500;
  // Failure detector thresholds, in missed heartbeats (m3). Defaults:
  // suspect after 1.5 s of silence, dead after 3 s.
  uint32_t suspect_after_missed = 3;
  uint32_t dead_after_missed = 6;
  BackoffOptions backoff;
  // Upper bound on SubmitRequest.count, so one typo cannot create a
  // million tasks.
  uint32_t max_tasks_per_submit = 1000;
  // Print placements, exits, kills, drains, deaths to stderr (fleetmaster
  // sets this; tests leave it off).
  bool log_events = false;
};

// What one tick() did, for tests and logging.
struct TickResult {
  std::vector<NodeId> suspected;  // alive -> suspect this tick
  std::vector<NodeId> died;       // declared dead this tick
  std::vector<TaskId> restarted;  // backoff over: back to pending
  ScheduleResult schedule;
};

class MasterCore {
 public:
  // first_epoch: the first epoch this master hands out. The default is
  // the wall clock in microseconds, so a restarted master (which has no
  // persistent state until m4) issues epochs above everything it issued
  // before; tests pass a small constant.
  explicit MasterCore(MasterOptions options = {},
                      uint64_t first_epoch = default_first_epoch());

  grpc::Status register_node(const v1::RegisterNodeRequest& req,
                             v1::RegisterNodeResponse* resp, Clock::time_point now);
  grpc::Status heartbeat(const v1::HeartbeatRequest& req,
                         v1::HeartbeatResponse* resp, Clock::time_point now);
  grpc::Status submit(const v1::SubmitRequest& req, v1::SubmitResponse* resp);
  grpc::Status list(const v1::ListRequest& req, v1::ListResponse* resp,
                    Clock::time_point now) const;
  grpc::Status kill(const v1::KillRequest& req, v1::KillResponse* resp);
  grpc::Status drain(const v1::DrainRequest& req, v1::DrainResponse* resp);

  // The cluster half of the reconciler: failure detection, backoff timers,
  // then one scheduling pass. MasterServer calls it every tick interval and
  // whenever needs_schedule() is set.
  TickResult tick(Clock::time_point now);

  // One scheduling pass. Every placement bumps the task's attempt, which is
  // what makes the next heartbeat from that node carry a start for it.
  ScheduleResult schedule(Clock::time_point now);

  // True when something happened since the last pass that could let a
  // pending task fit or that created pending work (a submission, a kill, a
  // node registering or recovering, a task moved off a draining node).
  // MasterServer uses it to tick right away instead of waiting for the
  // timer. Exits do not set it: heartbeat() schedules inline.
  bool needs_schedule() const { return needs_schedule_; }

  const ClusterState& state() const { return state_; }
  const FailureDetector& detector() const { return detector_; }
  uint32_t attempt(TaskId id) const { return tasks_.at(id).attempt; }
  uint32_t restarts(TaskId id) const { return tasks_.at(id).restarts; }
  // When a task in backoff becomes pending again.
  Clock::time_point restart_at(TaskId id) const { return tasks_.at(id).restart_at; }
  // A node's current epoch; 0 once it has been declared dead.
  uint64_t epoch(NodeId id) const { return nodes_.at(id).epoch; }

  // ClusterState's invariants plus the master's own; see the definition
  // for the list.
  std::string check_invariants() const;

  static uint64_t default_first_epoch();

 private:
  struct NodeMeta {
    std::string name;
    uint64_t epoch = 0;  // 0 = no valid registration (declared dead)
    // An operator drained it. Kept apart from NodeState because death
    // overwrites the state with kDead; when the node registers again it
    // must come back draining, not ready.
    bool drain_requested = false;
  };
  struct TaskMeta {
    std::vector<std::string> argv;
    std::string image;  // empty: the agent runtime's default
    RestartPolicy policy = RestartPolicy::kNever;
    uint32_t attempt = 0;  // 0 = never placed
    std::optional<NodeId> last_node;
    Clock::time_point placed_at{};  // of the current attempt
    // The master has told this attempt to stop so it can be moved off a
    // draining node. Its exit is then a move, not the task's own exit.
    bool moving = false;
    uint32_t restarts = 0;  // total reruns by the restart policy
    // Restarts in a row without an attempt that stayed up reset_after;
    // drives the backoff delay.
    uint32_t consecutive_restarts = 0;
    Clock::time_point restart_at{};  // valid while in backoff
    bool has_exit = false;
    int32_t exit_code = 0;
    int32_t signal = 0;
    bool oom_killed = false;
    std::string message;
  };

  // Is this reported instance the one the master currently wants on node?
  bool is_current(NodeId node, const v1::TaskInstance& inst) const;
  // A current instance exited: record it, then move it (drain), restart it
  // (policy) or finish it.
  void apply_exit(NodeId node, const v1::TaskStatus& st, Clock::time_point now);
  // The node half of the reconciler, after the report has been applied.
  void reconcile_node(NodeId node, const std::set<TaskId>& reported,
                      v1::HeartbeatResponse* resp);
  void declare_dead(NodeId node, const HealthChange& change);
  void log(const std::string& line) const;

  MasterOptions options_;
  ClusterState state_;
  Scheduler scheduler_;
  FailureDetector detector_;
  std::vector<NodeMeta> nodes_;  // parallel to state_.nodes()
  std::vector<TaskMeta> tasks_;  // parallel to state_.tasks()
  std::map<std::string, NodeId> node_by_name_;
  // Tasks in backoff, ordered by when they become pending again, so a tick
  // pops only the expired ones: O(log n) each instead of scanning every
  // task. Entries are erased when the task leaves backoff by any path.
  std::set<std::pair<Clock::time_point, TaskId>> backoff_;
  uint64_t next_epoch_;
  bool needs_schedule_ = false;
};

}  // namespace fleet
