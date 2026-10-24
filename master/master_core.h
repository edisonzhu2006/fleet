#pragma once
// MasterCore: the master's logic with no threads, no network and no disk
// (m2, m3; m4 made every state change a log record).
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
// exist: node names and epochs, each task's command line, attempt number,
// restart policy, backoff timer and how it last exited (all durable, in
// MasterState, m4), and heartbeat health (soft, the FailureDetector here).
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
// DECIDE, LOG, APPLY (m4). MasterCore never changes durable state
// directly. Each decision becomes a v1::WalRecord (proto/wal.proto), which
// emit() applies to MasterState through the same apply() that recovery
// replays, and queues; MasterServer takes the queue (take_records()) after
// every call and makes it durable before the call's reply leaves the
// process. MasterCore itself stays free of I/O, so tests drive it call by
// call and replay its records into a fresh MasterState to check that
// replay reproduces the live state exactly.
//
// RECOVERY (m4). Constructed from a recovered MasterState, the core starts
// in a report window: it accepts heartbeats and applies what they report,
// but runs no scheduling pass until every node that is not dead has
// reported once or `recovery_window` has passed, whichever comes first
// (report before schedule). A node that has not reported since the
// restart counts as suspect, so even after the window no new work goes to
// it. A node's first report is reconciled against the log: a task the
// log has running there that the node does not report never got its
// start (the reply carrying it was lost in the crash) and goes back to
// pending; an instance the node runs that the log does not know is told
// to stop, as any stale instance is.
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
#include "master_state.h"
#include "restart_policy.h"
#include "scheduler.h"
#include "wal.pb.h"

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
  // After a restart, the longest the master waits for agents to report
  // before its first scheduling pass (m4). Ends early once every node that
  // is not dead has reported. Two heartbeat intervals by default: an
  // agent whose channel is in gRPC's reconnect backoff (capped at 1 s,
  // agent.cpp) needs about that long to get its first beat through.
  std::chrono::milliseconds recovery_window{1000};
  // Wall clock = steady clock + this offset, in ms. Durable times (backoff
  // deadlines, placement times) are wall-clock ms (see proto/wal.proto);
  // the master reads only the steady clock, so it converts with one offset
  // fixed at construction. nullopt: measure it; tests pass 0 so the fake
  // steady clock is also the fake wall clock.
  std::optional<int64_t> wall_offset_ms;
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
  // A master with no history. epoch_floor: no epoch below it is issued.
  // The default is the wall clock in microseconds, which keeps epochs
  // above those of any earlier master even if its state directory was
  // lost; tests pass a small constant. With a log (m4) the epoch counter
  // is durable, so the floor is only that backstop.
  explicit MasterCore(MasterOptions options = {},
                      uint64_t epoch_floor = default_first_epoch());
  // A master restarted from durable state (Storage::open). It starts in
  // the report window (class comment); `now` is when the window starts.
  MasterCore(MasterOptions options, MasterState recovered, Clock::time_point now,
             uint64_t epoch_floor = default_first_epoch());

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

  // The records made since the last call, in order (m4). Whoever serves
  // replies must make these durable before sending any reply computed
  // after they were made; MasterServer does, under its lock.
  std::vector<v1::WalRecord> take_records();
  // In the report window after a restart: no scheduling yet.
  bool recovering() const { return recovering_; }

  const ClusterState& state() const { return state_.cluster(); }
  const MasterState& durable_state() const { return state_; }
  const FailureDetector& detector() const { return detector_; }
  uint32_t attempt(TaskId id) const { return state_.task(id).attempt; }
  uint32_t restarts(TaskId id) const { return state_.task(id).restarts; }
  // When a task in backoff becomes pending again.
  Clock::time_point restart_at(TaskId id) const {
    return from_wall_ms(state_.task(id).restart_at_ms);
  }
  // A node's current epoch; 0 once it has been declared dead.
  uint64_t epoch(NodeId id) const { return state_.node(id).epoch; }

  // ClusterState's invariants plus the master's own; see the definition
  // for the list.
  std::string check_invariants() const;

  static uint64_t default_first_epoch();

 private:
  class LoggingActuator;

  // Gives the record the next LSN, applies it to state_ and queues it for
  // take_records(). A record its own state refuses is a MasterCore bug:
  // throws std::logic_error rather than run on a state the log cannot
  // reproduce.
  void emit(v1::WalRecord record);
  uint64_t issue_epoch() const;
  int64_t wall_ms(Clock::time_point t) const;
  Clock::time_point from_wall_ms(int64_t ms) const;

  // Is this reported instance the one the master currently wants on node?
  bool is_current(NodeId node, const v1::TaskInstance& inst) const;
  // A current instance exited: decide whether it moves (drain), restarts
  // (policy) or finishes, and log that.
  void apply_exit(NodeId node, const v1::TaskStatus& st, Clock::time_point now);
  // The node half of the reconciler, after the report has been applied.
  void reconcile_node(NodeId node, const std::set<TaskId>& reported,
                      v1::HeartbeatResponse* resp);
  // First report from a node since a restart: requeue what it should run
  // but does not report (see the class comment).
  void reconcile_after_restart(NodeId node, const std::set<TaskId>& reported);
  void finish_recovery(Clock::time_point now, const char* why);
  void declare_dead(NodeId node, const HealthChange& change);
  void log(const std::string& line) const;

  MasterOptions options_;
  MasterState state_;  // durable: changed only by emit()
  Scheduler scheduler_;
  // Soft state: rebuilt from heartbeats after a restart, never logged.
  FailureDetector detector_;
  uint64_t epoch_floor_;
  int64_t wall_offset_ms_;
  std::vector<v1::WalRecord> records_;  // made, not yet taken
  bool needs_schedule_ = false;
  // Recovery (m4): the report window, and which nodes have not reported
  // since the restart.
  bool recovering_ = false;
  Clock::time_point recovery_started_{};
  Clock::time_point recovery_deadline_{};
  std::vector<bool> awaiting_report_;  // indexed by NodeId
  size_t awaiting_count_ = 0;
};

}  // namespace fleet
