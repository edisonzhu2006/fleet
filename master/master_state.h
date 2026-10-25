#pragma once
// MasterState: everything the master must not forget, and the one
// deterministic function that changes it, apply(record) (m4).
//
// WHY THIS EXISTS. Before m4, MasterCore changed its state directly from
// wherever a decision was made. To survive a crash, every one of those
// changes has to be written to disk first and replayed after, and replay
// must rebuild exactly the state the master had. The way to make that true
// by construction rather than by care is to have exactly one code path
// that changes durable state: MasterCore decides, describes the decision
// as a v1::WalRecord (proto/wal.proto), and calls apply(record) on this
// class; recovery reads the same records from disk and calls the same
// apply(). There is no second "replay version" of any transition to drift
// out of sync with the live one.
//
// DETERMINISM. apply() reads nothing but this state and the record: no
// clock, no scheduler, no failure detector, no randomness, no iteration
// over unordered containers. Anything that depends on time or policy (when
// a backoff ends, which node a task goes to, which epoch to issue) was
// decided by MasterCore before the record was made and is carried in it.
// So the same log applied to the same starting state always yields the
// same state, which is the property recovery needs and the property m4b's
// Raft followers need (they apply the leader's records). The randomized
// test in tests/wal_test.cpp checks it: replaying a live master's log gives
// a state whose snapshot equals the live one, after every operation.
//
// DURABLE VS SOFT STATE. Durable: nodes (name, shape, state, epoch, drain
// flag), tasks (spec, state, placement, attempt, restart counters, backoff
// deadline, last exit), the epoch counter, the LSN, and (m4b) the request
// ids of admitted submissions, so a retried submission is answered rather
// than admitted twice, on every replica and after a restart. Soft, never
// logged:
// heartbeat times and suspect flags (the FailureDetector, owned by
// MasterCore). Soft state describes the last few seconds; after a restart
// the master relearns it from heartbeats, and logging it would mean a
// record per heartbeat.
//
// ERRORS. apply() returns "" on success, or why the record does not fit
// the state (wrong LSN, unknown task, a transition ClusterState refuses).
// That only happens with a corrupt or misordered log, or a bug; recovery
// then refuses to start and the live path aborts. A refused record may
// have changed part of the state, so the state must be discarded.
//
// NOT THREAD-SAFE. MasterCore owns one instance under the master's lock.

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "cluster_state.h"
#include "fleet.pb.h"
#include "restart_policy.h"
#include "wal.pb.h"

namespace fleet {

// The master's per-node data beyond ClusterState's Node, indexed by NodeId.
struct NodeMeta {
  std::string name;
  uint64_t epoch = 0;  // 0 = no valid registration (declared dead)
  // An operator drained it. Kept apart from NodeState because death
  // overwrites the state with kDead; when the node registers again it
  // must come back draining, not ready.
  bool drain_requested = false;
};

// The master's per-task data beyond ClusterState's Task, indexed by TaskId.
struct TaskMeta {
  std::vector<std::string> argv;
  std::string image;  // empty: the agent runtime's default
  // Stateful task (m5): its checkpoint directory on shared storage and the
  // command that continues from a checkpoint there. Empty dir: stateless.
  std::string checkpoint_dir;
  std::vector<std::string> resume_argv;
  RestartPolicy policy = RestartPolicy::kNever;
  uint32_t attempt = 0;  // 0 = never placed
  std::optional<NodeId> last_node;
  int64_t placed_at_ms = 0;  // wall clock, of the current attempt
  // The master has told this attempt to stop so it can be moved off a
  // draining node. Its exit is then a move, not the task's own exit.
  bool moving = false;
  uint32_t restarts = 0;  // total reruns by the restart policy
  // Restarts in a row without an attempt that stayed up reset_after;
  // drives the backoff delay.
  uint32_t consecutive_restarts = 0;
  int64_t restart_at_ms = 0;  // wall clock; valid while in backoff
  bool has_exit = false;
  int32_t exit_code = 0;
  int32_t signal = 0;
  bool oom_killed = false;
  std::string message;
};

class MasterState {
 public:
  // Applies one record whose lsn must be lsn() + 1. See the class comment.
  std::string apply(const v1::WalRecord& record);

  // The LSN of the last record applied (0: none yet).
  uint64_t lsn() const { return lsn_; }
  // Every epoch issued so far is below this. MasterCore issues
  // max(next_epoch(), its floor); NodeRegistered moves it past the epoch.
  uint64_t next_epoch() const { return next_epoch_; }

  const ClusterState& cluster() const { return cluster_; }
  const NodeMeta& node(NodeId id) const { return nodes_.at(id); }
  const TaskMeta& task(TaskId id) const { return tasks_.at(id); }
  size_t node_count() const { return nodes_.size(); }
  std::optional<NodeId> find_node(const std::string& name) const;
  // A submission admitted with this request id (m4b): its first task id
  // and count. Kept for the life of the state, like the tasks themselves.
  struct Submission {
    uint32_t first_task_id = 0;
    uint32_t count = 0;
  };
  std::optional<Submission> find_submission(const std::string& request_id) const;
  // Tasks in backoff, ordered by when they become pending again (wall ms),
  // so the tick pops only expired ones: O(log n) each, not a scan.
  const std::set<std::pair<int64_t, TaskId>>& backoff() const { return backoff_; }

  // Soft state (not logged, not in snapshots): the failure detector's
  // suspicion, which only affects where new tasks may go. See the class
  // comment for why it is not durable.
  void set_suspect(NodeId id, bool suspect) { cluster_.set_node_suspect(id, suspect); }

  // The durable state as a snapshot (proto/wal.proto), and back. Derived
  // structures are rebuilt on load and verified with check_invariants();
  // a snapshot that does not rebuild cleanly is refused with *error set.
  void to_snapshot(v1::Snapshot* out) const;
  static std::optional<MasterState> from_snapshot(const v1::Snapshot& snap,
                                                  std::string* error);

  // ClusterState's invariants plus the metadata ones that need no soft
  // state: metadata vectors parallel to ClusterState's, names map to ids,
  // running tasks have an attempt and last_node, "moving" only on tasks
  // running on a draining node, the backoff set matches the tasks in
  // backoff, dead iff epoch 0, live epochs distinct and below next_epoch.
  std::string check_invariants() const;

 private:
  std::string apply_node_registered(const v1::NodeRegistered& r);
  std::string apply_node_died(const v1::NodeDied& r);
  std::string apply_node_draining(const v1::NodeDraining& r);
  std::string apply_tasks_submitted(const v1::TasksSubmitted& r);
  std::string apply_task_placed(const v1::TaskPlaced& r);
  std::string apply_task_preempted(const v1::TaskPreempted& r);
  std::string apply_task_requeued(const v1::TaskRequeued& r);
  std::string apply_task_moving(const v1::TaskMoving& r);
  std::string apply_task_exited(const v1::TaskExited& r);
  std::string apply_task_backoff_over(const v1::TaskBackoffOver& r);
  std::string apply_task_killed(const v1::TaskKilled& r);
  bool has_task(uint32_t id) const { return id < tasks_.size(); }

  ClusterState cluster_;
  std::vector<NodeMeta> nodes_;  // parallel to cluster_.nodes()
  std::vector<TaskMeta> tasks_;  // parallel to cluster_.tasks()
  std::map<std::string, NodeId> node_by_name_;
  std::map<std::string, Submission> submissions_;  // by request id
  std::set<std::pair<int64_t, TaskId>> backoff_;
  uint64_t next_epoch_ = 1;  // 0 is never issued: it means "revoked"
  uint64_t lsn_ = 0;
};

}  // namespace fleet
