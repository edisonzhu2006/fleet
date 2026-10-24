#pragma once
// ClusterState: the single owner of nodes, tasks and quotas (milestone 1).
//
// WHY THIS EXISTS. Every state change in fleet (a task is placed, evicted,
// finishes; a node drains or dies) has bookkeeping attached: node.allocated,
// the pending queue, the node's task set, the job's quota usage. If any
// caller could poke Task::state directly, one forgotten line would make the
// numbers drift. So all mutation goes through the methods below, each of
// which updates every derived structure in one place, and
// check_invariants() recomputes everything from scratch to prove it.
//
// WHAT IT DOES NOT DO. It never decides where a task goes; that is the
// Scheduler. ClusterState only checks that a decision is legal (the node is
// ready, the labels match, the task fits) and applies it. Keeping policy out
// of the state class is what lets three placement strategies share it.
//
// SINGLE-THREADED by design. The master owns one instance and serializes
// access to it with one mutex (master/master_server.h).

#include <compare>
#include <cstdint>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "model.h"

namespace fleet {

struct SubmitResult {
  bool ok = false;
  TaskId id = 0;       // valid only when ok
  std::string reason;  // why it was rejected, when !ok
};

// Order of the pending queue: higher priority first, then older (lower id)
// first. A std::set of (-priority, id) gives that order directly and makes
// "insert", "erase" and "visit in order" all O(log n).
struct PendingKey {
  int neg_priority;
  TaskId id;
  friend auto operator<=>(const PendingKey&, const PendingKey&) = default;
};

class ClusterState {
 public:
  // Nodes. Capacity must be positive and at most kMaxCapacity per dimension
  // (see the comment on kMaxCapacity for why there is a ceiling at all).
  NodeId add_node(Resources capacity, Labels labels = {});

  // Dead returns the node's running tasks to pending at once: the machine
  // is gone, so nothing is left to stop. Draining (m3) leaves them where
  // they are: the node takes no new tasks, and the master moves each one
  // with requeue() only once its agent reports the process gone, so two
  // copies never run at the same time. (m1 drained immediately, which is
  // what let a drained task's replacement start while the original was
  // still in its stop grace period.)
  void set_node_state(NodeId id, NodeState state);
  // Suspect (m3): see Node::suspect. Changes nothing but placement.
  void set_node_suspect(NodeId id, bool suspect);

  // Quotas. Setting a limit below current usage is allowed (existing tasks
  // are not killed, as in Borg); it only blocks new submissions.
  void set_quota(const std::string& job, Resources limit);

  // Admission control. Validates the spec and charges the job's quota; on
  // success the task is pending. Rejections carry a human-readable reason.
  SubmitResult submit(const TaskSpec& spec);

  // Transitions used by the scheduler and, later, the master. Each returns
  // false (and changes nothing) if the transition is illegal, so a buggy
  // caller fails loudly in tests instead of corrupting state.
  bool place(TaskId task, NodeId node);  // pending -> running
  bool evict(TaskId task);               // running -> pending (preemption)
  // running -> pending for reasons that are not priority churn: its node
  // was lost, or it was moved off a draining node (m3). Unlike evict(),
  // does not count as a preemption.
  bool requeue(TaskId task);
  bool finish(TaskId task, bool succeeded);  // running -> succeeded/failed
  // The restart policy path (m3): running -> backoff when the task exited
  // and will be rerun, then backoff -> pending when its delay is over.
  // Quota stays charged throughout: the task is alive, only waiting.
  bool enter_backoff(TaskId task);
  bool leave_backoff(TaskId task);
  bool kill(TaskId task);  // pending|running|backoff -> killed

  // True when the node accepts tasks (ready, not suspect), carries every
  // label the task requires, and has room for its request right now.
  bool feasible(const Task& task, const Node& node) const;
  static bool labels_match(const Labels& required, const Labels& have);

  const Node& node(NodeId id) const { return nodes_.at(id); }
  const Task& task(TaskId id) const { return tasks_.at(id); }
  const std::vector<Node>& nodes() const { return nodes_; }
  const std::vector<Task>& tasks() const { return tasks_; }
  const std::set<PendingKey>& pending() const { return pending_; }
  // Running tasks on a node, in id order (the preemption search relies on
  // a stable order to be deterministic).
  const std::set<TaskId>& tasks_on(NodeId id) const { return on_node_.at(id); }
  const std::map<std::string, JobQuota>& quotas() const { return quotas_; }

  // Recomputes every derived quantity from the task list and compares.
  // Returns an empty string when everything holds, otherwise one line per
  // violation. Checks:
  //   1. no node's allocated exceeds its capacity (no overcommit)
  //   2. every running task is on a node that is not dead (ready or
  //      draining, suspect or not) and listed in that node's task set;
  //      every pending task is in the pending queue; backoff and terminal
  //      tasks are in neither and have no node
  //   3. each node's allocated equals the sum of requests placed on it
  //   4. each job's quota usage equals the sum of its live requests
  // O(nodes + tasks): cheap enough to run after every operation in tests.
  std::string check_invariants() const;

  // Upper bound on a node's capacity per dimension: 2^31 milli-CPU (two
  // million cores) and 2^31 MB (2 PB). The scheduler compares fractions by
  // cross-multiplying (a/b < c/d as a*d < c*b) to keep scores exact; with
  // both factors below 2^31 the product stays below 2^62 and cannot
  // overflow int64.
  static constexpr int64_t kMaxCapacity = int64_t{1} << 31;

 private:
  void release_from_node(Task& t);  // running -> not on any node
  void enqueue_pending(Task& t);
  void release_quota(const Task& t);

  std::vector<Node> nodes_;
  std::vector<std::set<TaskId>> on_node_;  // parallel to nodes_
  // Tasks are never erased in m1: terminal tasks stay as history, so a
  // TaskId stays valid forever. Compaction (dropping old terminal tasks) is
  // a later concern once the master runs for days.
  std::vector<Task> tasks_;
  std::set<PendingKey> pending_;
  std::map<std::string, JobQuota> quotas_;
};

}  // namespace fleet
