// ClusterState implementation: every legal state transition and the
// from-scratch invariant checker. See cluster_state.h for the contract.

#include "cluster_state.h"

#include <sstream>
#include <stdexcept>

namespace fleet {

std::string to_string(const Resources& r) {
  std::ostringstream os;
  os << r.milli_cpu << "m/" << r.memory_mb << "MB";
  return os.str();
}

std::string_view to_string(NodeState s) {
  switch (s) {
    case NodeState::kReady: return "ready";
    case NodeState::kDraining: return "draining";
    case NodeState::kDead: return "dead";
  }
  return "?";
}

std::string_view to_string(TaskState s) {
  switch (s) {
    case TaskState::kPending: return "pending";
    case TaskState::kRunning: return "running";
    case TaskState::kSucceeded: return "succeeded";
    case TaskState::kFailed: return "failed";
    case TaskState::kKilled: return "killed";
    case TaskState::kBackoff: return "backoff";
  }
  return "?";
}

NodeId ClusterState::add_node(Resources capacity, Labels labels) {
  // A bad capacity is a programming or config error, not a runtime
  // condition to recover from, so it throws rather than returning a code.
  if (capacity.milli_cpu <= 0 || capacity.memory_mb <= 0 ||
      capacity.milli_cpu > kMaxCapacity || capacity.memory_mb > kMaxCapacity) {
    throw std::invalid_argument("node capacity out of range: " +
                                to_string(capacity));
  }
  Node n;
  n.id = static_cast<NodeId>(nodes_.size());
  n.capacity = capacity;
  n.labels = std::move(labels);
  nodes_.push_back(std::move(n));
  on_node_.emplace_back();
  return nodes_.back().id;
}

void ClusterState::set_node_state(NodeId id, NodeState state) {
  Node& n = nodes_.at(id);
  n.state = state;
  if (state != NodeState::kDead) return;
  // Copy the set: requeue() erases from on_node_[id] while we iterate.
  // requeue(), not evict(): losing a node is not a preemption, so
  // preempted_count (a signal about priority churn) is not bumped.
  const std::set<TaskId> running = on_node_[id];
  for (TaskId t : running) requeue(t);
}

void ClusterState::set_node_suspect(NodeId id, bool suspect) {
  nodes_.at(id).suspect = suspect;
}

void ClusterState::set_quota(const std::string& job, Resources limit) {
  quotas_[job].limit = limit;
}

SubmitResult ClusterState::submit(const TaskSpec& spec) {
  SubmitResult r;
  if (spec.job.empty()) {
    r.reason = "job name is empty";
    return r;
  }
  if (spec.priority < 0 || spec.priority > kMaxPriority) {
    r.reason = "priority " + std::to_string(spec.priority) +
               " outside 0.." + std::to_string(kMaxPriority);
    return r;
  }
  // Both dimensions must be positive: a zero request would let unlimited
  // tasks pile onto one node, and the scorer divides by node capacity, not
  // by the request, so zero is legal arithmetically but meaningless.
  if (spec.request.milli_cpu <= 0 || spec.request.memory_mb <= 0 ||
      spec.request.milli_cpu > kMaxCapacity ||
      spec.request.memory_mb > kMaxCapacity) {
    r.reason = "request out of range: " + to_string(spec.request);
    return r;
  }
  JobQuota& q = quotas_[spec.job];
  if (q.limit && !(q.used + spec.request).fits_in(*q.limit)) {
    r.reason = "job " + spec.job + " over quota: used " + to_string(q.used) +
               " + request " + to_string(spec.request) + " > limit " +
               to_string(*q.limit);
    return r;
  }
  q.used += spec.request;

  Task t;
  t.id = static_cast<TaskId>(tasks_.size());
  t.job = spec.job;
  t.priority = spec.priority;
  t.request = spec.request;
  t.required_labels = spec.required_labels;
  tasks_.push_back(std::move(t));
  enqueue_pending(tasks_.back());
  r.ok = true;
  r.id = tasks_.back().id;
  return r;
}

bool ClusterState::labels_match(const Labels& required, const Labels& have) {
  for (const auto& [key, value] : required) {
    auto it = have.find(key);
    if (it == have.end() || it->second != value) return false;
  }
  return true;
}

bool ClusterState::feasible(const Task& task, const Node& node) const {
  return node.accepts_tasks() &&
         task.request.fits_in(node.free()) &&
         labels_match(task.required_labels, node.labels);
}

bool ClusterState::place(TaskId task_id, NodeId node_id) {
  if (task_id >= tasks_.size() || node_id >= nodes_.size()) return false;
  Task& t = tasks_[task_id];
  Node& n = nodes_[node_id];
  if (t.state != TaskState::kPending || !feasible(t, n)) return false;
  pending_.erase(PendingKey{-t.priority, t.id});
  t.state = TaskState::kRunning;
  t.node = node_id;
  n.allocated += t.request;
  on_node_[node_id].insert(t.id);
  return true;
}

bool ClusterState::evict(TaskId task_id) {
  if (task_id >= tasks_.size()) return false;
  Task& t = tasks_[task_id];
  if (t.state != TaskState::kRunning) return false;
  release_from_node(t);
  ++t.preempted_count;
  enqueue_pending(t);  // quota stays charged: the task is still alive
  return true;
}

bool ClusterState::requeue(TaskId task_id) {
  if (task_id >= tasks_.size()) return false;
  Task& t = tasks_[task_id];
  if (t.state != TaskState::kRunning) return false;
  release_from_node(t);
  enqueue_pending(t);  // quota stays charged: the task is still alive
  return true;
}

bool ClusterState::enter_backoff(TaskId task_id) {
  if (task_id >= tasks_.size()) return false;
  Task& t = tasks_[task_id];
  if (t.state != TaskState::kRunning) return false;
  release_from_node(t);
  t.state = TaskState::kBackoff;  // not queued: the master's timer requeues it
  return true;
}

bool ClusterState::leave_backoff(TaskId task_id) {
  if (task_id >= tasks_.size()) return false;
  Task& t = tasks_[task_id];
  if (t.state != TaskState::kBackoff) return false;
  enqueue_pending(t);
  return true;
}

bool ClusterState::finish(TaskId task_id, bool succeeded) {
  if (task_id >= tasks_.size()) return false;
  Task& t = tasks_[task_id];
  if (t.state != TaskState::kRunning) return false;
  release_from_node(t);
  t.state = succeeded ? TaskState::kSucceeded : TaskState::kFailed;
  release_quota(t);
  return true;
}

bool ClusterState::kill(TaskId task_id) {
  if (task_id >= tasks_.size()) return false;
  Task& t = tasks_[task_id];
  if (t.state == TaskState::kRunning) {
    release_from_node(t);
  } else if (t.state == TaskState::kPending) {
    pending_.erase(PendingKey{-t.priority, t.id});
  } else if (t.state != TaskState::kBackoff) {
    return false;  // already terminal
  }
  t.state = TaskState::kKilled;
  release_quota(t);
  return true;
}

void ClusterState::release_from_node(Task& t) {
  Node& n = nodes_[*t.node];
  n.allocated -= t.request;
  on_node_[n.id].erase(t.id);
  t.node.reset();
}

void ClusterState::enqueue_pending(Task& t) {
  t.state = TaskState::kPending;
  pending_.insert(PendingKey{-t.priority, t.id});
}

void ClusterState::release_quota(const Task& t) {
  quotas_[t.job].used -= t.request;
}

std::string ClusterState::check_invariants() const {
  std::ostringstream err;
  std::vector<Resources> alloc(nodes_.size());
  std::vector<size_t> count(nodes_.size(), 0);
  std::map<std::string, Resources> job_used;
  size_t pending_count = 0;

  for (const Task& t : tasks_) {
    const bool queued = pending_.count(PendingKey{-t.priority, t.id}) > 0;
    if (!is_terminal(t.state)) job_used[t.job] += t.request;
    if (t.state == TaskState::kRunning) {
      if (!t.node || *t.node >= nodes_.size()) {
        err << "task " << t.id << " running without a valid node\n";
        continue;
      }
      const Node& n = nodes_[*t.node];
      alloc[n.id] += t.request;
      ++count[n.id];
      if (n.state == NodeState::kDead) {
        err << "task " << t.id << " running on dead node " << n.id << "\n";
      }
      if (on_node_[n.id].count(t.id) == 0) {
        err << "task " << t.id << " missing from node " << n.id
            << " task set\n";
      }
      if (queued) err << "running task " << t.id << " is in pending queue\n";
    } else {
      if (t.node) err << "non-running task " << t.id << " has a node\n";
      if (t.state == TaskState::kPending) {
        ++pending_count;
        if (!queued) err << "pending task " << t.id << " not in queue\n";
      } else if (queued) {
        err << "terminal task " << t.id << " is in pending queue\n";
      }
    }
  }
  if (pending_count != pending_.size()) {
    err << "pending queue has " << pending_.size() << " entries, expected "
        << pending_count << "\n";
  }
  for (const Node& n : nodes_) {
    if (!n.allocated.fits_in(n.capacity)) {
      err << "node " << n.id << " overcommitted: " << to_string(n.allocated)
          << " > " << to_string(n.capacity) << "\n";
    }
    if (!(n.allocated == alloc[n.id])) {
      err << "node " << n.id << " allocated " << to_string(n.allocated)
          << " but placed requests sum to " << to_string(alloc[n.id]) << "\n";
    }
    if (on_node_[n.id].size() != count[n.id]) {
      err << "node " << n.id << " task set has " << on_node_[n.id].size()
          << " entries, expected " << count[n.id] << "\n";
    }
  }
  for (const auto& [job, q] : quotas_) {
    auto it = job_used.find(job);
    const Resources expect = it == job_used.end() ? Resources{} : it->second;
    if (!(q.used == expect)) {
      err << "job " << job << " quota used " << to_string(q.used)
          << " but live requests sum to " << to_string(expect) << "\n";
    }
  }
  return err.str();
}

}  // namespace fleet
