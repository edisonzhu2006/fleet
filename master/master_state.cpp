// MasterState implementation: apply() for each record type, snapshots in
// and out, and the durable-state invariants. See master_state.h.

#include "master_state.h"

#include <sstream>
#include <stdexcept>

#include "proto_convert.h"

namespace fleet {

std::optional<NodeId> MasterState::find_node(const std::string& name) const {
  auto it = node_by_name_.find(name);
  if (it == node_by_name_.end()) return std::nullopt;
  return it->second;
}

std::optional<MasterState::Submission> MasterState::find_submission(
    const std::string& request_id) const {
  auto it = submissions_.find(request_id);
  if (it == submissions_.end()) return std::nullopt;
  return it->second;
}

std::string MasterState::apply(const v1::WalRecord& r) {
  // The LSN check is what turns "the snapshot and the log do not belong
  // together" or "a record was lost in the middle" into an error instead
  // of a silently different state.
  if (r.lsn() != lsn_ + 1) {
    return "record lsn " + std::to_string(r.lsn()) + " does not follow " + std::to_string(lsn_);
  }
  std::string err;
  switch (r.op_case()) {
    case v1::WalRecord::kNodeRegistered: err = apply_node_registered(r.node_registered()); break;
    case v1::WalRecord::kNodeDied: err = apply_node_died(r.node_died()); break;
    case v1::WalRecord::kNodeDraining: err = apply_node_draining(r.node_draining()); break;
    case v1::WalRecord::kTasksSubmitted: err = apply_tasks_submitted(r.tasks_submitted()); break;
    case v1::WalRecord::kTaskPlaced: err = apply_task_placed(r.task_placed()); break;
    case v1::WalRecord::kTaskPreempted: err = apply_task_preempted(r.task_preempted()); break;
    case v1::WalRecord::kTaskRequeued: err = apply_task_requeued(r.task_requeued()); break;
    case v1::WalRecord::kTaskMoving: err = apply_task_moving(r.task_moving()); break;
    case v1::WalRecord::kTaskExited: err = apply_task_exited(r.task_exited()); break;
    case v1::WalRecord::kTaskBackoffOver: err = apply_task_backoff_over(r.task_backoff_over()); break;
    case v1::WalRecord::kTaskKilled: err = apply_task_killed(r.task_killed()); break;
    case v1::WalRecord::OP_NOT_SET:
      // Also what a record from a newer master with an op this build does
      // not know looks like. Skipping it would silently diverge, so no.
      err = "record has no op this build understands";
      break;
  }
  if (!err.empty()) return "lsn " + std::to_string(r.lsn()) + ": " + err;
  lsn_ = r.lsn();
  return {};
}

std::string MasterState::apply_node_registered(const v1::NodeRegistered& r) {
  // Epochs strictly increase across every registration ever logged, which
  // is what makes them fencing tokens (NOTES.md m3) and what lets a
  // restarted master keep issuing larger ones without trusting the clock.
  if (r.epoch() == 0 || r.epoch() < next_epoch_) {
    return "epoch " + std::to_string(r.epoch()) + " is not above " +
           std::to_string(next_epoch_ - 1);
  }
  const Resources cap = from_proto(r.capacity());
  const Labels labels = from_proto(r.labels());
  if (r.node_id() == nodes_.size()) {
    if (node_by_name_.count(r.name())) return "node " + r.name() + " already exists";
    try {
      cluster_.add_node(cap, labels);
    } catch (const std::invalid_argument& e) {
      return e.what();
    }
    nodes_.push_back(NodeMeta{r.name(), 0, false});
    node_by_name_.emplace(r.name(), r.node_id());
  } else {
    if (r.node_id() > nodes_.size() || nodes_[r.node_id()].name != r.name()) {
      return "node id " + std::to_string(r.node_id()) + " is not node " + r.name();
    }
    const Node& n = cluster_.node(r.node_id());
    if (!(n.capacity == cap) || n.labels != labels) {
      return "node " + r.name() + " re-registered with a different shape";
    }
    // A new registration means the agent process the master knew, and
    // whatever it ran, is gone or fenced: its tasks go back to pending and
    // restart at new attempts. A dead node comes back ready, or draining
    // if an operator drained it before it died.
    const std::set<TaskId> on_node = cluster_.tasks_on(r.node_id());
    for (TaskId t : on_node) {
      cluster_.requeue(t);
      tasks_[t].moving = false;
    }
    if (n.state == NodeState::kDead) {
      cluster_.set_node_state(r.node_id(), nodes_[r.node_id()].drain_requested
                                               ? NodeState::kDraining
                                               : NodeState::kReady);
    }
  }
  nodes_[r.node_id()].epoch = r.epoch();
  next_epoch_ = r.epoch() + 1;
  return {};
}

std::string MasterState::apply_node_died(const v1::NodeDied& r) {
  if (r.node_id() >= nodes_.size()) return "no node " + std::to_string(r.node_id());
  if (cluster_.node(r.node_id()).state == NodeState::kDead) return "node already dead";
  const std::set<TaskId> lost = cluster_.tasks_on(r.node_id());
  // Revoke the epoch: nothing reported under it is believed again, and a
  // heartbeat carrying it is refused until the node registers again.
  nodes_[r.node_id()].epoch = 0;
  cluster_.set_node_suspect(r.node_id(), false);
  cluster_.set_node_state(r.node_id(), NodeState::kDead);  // requeues its tasks
  // A task that was being moved off this node (it was draining) is now
  // simply pending: the node is gone, there is nothing left to stop.
  for (TaskId t : lost) tasks_[t].moving = false;
  return {};
}

std::string MasterState::apply_node_draining(const v1::NodeDraining& r) {
  if (r.node_id() >= nodes_.size()) return "no node " + std::to_string(r.node_id());
  if (cluster_.node(r.node_id()).state == NodeState::kDead) return "cannot drain a dead node";
  cluster_.set_node_state(r.node_id(), NodeState::kDraining);
  nodes_[r.node_id()].drain_requested = true;
  return {};
}

std::string MasterState::apply_tasks_submitted(const v1::TasksSubmitted& r) {
  if (r.first_task_id() != tasks_.size()) {
    return "first task id " + std::to_string(r.first_task_id()) + ", expected " +
           std::to_string(tasks_.size());
  }
  if (r.count() == 0) return "empty submission";
  const auto policy = from_proto(r.restart_policy());
  if (!policy) return "unknown restart policy";
  // MasterCore answers a repeated request id from the state instead of
  // logging it again, so a second one in the log is a bug.
  if (!r.request_id().empty() && submissions_.count(r.request_id())) {
    return "request id " + r.request_id() + " was already admitted";
  }
  const TaskSpec spec{r.job(), r.priority(), from_proto(r.request()),
                      from_proto(r.required_labels())};
  for (uint32_t i = 0; i < r.count(); ++i) {
    // MasterCore admitted the whole batch before logging it (validation
    // and quota), so a refusal here means the log does not match.
    SubmitResult s = cluster_.submit(spec);
    if (!s.ok) return "task " + std::to_string(r.first_task_id() + i) + " refused: " + s.reason;
    TaskMeta m;
    m.argv.assign(r.argv().begin(), r.argv().end());
    m.image = r.image();
    m.policy = *policy;
    tasks_.push_back(std::move(m));
  }
  if (!r.request_id().empty()) {
    submissions_.emplace(r.request_id(), Submission{r.first_task_id(), r.count()});
  }
  return {};
}

std::string MasterState::apply_task_placed(const v1::TaskPlaced& r) {
  if (!has_task(r.task_id())) return "no task " + std::to_string(r.task_id());
  TaskMeta& m = tasks_[r.task_id()];
  if (r.attempt() != m.attempt + 1) {
    return "task " + std::to_string(r.task_id()) + " placed as attempt " +
           std::to_string(r.attempt()) + " after attempt " + std::to_string(m.attempt);
  }
  if (!cluster_.place(r.task_id(), r.node_id())) {
    return "task " + std::to_string(r.task_id()) + " cannot be placed on node " +
           std::to_string(r.node_id());
  }
  m.attempt = r.attempt();
  m.last_node = r.node_id();
  m.placed_at_ms = r.placed_at_ms();
  m.moving = false;
  m.has_exit = false;  // a new attempt has not exited yet
  m.oom_killed = false;
  return {};
}

std::string MasterState::apply_task_preempted(const v1::TaskPreempted& r) {
  if (!has_task(r.task_id()) || !cluster_.evict(r.task_id())) {
    return "task " + std::to_string(r.task_id()) + " cannot be preempted";
  }
  tasks_[r.task_id()].moving = false;
  return {};
}

std::string MasterState::apply_task_requeued(const v1::TaskRequeued& r) {
  if (!has_task(r.task_id()) || !cluster_.requeue(r.task_id())) {
    return "task " + std::to_string(r.task_id()) + " cannot be requeued";
  }
  tasks_[r.task_id()].moving = false;
  return {};
}

std::string MasterState::apply_task_moving(const v1::TaskMoving& r) {
  if (!has_task(r.task_id())) return "no task " + std::to_string(r.task_id());
  const Task& t = cluster_.task(r.task_id());
  if (t.state != TaskState::kRunning ||
      cluster_.node(*t.node).state != NodeState::kDraining) {
    return "task " + std::to_string(r.task_id()) + " is not running on a draining node";
  }
  tasks_[r.task_id()].moving = true;
  return {};
}

std::string MasterState::apply_task_exited(const v1::TaskExited& r) {
  if (!has_task(r.task_id())) return "no task " + std::to_string(r.task_id());
  const TaskId id = r.task_id();
  if (cluster_.task(id).state != TaskState::kRunning) {
    return "task " + std::to_string(id) + " exited but is not running";
  }
  TaskMeta& m = tasks_[id];
  m.has_exit = true;
  m.exit_code = r.exit_code();
  m.signal = r.signal();
  m.oom_killed = r.oom_killed();
  m.message = r.message();
  m.moving = false;
  switch (r.outcome()) {
    case v1::TaskExited::OUTCOME_MOVED:
      cluster_.requeue(id);
      return {};
    case v1::TaskExited::OUTCOME_BACKOFF:
      cluster_.enter_backoff(id);
      ++m.restarts;
      m.consecutive_restarts = r.consecutive_restarts();
      m.restart_at_ms = r.restart_at_ms();
      backoff_.insert({m.restart_at_ms, id});
      return {};
    case v1::TaskExited::OUTCOME_FINISHED:
      cluster_.finish(id, r.succeeded());
      return {};
    default:
      return "task " + std::to_string(id) + " exit has no outcome";
  }
}

std::string MasterState::apply_task_backoff_over(const v1::TaskBackoffOver& r) {
  if (!has_task(r.task_id())) return "no task " + std::to_string(r.task_id());
  const TaskMeta& m = tasks_[r.task_id()];
  if (!backoff_.erase({m.restart_at_ms, r.task_id()}) || !cluster_.leave_backoff(r.task_id())) {
    return "task " + std::to_string(r.task_id()) + " is not in backoff";
  }
  return {};
}

std::string MasterState::apply_task_killed(const v1::TaskKilled& r) {
  if (!has_task(r.task_id())) return "no task " + std::to_string(r.task_id());
  TaskMeta& m = tasks_[r.task_id()];
  if (cluster_.task(r.task_id()).state == TaskState::kBackoff) {
    backoff_.erase({m.restart_at_ms, r.task_id()});
  }
  if (!cluster_.kill(r.task_id())) {
    return "task " + std::to_string(r.task_id()) + " is already terminal";
  }
  m.moving = false;  // a kill ends a move too
  return {};
}

void MasterState::to_snapshot(v1::Snapshot* out) const {
  out->Clear();
  out->set_lsn(lsn_);
  out->set_next_epoch(next_epoch_);
  for (const Node& n : cluster_.nodes()) {
    const NodeMeta& m = nodes_[n.id];
    v1::SnapshotNode* sn = out->add_nodes();
    sn->set_name(m.name);
    to_proto(n.capacity, sn->mutable_capacity());
    sn->mutable_labels()->insert(n.labels.begin(), n.labels.end());
    sn->set_state(to_proto(n.state));
    sn->set_epoch(m.epoch);
    sn->set_drain_requested(m.drain_requested);
  }
  for (const Task& t : cluster_.tasks()) {
    const TaskMeta& m = tasks_[t.id];
    v1::SnapshotTask* st = out->add_tasks();
    st->set_job(t.job);
    st->set_priority(t.priority);
    to_proto(t.request, st->mutable_request());
    st->mutable_required_labels()->insert(t.required_labels.begin(), t.required_labels.end());
    st->set_state(to_proto(t.state));
    if (t.node) st->set_node(*t.node);
    st->set_preempted_count(t.preempted_count);
    for (const std::string& a : m.argv) st->add_argv(a);
    st->set_image(m.image);
    st->set_restart_policy(to_proto(m.policy));
    st->set_attempt(m.attempt);
    if (m.last_node) st->set_last_node(*m.last_node);
    st->set_placed_at_ms(m.placed_at_ms);
    st->set_moving(m.moving);
    st->set_restarts(m.restarts);
    st->set_consecutive_restarts(m.consecutive_restarts);
    st->set_restart_at_ms(m.restart_at_ms);
    st->set_has_exit(m.has_exit);
    st->set_exit_code(m.exit_code);
    st->set_signal(m.signal);
    st->set_oom_killed(m.oom_killed);
    st->set_message(m.message);
  }
  for (const auto& [id, sub] : submissions_) {  // std::map: in id order
    v1::SnapshotSubmission* ss = out->add_submissions();
    ss->set_request_id(id);
    ss->set_first_task_id(sub.first_task_id);
    ss->set_count(sub.count);
  }
}

std::optional<MasterState> MasterState::from_snapshot(const v1::Snapshot& snap,
                                                      std::string* error) {
  auto fail = [&](const std::string& why) -> std::optional<MasterState> {
    if (error) *error = "snapshot at lsn " + std::to_string(snap.lsn()) + ": " + why;
    return std::nullopt;
  };
  MasterState s;
  s.lsn_ = snap.lsn();
  s.next_epoch_ = snap.next_epoch();
  if (s.next_epoch_ == 0) return fail("next epoch 0");
  for (int i = 0; i < snap.nodes_size(); ++i) {
    const v1::SnapshotNode& sn = snap.nodes(i);
    const auto state = from_proto(sn.state());
    if (!state) return fail("node " + std::to_string(i) + " has no valid state");
    try {
      s.cluster_.restore_node(from_proto(sn.capacity()), from_proto(sn.labels()), *state);
    } catch (const std::invalid_argument& e) {
      return fail(e.what());
    }
    if (!s.node_by_name_.emplace(sn.name(), static_cast<NodeId>(i)).second) {
      return fail("duplicate node name " + sn.name());
    }
    s.nodes_.push_back(NodeMeta{sn.name(), sn.epoch(), sn.drain_requested()});
  }
  for (int i = 0; i < snap.tasks_size(); ++i) {
    const v1::SnapshotTask& st = snap.tasks(i);
    const auto state = from_proto(st.state());
    const auto policy = from_proto(st.restart_policy());
    if (!state || !policy) return fail("task " + std::to_string(i) + " has an invalid enum");
    Task t;
    t.id = static_cast<TaskId>(i);
    t.job = st.job();
    t.priority = st.priority();
    t.request = from_proto(st.request());
    t.required_labels = from_proto(st.required_labels());
    t.state = *state;
    if (st.has_node()) t.node = st.node();
    t.preempted_count = st.preempted_count();
    if (!s.cluster_.restore_task(t)) return fail("task " + std::to_string(i) + " does not fit");
    TaskMeta m;
    m.argv.assign(st.argv().begin(), st.argv().end());
    m.image = st.image();
    m.policy = *policy;
    m.attempt = st.attempt();
    if (st.has_last_node()) m.last_node = st.last_node();
    m.placed_at_ms = st.placed_at_ms();
    m.moving = st.moving();
    m.restarts = st.restarts();
    m.consecutive_restarts = st.consecutive_restarts();
    m.restart_at_ms = st.restart_at_ms();
    m.has_exit = st.has_exit();
    m.exit_code = st.exit_code();
    m.signal = st.signal();
    m.oom_killed = st.oom_killed();
    m.message = st.message();
    if (t.state == TaskState::kBackoff) s.backoff_.insert({m.restart_at_ms, t.id});
    s.tasks_.push_back(std::move(m));
  }
  for (const v1::SnapshotSubmission& ss : snap.submissions()) {
    if (ss.request_id().empty() || ss.count() == 0 ||
        uint64_t{ss.first_task_id()} + ss.count() > s.tasks_.size() ||
        !s.submissions_.emplace(ss.request_id(), Submission{ss.first_task_id(), ss.count()})
             .second) {
      return fail("bad submission record for request id '" + ss.request_id() + "'");
    }
  }
  // Everything derived was rebuilt above; prove it is consistent before
  // anyone acts on it.
  if (std::string e = s.check_invariants(); !e.empty()) return fail(e);
  return s;
}

std::string MasterState::check_invariants() const {
  std::ostringstream err;
  err << cluster_.check_invariants();
  if (nodes_.size() != cluster_.nodes().size() || tasks_.size() != cluster_.tasks().size()) {
    err << "metadata has " << nodes_.size() << " nodes and " << tasks_.size()
        << " tasks, state has " << cluster_.nodes().size() << " and "
        << cluster_.tasks().size() << "\n";
    return err.str();
  }
  if (node_by_name_.size() != nodes_.size()) err << "node name index size mismatch\n";
  for (const auto& [name, id] : node_by_name_) {
    if (id >= nodes_.size() || nodes_[id].name != name) {
      err << "node name " << name << " maps to wrong id " << id << "\n";
    }
  }
  size_t in_backoff = 0;
  for (const Task& t : cluster_.tasks()) {
    const TaskMeta& m = tasks_[t.id];
    if (t.state == TaskState::kRunning && (m.attempt == 0 || m.last_node != t.node)) {
      err << "running task " << t.id << " has attempt " << m.attempt << " or wrong last_node\n";
    }
    if (m.moving && (t.state != TaskState::kRunning ||
                     cluster_.node(*t.node).state != NodeState::kDraining)) {
      err << "task " << t.id << " marked moving but not running on a draining node\n";
    }
    if (t.state == TaskState::kBackoff) {
      ++in_backoff;
      if (!backoff_.count({m.restart_at_ms, t.id})) {
        err << "task " << t.id << " in backoff has no timer\n";
      }
    }
  }
  if (in_backoff != backoff_.size()) {
    err << "backoff timers: " << backoff_.size() << ", tasks in backoff: " << in_backoff << "\n";
  }
  std::set<uint64_t> epochs;
  for (const Node& n : cluster_.nodes()) {
    const uint64_t e = nodes_[n.id].epoch;
    if ((n.state == NodeState::kDead) != (e == 0)) {
      err << "node " << n.id << " state " << to_string(n.state) << " but epoch " << e << "\n";
    }
    if (e != 0 && (e >= next_epoch_ || !epochs.insert(e).second)) {
      err << "node " << n.id << " epoch " << e << " reused or not yet issued\n";
    }
  }
  return err.str();
}

}  // namespace fleet
