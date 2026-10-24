// MasterCore implementation: registration, heartbeat reconciliation,
// admission, list, kill, drain. See master_core.h for the contract.

#include "master_core.h"

#include "log.h"

#include <cstdio>
#include <set>
#include <sstream>
#include <stdexcept>

namespace fleet {
namespace {

Resources from_proto(const v1::Resources& r) {
  return Resources{r.milli_cpu(), r.memory_mb()};
}

void to_proto(const Resources& r, v1::Resources* out) {
  out->set_milli_cpu(r.milli_cpu);
  out->set_memory_mb(r.memory_mb);
}

Labels from_proto(const google::protobuf::Map<std::string, std::string>& m) {
  return Labels(m.begin(), m.end());
}

v1::NodeState to_proto(NodeState s) {
  switch (s) {
    case NodeState::kReady: return v1::NODE_STATE_READY;
    case NodeState::kDraining: return v1::NODE_STATE_DRAINING;
    case NodeState::kDead: return v1::NODE_STATE_DEAD;
  }
  return v1::NODE_STATE_UNSPECIFIED;
}

v1::TaskState to_proto(TaskState s) {
  switch (s) {
    case TaskState::kPending: return v1::TASK_STATE_PENDING;
    case TaskState::kRunning: return v1::TASK_STATE_RUNNING;
    case TaskState::kSucceeded: return v1::TASK_STATE_SUCCEEDED;
    case TaskState::kFailed: return v1::TASK_STATE_FAILED;
    case TaskState::kKilled: return v1::TASK_STATE_KILLED;
  }
  return v1::TASK_STATE_UNSPECIFIED;
}

}  // namespace

MasterCore::MasterCore(MasterOptions options, uint64_t seed)
    : options_(options), scheduler_(options.scheduler), rng_(seed) {}

uint64_t MasterCore::new_incarnation() {
  // Zero is what an agent that never registered would send, so never hand
  // it out.
  uint64_t v = 0;
  while (v == 0) v = rng_();
  return v;
}

void MasterCore::log(const std::string& line) const {
  if (options_.log_events) log_line("fleetmaster", line);
}

grpc::Status MasterCore::register_node(const v1::RegisterNodeRequest& req,
                                       v1::RegisterNodeResponse* resp,
                                       Clock::time_point now) {
  if (req.name().empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "node name is empty"};
  }
  const Resources cap = from_proto(req.capacity());
  const Labels labels = from_proto(req.labels());
  NodeId id = 0;
  auto it = node_by_name_.find(req.name());
  if (it == node_by_name_.end()) {
    try {
      id = state_.add_node(cap, labels);
    } catch (const std::invalid_argument& e) {
      return {grpc::StatusCode::INVALID_ARGUMENT, e.what()};
    }
    nodes_.push_back(NodeMeta{req.name(), 0, now});
    node_by_name_.emplace(req.name(), id);
    log("node " + req.name() + " registered as id " + std::to_string(id) +
        " capacity " + to_string(cap));
  } else {
    // The same agent coming back (it restarted, or it lost contact and was
    // told its incarnation is stale). ClusterState cannot resize a node,
    // so a changed shape is refused rather than silently ignored.
    id = it->second;
    const Node& n = state_.node(id);
    if (!(n.capacity == cap) || n.labels != labels) {
      return {grpc::StatusCode::FAILED_PRECONDITION,
              "node " + req.name() + " re-registered with a different "
              "capacity or labels; resizing a node is not supported"};
    }
    // A new incarnation means the old agent process, and with it the
    // master's record of what it ran, is gone. Requeue its tasks rather
    // than assume the new agent still runs them: they are restarted
    // (possibly on this same node) at a new attempt, and any survivor of
    // the old incarnation that the agent reports is stopped as stale.
    // set_node_state(draining) is the one transition that requeues every
    // task on a node; restoring kReady right after keeps the node usable.
    // A node an operator drained stays drained.
    size_t requeued = state_.tasks_on(id).size();
    if (n.state == NodeState::kReady) {
      state_.set_node_state(id, NodeState::kDraining);
      state_.set_node_state(id, NodeState::kReady);
    }
    log("node " + req.name() + " re-registered; requeued " +
        std::to_string(requeued) + " task(s) of its previous incarnation");
  }
  nodes_[id].incarnation = new_incarnation();
  nodes_[id].last_heartbeat = now;
  needs_schedule_ = true;
  resp->set_node_id(id);
  resp->set_incarnation(nodes_[id].incarnation);
  resp->set_heartbeat_interval_ms(options_.heartbeat_interval_ms);
  return grpc::Status::OK;
}

bool MasterCore::is_current(NodeId node, const v1::TaskInstance& inst) const {
  if (inst.incarnation() != nodes_[node].incarnation) return false;
  if (inst.task_id() >= state_.tasks().size()) return false;
  const Task& t = state_.task(inst.task_id());
  return t.state == TaskState::kRunning && t.node == node &&
         inst.attempt() == tasks_[t.id].attempt;
}

grpc::Status MasterCore::heartbeat(const v1::HeartbeatRequest& req,
                                   v1::HeartbeatResponse* resp,
                                   Clock::time_point now) {
  const NodeId node = req.node_id();
  if (node >= nodes_.size()) {
    return {grpc::StatusCode::NOT_FOUND,
            "unknown node id " + std::to_string(node) + "; register again"};
  }
  if (req.incarnation() != nodes_[node].incarnation) {
    return {grpc::StatusCode::FAILED_PRECONDITION,
            "stale incarnation for node " + nodes_[node].name +
                "; register again"};
  }
  nodes_[node].last_heartbeat = now;

  // Pass 1: the agent's report. Exits of the current instance are applied;
  // every exit is acknowledged (forget), applied or not, because an exit of
  // a stale instance carries no information the master still needs. A
  // running instance that is not current is told to stop.
  std::set<TaskId> reported;
  bool exits_applied = false;
  for (const v1::TaskStatus& st : req.tasks()) {
    const v1::TaskInstance& inst = st.instance();
    const bool current = is_current(node, inst);
    const bool exited = st.phase() == v1::TaskStatus::PHASE_EXITED ||
                        st.phase() == v1::TaskStatus::PHASE_START_FAILED;
    if (!exited) {
      // RUNNING, or a phase this master does not know (a newer agent):
      // either way the process exists, so it is kept only if current.
      if (current) {
        reported.insert(inst.task_id());
      } else {
        *resp->add_stop() = inst;
      }
      continue;
    }
    *resp->add_forget() = inst;
    if (!current) continue;
    TaskMeta& m = tasks_[inst.task_id()];
    m.has_exit = true;
    m.exit_code = st.exit_code();
    m.signal = st.signal();
    m.oom_killed = st.oom_killed();
    m.message = st.message();
    const bool ok = st.phase() == v1::TaskStatus::PHASE_EXITED &&
                    st.signal() == 0 && st.exit_code() == 0;
    state_.finish(inst.task_id(), ok);
    exits_applied = true;
    log("task " + std::to_string(inst.task_id()) + " (" +
        state_.task(inst.task_id()).job + ") " + (ok ? "succeeded" : "failed") +
        " on " + nodes_[node].name + ": " +
        (st.phase() == v1::TaskStatus::PHASE_START_FAILED
             ? "start failed: " + st.message()
         : st.oom_killed() ? "OOM-killed, exit " + std::to_string(st.exit_code())
         : st.signal()     ? "signal " + std::to_string(st.signal())
                           : "exit " + std::to_string(st.exit_code())));
  }

  // An exit just freed room on this node. Run a scheduling pass now,
  // before building the reply, so a pending task placed into that room is
  // started by this reply instead of waiting a full heartbeat interval for
  // the node's next one (measured in NOTES.md m2: this was the slow path
  // for most placements in bench/local_cluster.sh). The pass is the same
  // one the scheduler thread runs, over the whole cluster; it is
  // microseconds, and the lock is already held.
  if (exits_applied) schedule();

  // Pass 2: desired state. Every task placed here that the agent did not
  // report running gets a start. Exits applied above are no longer
  // running, so a finished task is never restarted by its own report.
  for (TaskId id : state_.tasks_on(node)) {
    if (reported.count(id)) continue;
    const Task& t = state_.task(id);
    v1::TaskLaunch* l = resp->add_start();
    l->mutable_instance()->set_task_id(id);
    l->mutable_instance()->set_attempt(tasks_[id].attempt);
    l->mutable_instance()->set_incarnation(nodes_[node].incarnation);
    l->set_job(t.job);
    for (const std::string& a : tasks_[id].argv) l->add_argv(a);
    to_proto(t.request, l->mutable_limits());
    l->set_image(tasks_[id].image);
  }
  return grpc::Status::OK;
}

grpc::Status MasterCore::submit(const v1::SubmitRequest& req,
                                v1::SubmitResponse* resp) {
  if (!valid_job_name(req.job())) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "job name must match [A-Za-z0-9._-]{1,64}: '" + req.job() + "'"};
  }
  if (req.argv().empty() || req.argv(0).empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "empty command"};
  }
  // Checked here so a bad image is a submit error the user sees at once,
  // not a start failure on some node later. The agent checks again.
  if (!req.image().empty() && !valid_image_ref(req.image())) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "invalid image reference: '" + req.image() + "'"};
  }
  const uint32_t count = req.count() == 0 ? 1 : req.count();
  if (count > options_.max_tasks_per_submit) {
    return {grpc::StatusCode::INVALID_ARGUMENT,
            "count " + std::to_string(count) + " exceeds " +
                std::to_string(options_.max_tasks_per_submit)};
  }
  const TaskSpec spec{req.job(), req.priority(), from_proto(req.request()),
                      from_proto(req.required_labels())};

  // All or nothing: check quota for the whole batch before admitting any,
  // so a 5-task job over quota does not end up with 3 tasks admitted.
  // (Per-task validation errors are caught by the first submit below,
  // before anything is admitted, because all tasks share one spec.)
  auto q = state_.quotas().find(spec.job);
  if (q != state_.quotas().end() && q->second.limit) {
    Resources total{spec.request.milli_cpu * count,
                    spec.request.memory_mb * count};
    if (!(q->second.used + total).fits_in(*q->second.limit)) {
      return {grpc::StatusCode::RESOURCE_EXHAUSTED,
              "job " + spec.job + " over quota: used " +
                  to_string(q->second.used) + " + request " + to_string(total) +
                  " > limit " + to_string(*q->second.limit)};
    }
  }
  for (uint32_t i = 0; i < count; ++i) {
    SubmitResult r = state_.submit(spec);
    if (!r.ok) {
      // Only reachable on i == 0 (see above), so nothing was admitted.
      const bool quota = r.reason.find("over quota") != std::string::npos;
      return {quota ? grpc::StatusCode::RESOURCE_EXHAUSTED
                    : grpc::StatusCode::INVALID_ARGUMENT,
              r.reason};
    }
    TaskMeta m;
    m.argv.assign(req.argv().begin(), req.argv().end());
    m.image = req.image();
    tasks_.push_back(std::move(m));
    resp->add_task_ids(r.id);
  }
  needs_schedule_ = true;
  log("job " + spec.job + ": admitted " + std::to_string(count) +
      " task(s), priority " + std::to_string(spec.priority) + ", " +
      to_string(spec.request) + " each");
  return grpc::Status::OK;
}

grpc::Status MasterCore::list(const v1::ListRequest& req, v1::ListResponse* resp,
                              Clock::time_point now) const {
  for (const Node& n : state_.nodes()) {
    v1::NodeInfo* ni = resp->add_nodes();
    ni->set_id(n.id);
    ni->set_name(nodes_[n.id].name);
    ni->set_state(to_proto(n.state));
    to_proto(n.capacity, ni->mutable_capacity());
    to_proto(n.allocated, ni->mutable_allocated());
    ni->mutable_labels()->insert(n.labels.begin(), n.labels.end());
    ni->set_ms_since_heartbeat(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            now - nodes_[n.id].last_heartbeat)
            .count());
    ni->set_running_tasks(static_cast<uint32_t>(state_.tasks_on(n.id).size()));
  }
  for (const Task& t : state_.tasks()) {
    if (!req.job().empty() && t.job != req.job()) continue;
    const TaskMeta& m = tasks_[t.id];
    v1::TaskInfo* ti = resp->add_tasks();
    ti->set_id(t.id);
    ti->set_job(t.job);
    ti->set_priority(t.priority);
    ti->set_state(to_proto(t.state));
    to_proto(t.request, ti->mutable_request());
    if (m.last_node) ti->set_node(nodes_[*m.last_node].name);
    ti->set_attempt(m.attempt);
    for (const std::string& a : m.argv) ti->add_argv(a);
    ti->set_has_exit(m.has_exit);
    ti->set_exit_code(m.exit_code);
    ti->set_signal(m.signal);
    ti->set_message(m.message);
    ti->set_preempted_count(t.preempted_count);
    ti->set_image(m.image);
    ti->set_oom_killed(m.oom_killed);
  }
  return grpc::Status::OK;
}

grpc::Status MasterCore::kill(const v1::KillRequest& req, v1::KillResponse* resp) {
  // Killing only changes desired state. The process is stopped by the
  // node's next heartbeat, which reports it running and gets it back in
  // the stop list because the task is no longer current.
  std::vector<TaskId> targets;
  if (req.target_case() == v1::KillRequest::kTaskId) {
    if (req.task_id() >= state_.tasks().size()) {
      return {grpc::StatusCode::NOT_FOUND,
              "no task " + std::to_string(req.task_id())};
    }
    if (is_terminal(state_.task(req.task_id()).state)) {
      return {grpc::StatusCode::FAILED_PRECONDITION,
              "task " + std::to_string(req.task_id()) + " already " +
                  std::string(to_string(state_.task(req.task_id()).state))};
    }
    targets.push_back(req.task_id());
  } else if (req.target_case() == v1::KillRequest::kJob) {
    for (const Task& t : state_.tasks()) {
      if (t.job == req.job() && !is_terminal(t.state)) targets.push_back(t.id);
    }
  } else {
    return {grpc::StatusCode::INVALID_ARGUMENT, "kill needs a task id or a job"};
  }
  for (TaskId id : targets) {
    if (state_.kill(id)) resp->add_killed(id);
  }
  if (!targets.empty()) needs_schedule_ = true;
  log("killed " + std::to_string(resp->killed_size()) + " task(s)");
  return grpc::Status::OK;
}

grpc::Status MasterCore::drain(const v1::DrainRequest& req, v1::DrainResponse* resp) {
  auto it = node_by_name_.find(req.node());
  if (it == node_by_name_.end()) {
    return {grpc::StatusCode::NOT_FOUND, "no node " + req.node()};
  }
  const NodeId id = it->second;
  // Copy: set_node_state empties the set we would be iterating.
  const std::set<TaskId> running = state_.tasks_on(id);
  state_.set_node_state(id, NodeState::kDraining);
  for (TaskId t : running) resp->add_evicted(t);
  needs_schedule_ = true;
  log("drained " + req.node() + ": " + std::to_string(running.size()) +
      " task(s) back to pending");
  return grpc::Status::OK;
}

ScheduleResult MasterCore::schedule() {
  needs_schedule_ = false;
  ScheduleResult r = scheduler_.schedule_pending(state_);
  for (const PreemptionEvent& p : r.preemptions) {
    log("task " + std::to_string(p.victim) + " preempted on " +
        nodes_[p.node].name + " by task " + std::to_string(p.preemptor));
  }
  // A task can be placed, preempted and placed again within one pass; each
  // placement is a new attempt, so the bump happens per placement event.
  for (const Placement& p : r.placements) {
    TaskMeta& m = tasks_[p.task];
    ++m.attempt;
    m.last_node = p.node;
    m.has_exit = false;  // a new attempt has not exited yet
    m.oom_killed = false;
    log("task " + std::to_string(p.task) + " (" + state_.task(p.task).job +
        ") placed on " + nodes_[p.node].name + ", attempt " +
        std::to_string(m.attempt));
  }
  return r;
}

std::string MasterCore::check_invariants() const {
  std::ostringstream err;
  err << state_.check_invariants();
  if (nodes_.size() != state_.nodes().size()) {
    err << "node metadata has " << nodes_.size() << " entries, state has "
        << state_.nodes().size() << "\n";
  }
  if (tasks_.size() != state_.tasks().size()) {
    err << "task metadata has " << tasks_.size() << " entries, state has "
        << state_.tasks().size() << "\n";
  }
  for (const auto& [name, id] : node_by_name_) {
    if (id >= nodes_.size() || nodes_[id].name != name) {
      err << "node name " << name << " maps to wrong id " << id << "\n";
    }
  }
  for (const Task& t : state_.tasks()) {
    if (t.id >= tasks_.size()) break;
    if (t.state == TaskState::kRunning &&
        (tasks_[t.id].attempt == 0 || tasks_[t.id].last_node != t.node)) {
      err << "running task " << t.id << " has attempt "
          << tasks_[t.id].attempt << " or wrong last_node\n";
    }
  }
  return err.str();
}

}  // namespace fleet
