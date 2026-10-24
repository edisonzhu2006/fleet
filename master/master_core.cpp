// MasterCore implementation: registration and epochs, heartbeat
// reconciliation, the tick (failure detection, backoff timers, scheduling),
// admission, list, kill, drain. See master_core.h for the model.

#include "master_core.h"

#include "log.h"

#include <algorithm>
#include <cstdio>
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
    case TaskState::kBackoff: return v1::TASK_STATE_BACKOFF;
  }
  return v1::TASK_STATE_UNSPECIFIED;
}

v1::RestartPolicy to_proto(RestartPolicy p) {
  switch (p) {
    case RestartPolicy::kNever: return v1::RESTART_POLICY_NEVER;
    case RestartPolicy::kOnFailure: return v1::RESTART_POLICY_ON_FAILURE;
    case RestartPolicy::kAlways: return v1::RESTART_POLICY_ALWAYS;
  }
  return v1::RESTART_POLICY_UNSPECIFIED;
}

int64_t ms(Clock::duration d) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
}

FailureDetectorOptions detector_options(const MasterOptions& o) {
  return {std::chrono::milliseconds(o.heartbeat_interval_ms), o.suspect_after_missed,
          o.dead_after_missed};
}

}  // namespace

MasterCore::MasterCore(MasterOptions options, uint64_t first_epoch)
    : options_(options),
      scheduler_(options.scheduler),
      detector_(detector_options(options)),
      // Zero means "no valid registration", so it is never issued.
      next_epoch_(first_epoch == 0 ? 1 : first_epoch) {}

uint64_t MasterCore::default_first_epoch() {
  // Microseconds since 1970 (about 2^50.7 in 2026). A master that restarts
  // a second later starts a million epochs higher than this one did, so
  // unless it issued more than one registration per microsecond of its
  // lifetime, the new epochs are above every old one. That is what keeps
  // an old process tagged "task 0, attempt 1, epoch E" from matching a new
  // master's task 0. It relies on the wall clock not stepping backwards
  // across the restart; m4 persists the counter in the write-ahead log and
  // removes that assumption.
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count());
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
    nodes_.push_back(NodeMeta{req.name(), 0});
    node_by_name_.emplace(req.name(), id);
    log("node " + req.name() + " registered as id " + std::to_string(id) +
        " capacity " + to_string(cap));
  } else {
    // The same node coming back: its agent restarted, or it was declared
    // dead (or told its epoch is stale) and has killed its tasks.
    // ClusterState cannot resize a node, so a changed shape is refused
    // rather than silently ignored.
    id = it->second;
    const Node& n = state_.node(id);
    if (!(n.capacity == cap) || n.labels != labels) {
      return {grpc::StatusCode::FAILED_PRECONDITION,
              "node " + req.name() + " re-registered with a different "
              "capacity or labels; resizing a node is not supported"};
    }
    // A new registration means the agent process the master knew, and
    // whatever it ran, is gone or fenced. Requeue its tasks rather than
    // assume the new agent still runs them: they restart (possibly here)
    // at a new attempt, and any survivor reported under the old epoch is
    // stopped as stale. A dead node has no tasks left; it comes back
    // ready. A node an operator drained stays drained.
    const std::set<TaskId> on_node = state_.tasks_on(id);
    for (TaskId t : on_node) {
      state_.requeue(t);
      tasks_[t].moving = false;
    }
    const bool was_dead = n.state == NodeState::kDead;
    if (was_dead) {
      state_.set_node_state(id, nodes_[id].drain_requested ? NodeState::kDraining
                                                           : NodeState::kReady);
    }
    state_.set_node_suspect(id, false);
    log("node " + req.name() + " re-registered" + (was_dead ? " after being declared dead" : "") +
        "; requeued " + std::to_string(on_node.size()) + " task(s) of its previous epoch");
  }
  nodes_[id].epoch = next_epoch_++;
  detector_.reset(id, now);
  needs_schedule_ = true;
  resp->set_node_id(id);
  resp->set_epoch(nodes_[id].epoch);
  resp->set_heartbeat_interval_ms(options_.heartbeat_interval_ms);
  return grpc::Status::OK;
}

bool MasterCore::is_current(NodeId node, const v1::TaskInstance& inst) const {
  if (nodes_[node].epoch == 0 || inst.epoch() != nodes_[node].epoch) return false;
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
  // Fencing. The whole report is ignored, not just its tasks: a node
  // declared dead has had its tasks rescheduled, so its exits describe
  // instances the master no longer tracks, and its liveness must not
  // count either (only a new registration revives a dead node).
  if (nodes_[node].epoch == 0 || req.epoch() != nodes_[node].epoch) {
    const std::string why =
        nodes_[node].epoch == 0
            ? "node " + nodes_[node].name + " was declared dead; epoch " +
                  std::to_string(req.epoch()) + " is revoked"
            : "stale epoch " + std::to_string(req.epoch()) + " for node " +
                  nodes_[node].name + " (current " + std::to_string(nodes_[node].epoch) + ")";
    return {grpc::StatusCode::FAILED_PRECONDITION,
            why + "; kill all tasks and register again"};
  }
  for (const HealthChange& c : detector_.heard_from(node, now)) {
    state_.set_node_suspect(node, false);
    needs_schedule_ = true;  // it can take placements again
    log("node " + nodes_[node].name + " recovered from " + std::string(to_string(c.from)) +
        " after " + std::to_string(ms(c.silence)) + " ms of silence");
  }

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
    apply_exit(node, st, now);
    exits_applied = true;
  }

  // An exit just freed room on this node. Run a scheduling pass now,
  // before building the reply, so a pending task placed into that room is
  // started by this reply instead of waiting a full heartbeat interval for
  // the node's next one (measured in NOTES.md m2). The pass is the same
  // one the tick runs, over the whole cluster; the lock is already held.
  if (exits_applied) schedule(now);

  reconcile_node(node, reported, resp);
  return grpc::Status::OK;
}

void MasterCore::apply_exit(NodeId node, const v1::TaskStatus& st, Clock::time_point now) {
  const TaskId id = st.instance().task_id();
  TaskMeta& m = tasks_[id];
  m.has_exit = true;
  m.exit_code = st.exit_code();
  m.signal = st.signal();
  m.oom_killed = st.oom_killed();
  m.message = st.message();
  const bool started = st.phase() == v1::TaskStatus::PHASE_EXITED;
  const bool ok = started && st.signal() == 0 && st.exit_code() == 0;
  const std::string how =
      !started        ? "start failed: " + st.message()
      : st.oom_killed() ? "OOM-killed, exit " + std::to_string(st.exit_code())
      : st.signal()     ? "signal " + std::to_string(st.signal())
                        : "exit " + std::to_string(st.exit_code());
  const std::string who = "task " + std::to_string(id) + " (" + state_.task(id).job + ")";

  // 1. The master stopped it to move it off a draining node: this exit is
  //    the move completing, not the task's own doing, so it goes back to
  //    pending whatever its policy and its backoff is untouched. (If the
  //    task happened to finish on its own in the instant between the stop
  //    being sent and arriving, it runs once more: at-least-once for that
  //    one race, never two copies at once.)
  if (m.moving) {
    state_.requeue(id);
    m.moving = false;
    needs_schedule_ = true;
    log(who + " stopped on draining " + nodes_[node].name + " (" + how +
        "); moving it to another node");
    return;
  }
  // 2. Its restart policy says run it again: wait out the backoff.
  if (should_restart(m.policy, ok)) {
    // An attempt that stayed up reset_after was healthy, so the crash loop
    // (if any) starts over at the base delay.
    if (now - m.placed_at >= options_.backoff.reset_after) m.consecutive_restarts = 0;
    ++m.consecutive_restarts;
    ++m.restarts;
    const auto delay = backoff_delay(m.consecutive_restarts, options_.backoff);
    m.restart_at = now + delay;
    state_.enter_backoff(id);
    backoff_.insert({m.restart_at, id});
    log(who + " exited on " + nodes_[node].name + " (" + how + "); policy " +
        std::string(to_string(m.policy)) + ": restart #" + std::to_string(m.restarts) +
        " in " + std::to_string(delay.count()) + " ms");
    return;
  }
  // 3. Final.
  state_.finish(id, ok);
  log(who + " " + (ok ? "succeeded" : "failed") + " on " + nodes_[node].name + ": " + how);
}

void MasterCore::reconcile_node(NodeId node, const std::set<TaskId>& reported,
                                v1::HeartbeatResponse* resp) {
  const bool draining = state_.node(node).state == NodeState::kDraining;
  // Copy: requeue() below removes tasks from the set being iterated.
  const std::set<TaskId> desired = state_.tasks_on(node);
  for (TaskId id : desired) {
    TaskMeta& m = tasks_[id];
    if (draining) {
      // Graceful drain: stop first, reschedule after. A task the agent
      // reports running is told to stop (again on every heartbeat until
      // its exit arrives; stop is idempotent on the agent). A task it does
      // not report is not running there: the agent sends each heartbeat
      // only after applying the previous reply, and no start for this node
      // has been sent since the drain began, so no start is in flight. It
      // can move now.
      if (reported.count(id)) {
        m.moving = true;
        v1::TaskInstance* s = resp->add_stop();
        s->set_task_id(id);
        s->set_attempt(m.attempt);
        s->set_epoch(nodes_[node].epoch);
      } else {
        state_.requeue(id);
        needs_schedule_ = true;
        log("task " + std::to_string(id) + " (" + state_.task(id).job +
            ") was not running on draining " + nodes_[node].name + "; moving it");
      }
      continue;
    }
    // Every task placed here that the agent did not report running gets a
    // start. Exits applied above are no longer on this node, so a finished
    // task is never restarted by its own report.
    if (reported.count(id)) continue;
    const Task& t = state_.task(id);
    v1::TaskLaunch* l = resp->add_start();
    l->mutable_instance()->set_task_id(id);
    l->mutable_instance()->set_attempt(m.attempt);
    l->mutable_instance()->set_epoch(nodes_[node].epoch);
    l->set_job(t.job);
    for (const std::string& a : m.argv) l->add_argv(a);
    to_proto(t.request, l->mutable_limits());
    l->set_image(m.image);
  }
}

void MasterCore::declare_dead(NodeId node, const HealthChange& c) {
  const std::set<TaskId> lost = state_.tasks_on(node);
  // Revoke the epoch first: from here on nothing this node reports is
  // believed, and its heartbeats are refused until it registers again.
  nodes_[node].epoch = 0;
  state_.set_node_suspect(node, false);
  state_.set_node_state(node, NodeState::kDead);  // requeues its tasks
  // A task that was being moved off this node (it was draining) is now
  // simply pending: the node is gone, there is nothing left to stop.
  for (TaskId t : lost) tasks_[t].moving = false;
  needs_schedule_ = true;
  log("node " + nodes_[node].name + " declared dead after " + std::to_string(c.missed) +
      " missed heartbeats (" + std::to_string(ms(c.silence)) + " ms silent); epoch revoked, " +
      std::to_string(lost.size()) + " task(s) back to pending");
}

TickResult MasterCore::tick(Clock::time_point now) {
  TickResult r;
  // 1. Silence into decisions.
  for (const HealthChange& c : detector_.evaluate(now)) {
    if (c.to == Health::kDead) {
      declare_dead(c.node, c);
      r.died.push_back(c.node);
    } else if (c.to == Health::kSuspect) {
      state_.set_node_suspect(c.node, true);
      r.suspected.push_back(c.node);
      log("node " + nodes_[c.node].name + " suspect: " + std::to_string(c.missed) +
          " missed heartbeats (" + std::to_string(ms(c.silence)) + " ms silent); no new tasks");
    }
  }
  // 2. Backoff timers that are over: the task is pending again.
  while (!backoff_.empty() && backoff_.begin()->first <= now) {
    const TaskId id = backoff_.begin()->second;
    backoff_.erase(backoff_.begin());
    state_.leave_backoff(id);
    r.restarted.push_back(id);
  }
  // 3. Give every pending task a node, if one fits.
  r.schedule = schedule(now);
  return r;
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
  RestartPolicy policy = RestartPolicy::kNever;
  switch (req.restart_policy()) {
    case v1::RESTART_POLICY_UNSPECIFIED:  // an m2 client: same behavior as before
    case v1::RESTART_POLICY_NEVER: policy = RestartPolicy::kNever; break;
    case v1::RESTART_POLICY_ON_FAILURE: policy = RestartPolicy::kOnFailure; break;
    case v1::RESTART_POLICY_ALWAYS: policy = RestartPolicy::kAlways; break;
    default:  // a value from a newer client this master does not know
      return {grpc::StatusCode::INVALID_ARGUMENT,
              "unknown restart policy " + std::to_string(req.restart_policy())};
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
    m.policy = policy;
    tasks_.push_back(std::move(m));
    resp->add_task_ids(r.id);
  }
  needs_schedule_ = true;
  log("job " + spec.job + ": admitted " + std::to_string(count) +
      " task(s), priority " + std::to_string(spec.priority) + ", " +
      to_string(spec.request) + " each, restart " + std::string(to_string(policy)));
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
    ni->set_ms_since_heartbeat(ms(now - detector_.last_heard(n.id)));
    ni->set_running_tasks(static_cast<uint32_t>(state_.tasks_on(n.id).size()));
    ni->set_suspect(n.suspect);
    ni->set_epoch(nodes_[n.id].epoch);
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
    ti->set_restart_policy(to_proto(m.policy));
    ti->set_restarts(m.restarts);
    if (t.state == TaskState::kBackoff) {
      ti->set_restart_in_ms(std::max<int64_t>(0, ms(m.restart_at - now)));
    }
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
    if (state_.task(id).state == TaskState::kBackoff) {
      backoff_.erase({tasks_[id].restart_at, id});
    }
    if (state_.kill(id)) resp->add_killed(id);
    tasks_[id].moving = false;  // a kill ends a move too
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
  if (state_.node(id).state == NodeState::kDead) {
    // Its tasks were already rescheduled when it died. Marking it draining
    // would also make `list` claim a dead machine is merely draining.
    return {grpc::StatusCode::FAILED_PRECONDITION,
            "node " + req.node() + " is dead; it has no tasks to move"};
  }
  // Only desired state changes here: the node takes no new tasks, and the
  // node half of the reconciler stops and moves its tasks over the next
  // heartbeats. Draining an already draining node is a no-op that reports
  // what is still moving.
  state_.set_node_state(id, NodeState::kDraining);
  nodes_[id].drain_requested = true;
  for (TaskId t : state_.tasks_on(id)) resp->add_moving(t);
  log("draining " + req.node() + ": " + std::to_string(resp->moving_size()) +
      " task(s) to stop and move");
  return grpc::Status::OK;
}

ScheduleResult MasterCore::schedule(Clock::time_point now) {
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
    m.placed_at = now;
    m.moving = false;
    m.has_exit = false;  // a new attempt has not exited yet
    m.oom_killed = false;
    log("task " + std::to_string(p.task) + " (" + state_.task(p.task).job +
        ") placed on " + nodes_[p.node].name + ", attempt " +
        std::to_string(m.attempt));
  }
  return r;
}

std::string MasterCore::check_invariants() const {
  // On top of ClusterState's own checks:
  //   - metadata vectors are parallel to ClusterState's, names map to ids;
  //   - every placed task has attempt >= 1 and last_node == its node;
  //   - "moving" only on tasks running on a draining node;
  //   - backoff_ holds exactly the tasks in kBackoff, at their restart_at;
  //   - detector health agrees with node state: dead <-> kDead <-> epoch
  //     0, suspect <-> Node::suspect;
  //   - epochs of live nodes are distinct and below next_epoch_.
  std::ostringstream err;
  err << state_.check_invariants();
  if (nodes_.size() != state_.nodes().size() || detector_.size() != state_.nodes().size()) {
    err << "node metadata has " << nodes_.size() << " entries, detector "
        << detector_.size() << ", state " << state_.nodes().size() << "\n";
    return err.str();
  }
  if (tasks_.size() != state_.tasks().size()) {
    err << "task metadata has " << tasks_.size() << " entries, state has "
        << state_.tasks().size() << "\n";
    return err.str();
  }
  for (const auto& [name, id] : node_by_name_) {
    if (id >= nodes_.size() || nodes_[id].name != name) {
      err << "node name " << name << " maps to wrong id " << id << "\n";
    }
  }
  size_t in_backoff = 0;
  for (const Task& t : state_.tasks()) {
    const TaskMeta& m = tasks_[t.id];
    if (t.state == TaskState::kRunning && (m.attempt == 0 || m.last_node != t.node)) {
      err << "running task " << t.id << " has attempt " << m.attempt
          << " or wrong last_node\n";
    }
    if (m.moving && (t.state != TaskState::kRunning ||
                     state_.node(*t.node).state != NodeState::kDraining)) {
      err << "task " << t.id << " marked moving but not running on a draining node\n";
    }
    if (t.state == TaskState::kBackoff) {
      ++in_backoff;
      if (!backoff_.count({m.restart_at, t.id})) {
        err << "task " << t.id << " in backoff has no timer\n";
      }
    }
  }
  if (in_backoff != backoff_.size()) {
    err << "backoff timers: " << backoff_.size() << ", tasks in backoff: " << in_backoff << "\n";
  }
  std::set<uint64_t> epochs;
  for (const Node& n : state_.nodes()) {
    const Health h = detector_.health(n.id);
    const uint64_t e = nodes_[n.id].epoch;
    if ((h == Health::kDead) != (n.state == NodeState::kDead) ||
        (n.state == NodeState::kDead) != (e == 0)) {
      err << "node " << n.id << " health " << to_string(h) << ", state "
          << to_string(n.state) << ", epoch " << e << " disagree\n";
    }
    if ((h == Health::kSuspect) != n.suspect) {
      err << "node " << n.id << " health " << to_string(h) << " but suspect flag "
          << n.suspect << "\n";
    }
    if (e != 0 && (e >= next_epoch_ || !epochs.insert(e).second)) {
      err << "node " << n.id << " epoch " << e << " reused or not yet issued\n";
    }
  }
  return err.str();
}

}  // namespace fleet
