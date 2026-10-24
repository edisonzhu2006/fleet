// MasterCore implementation: registration and epochs, heartbeat
// reconciliation, the tick (failure detection, backoff timers, scheduling),
// admission, list, kill, drain, and (m4) turning each decision into a log
// record plus the report window after a restart. See master_core.h.

#include "master_core.h"

#include "log.h"
#include "proto_convert.h"

#include <algorithm>
#include <cstdio>
#include <sstream>
#include <stdexcept>

namespace fleet {
namespace {

int64_t ms(Clock::duration d) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
}

FailureDetectorOptions detector_options(const MasterOptions& o) {
  return {std::chrono::milliseconds(o.heartbeat_interval_ms), o.suspect_after_missed,
          o.dead_after_missed};
}

int64_t measure_wall_offset_ms() {
  // Read the two clocks back to back; the microseconds between the reads
  // are noise next to the millisecond resolution of durable times.
  const auto wall = std::chrono::system_clock::now();
  const auto steady = Clock::now();
  return std::chrono::duration_cast<std::chrono::milliseconds>(wall.time_since_epoch()).count() -
         ms(steady.time_since_epoch());
}

}  // namespace

// The scheduler's decisions, carried out as log records (see Actuator in
// core/scheduler.h). emit() applies each record to the very ClusterState
// the pass is reading, which is what the Actuator contract requires.
class MasterCore::LoggingActuator final : public Actuator {
 public:
  LoggingActuator(MasterCore& core, int64_t now_ms) : core_(core), now_ms_(now_ms) {}
  bool place(TaskId task, NodeId node) override {
    v1::WalRecord r;
    v1::TaskPlaced* p = r.mutable_task_placed();
    p->set_task_id(task);
    p->set_node_id(node);
    p->set_attempt(core_.state_.task(task).attempt + 1);
    p->set_placed_at_ms(now_ms_);
    core_.emit(std::move(r));
    core_.log("task " + std::to_string(task) + " (" + core_.state_.cluster().task(task).job +
              ") placed on " + core_.state_.node(node).name + ", attempt " +
              std::to_string(core_.state_.task(task).attempt));
    return true;  // emit() throws instead of returning a refusal
  }
  bool evict(TaskId task) override {
    v1::WalRecord r;
    r.mutable_task_preempted()->set_task_id(task);
    core_.emit(std::move(r));
    return true;
  }

 private:
  MasterCore& core_;
  int64_t now_ms_;
};

MasterCore::MasterCore(MasterOptions options, uint64_t epoch_floor)
    : options_(options),
      scheduler_(options.scheduler),
      detector_(detector_options(options)),
      // Zero means "no valid registration", so it is never issued.
      epoch_floor_(epoch_floor == 0 ? 1 : epoch_floor),
      wall_offset_ms_(options.wall_offset_ms ? *options.wall_offset_ms
                                             : measure_wall_offset_ms()) {}

MasterCore::MasterCore(MasterOptions options, MasterState recovered, Clock::time_point now,
                       uint64_t epoch_floor)
    : MasterCore(options, epoch_floor) {
  state_ = std::move(recovered);
  recovering_ = true;
  recovery_started_ = now;
  recovery_deadline_ = now + options_.recovery_window;
  awaiting_report_.assign(state_.node_count(), false);
  // Heartbeat history did not survive the restart, so every node starts
  // with a clean silence clock: no node is declared dead for silence that
  // happened while the master was down. A node that is not dead starts
  // suspect (it keeps its tasks but gets no new ones) until it reports;
  // if it never does, the detector declares it dead after the usual
  // dead_after missed beats, counted from now. Dead nodes stay dead.
  for (NodeId id = 0; id < state_.node_count(); ++id) {
    if (state_.cluster().node(id).state == NodeState::kDead) {
      detector_.reset(id, now, Health::kDead);
      continue;
    }
    detector_.reset(id, now, Health::kSuspect);
    state_.set_suspect(id, true);
    awaiting_report_[id] = true;
    ++awaiting_count_;
  }
  log("recovered at lsn " + std::to_string(state_.lsn()) + ": " +
      std::to_string(state_.node_count()) + " node(s), " +
      std::to_string(state_.cluster().tasks().size()) + " task(s); waiting up to " +
      std::to_string(options_.recovery_window.count()) + " ms for " +
      std::to_string(awaiting_count_) + " node(s) to report before scheduling");
  if (awaiting_count_ == 0) finish_recovery(now, "no live nodes to wait for");
}

uint64_t MasterCore::default_first_epoch() {
  // Microseconds since 1970 (about 2^50.7 in 2026). Since m4 the epoch
  // counter is durable, so this is only a floor: it keeps a master whose
  // state directory was lost (or a test without one) above every epoch an
  // earlier master issued, provided the wall clock did not step back.
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(
                                   std::chrono::system_clock::now().time_since_epoch())
                                   .count());
}

void MasterCore::log(const std::string& line) const {
  if (options_.log_events) log_line("fleetmaster", line);
}

void MasterCore::emit(v1::WalRecord record) {
  record.set_lsn(state_.lsn() + 1);
  if (std::string err = state_.apply(record); !err.empty()) {
    throw std::logic_error("MasterCore made a record its own state refuses: " + err);
  }
  records_.push_back(std::move(record));
}

std::vector<v1::WalRecord> MasterCore::take_records() {
  std::vector<v1::WalRecord> out;
  out.swap(records_);
  return out;
}

uint64_t MasterCore::issue_epoch() const {
  return std::max(state_.next_epoch(), epoch_floor_);
}

int64_t MasterCore::wall_ms(Clock::time_point t) const {
  return wall_offset_ms_ + ms(t.time_since_epoch());
}

Clock::time_point MasterCore::from_wall_ms(int64_t wall) const {
  return Clock::time_point(std::chrono::milliseconds(wall - wall_offset_ms_));
}

grpc::Status MasterCore::register_node(const v1::RegisterNodeRequest& req,
                                       v1::RegisterNodeResponse* resp,
                                       Clock::time_point now) {
  if (req.name().empty()) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "node name is empty"};
  }
  const Resources cap = from_proto(req.capacity());
  const Labels labels = from_proto(req.labels());
  const std::optional<NodeId> known = state_.find_node(req.name());
  // Checked before anything is logged: a record must apply.
  if (!known && (cap.milli_cpu <= 0 || cap.memory_mb <= 0 ||
                 cap.milli_cpu > ClusterState::kMaxCapacity ||
                 cap.memory_mb > ClusterState::kMaxCapacity)) {
    return {grpc::StatusCode::INVALID_ARGUMENT, "node capacity out of range: " + to_string(cap)};
  }
  if (known) {
    // The same node coming back: its agent restarted, or it was declared
    // dead (or told its epoch is stale) and has killed its tasks.
    // ClusterState cannot resize a node, so a changed shape is refused
    // rather than silently ignored.
    const Node& n = state_.cluster().node(*known);
    if (!(n.capacity == cap) || n.labels != labels) {
      return {grpc::StatusCode::FAILED_PRECONDITION,
              "node " + req.name() + " re-registered with a different "
              "capacity or labels; resizing a node is not supported"};
    }
  }
  const NodeId id = known ? *known : static_cast<NodeId>(state_.node_count());
  const bool was_dead = known && state_.cluster().node(id).state == NodeState::kDead;
  const size_t requeued = known ? state_.cluster().tasks_on(id).size() : 0;

  // A returning node's tasks of its old epoch go back to pending in the
  // apply (see MasterState): they restart, possibly here, at new attempts,
  // and any survivor reported under the old epoch is stopped as stale.
  v1::WalRecord r;
  v1::NodeRegistered* nr = r.mutable_node_registered();
  nr->set_node_id(id);
  nr->set_name(req.name());
  *nr->mutable_capacity() = req.capacity();
  *nr->mutable_labels() = req.labels();
  nr->set_epoch(issue_epoch());
  emit(std::move(r));

  detector_.reset(id, now);
  state_.set_suspect(id, false);
  if (id >= awaiting_report_.size()) awaiting_report_.resize(id + 1, false);
  if (awaiting_report_[id]) {
    // A registration is a report too: the new agent runs nothing yet, and
    // the apply above already requeued what the old one ran.
    awaiting_report_[id] = false;
    --awaiting_count_;
  }
  needs_schedule_ = true;
  if (known) {
    log("node " + req.name() + " re-registered" + (was_dead ? " after being declared dead" : "") +
        "; requeued " + std::to_string(requeued) + " task(s) of its previous epoch");
  } else {
    log("node " + req.name() + " registered as id " + std::to_string(id) + " capacity " +
        to_string(cap));
  }
  resp->set_node_id(id);
  resp->set_epoch(state_.node(id).epoch);
  resp->set_heartbeat_interval_ms(options_.heartbeat_interval_ms);
  return grpc::Status::OK;
}

bool MasterCore::is_current(NodeId node, const v1::TaskInstance& inst) const {
  const uint64_t epoch = state_.node(node).epoch;
  if (epoch == 0 || inst.epoch() != epoch) return false;
  if (inst.task_id() >= state_.cluster().tasks().size()) return false;
  const Task& t = state_.cluster().task(inst.task_id());
  return t.state == TaskState::kRunning && t.node == node &&
         inst.attempt() == state_.task(t.id).attempt;
}

grpc::Status MasterCore::heartbeat(const v1::HeartbeatRequest& req,
                                   v1::HeartbeatResponse* resp,
                                   Clock::time_point now) {
  const NodeId node = req.node_id();
  if (node >= state_.node_count()) {
    return {grpc::StatusCode::NOT_FOUND,
            "unknown node id " + std::to_string(node) + "; register again"};
  }
  // Fencing. The whole report is ignored, not just its tasks: a node
  // declared dead has had its tasks rescheduled, so its exits describe
  // instances the master no longer tracks, and its liveness must not
  // count either (only a new registration revives a dead node).
  const NodeMeta& meta = state_.node(node);
  if (meta.epoch == 0 || req.epoch() != meta.epoch) {
    const std::string why =
        meta.epoch == 0
            ? "node " + meta.name + " was declared dead; epoch " + std::to_string(req.epoch()) +
                  " is revoked"
            : "stale epoch " + std::to_string(req.epoch()) + " for node " + meta.name +
                  " (current " + std::to_string(meta.epoch) + ")";
    return {grpc::StatusCode::FAILED_PRECONDITION, why + "; kill all tasks and register again"};
  }
  const bool first_since_restart = node < awaiting_report_.size() && awaiting_report_[node];
  for (const HealthChange& c : detector_.heard_from(node, now)) {
    state_.set_suspect(node, false);
    needs_schedule_ = true;  // it can take placements again
    if (!first_since_restart) {
      log("node " + meta.name + " recovered from " + std::string(to_string(c.from)) + " after " +
          std::to_string(ms(c.silence)) + " ms of silence");
    }
  }

  // Pass 1: the agent's report. Exits of the current instance are applied;
  // every exit is acknowledged (forget), applied or not, because an exit of
  // a stale instance carries no information the master still needs. A
  // running instance that is not current is told to stop. The forget is
  // safe only because the exit's record is durable before this reply is
  // sent (MasterServer), so a master that crashes cannot lose an exit the
  // agent has already dropped.
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

  if (first_since_restart) reconcile_after_restart(node, reported);

  // An exit just freed room on this node. Run a scheduling pass now,
  // before building the reply, so a pending task placed into that room is
  // started by this reply instead of waiting a full heartbeat interval for
  // the node's next one (measured in NOTES.md m2). The pass is the same
  // one the tick runs, over the whole cluster; the lock is already held.
  // Not during the report window after a restart: nothing is scheduled
  // until the reports are in.
  if (exits_applied && !recovering_) schedule(now);

  reconcile_node(node, reported, resp);
  return grpc::Status::OK;
}

void MasterCore::reconcile_after_restart(NodeId node, const std::set<TaskId>& reported) {
  // Every task the log has running here and the node does not report. The
  // agent applies each reply before sending its next heartbeat, so if the
  // start had reached it, this report would list the task (running or
  // exited). It does not: the reply carrying the start was lost when the
  // master died. Requeue it rather than re-send the start, so the
  // scheduler places it afresh at a new attempt.
  const std::set<TaskId> placed = state_.cluster().tasks_on(node);
  size_t requeued = 0;
  for (TaskId id : placed) {
    if (reported.count(id)) continue;
    v1::WalRecord r;
    r.mutable_task_requeued()->set_task_id(id);
    r.mutable_task_requeued()->set_reason(v1::TaskRequeued::REASON_UNREPORTED_AFTER_RESTART);
    emit(std::move(r));
    ++requeued;
  }
  awaiting_report_[node] = false;
  --awaiting_count_;
  needs_schedule_ = true;  // also wakes the tick to end the window early
  log("node " + state_.node(node).name + " reported after restart: " +
      std::to_string(reported.size()) + " task(s) confirmed running, " +
      std::to_string(requeued) + " not running there requeued; " +
      std::to_string(awaiting_count_) + " node(s) still to report");
}

void MasterCore::finish_recovery(Clock::time_point now, const char* why) {
  recovering_ = false;
  needs_schedule_ = true;
  log("recovery complete after " + std::to_string(ms(now - recovery_started_)) + " ms (" + why +
      "); scheduling resumes" +
      (awaiting_count_ > 0 ? "; " + std::to_string(awaiting_count_) +
                                 " node(s) not yet reported stay suspect"
                           : std::string()));
}

void MasterCore::apply_exit(NodeId node, const v1::TaskStatus& st, Clock::time_point now) {
  const TaskId id = st.instance().task_id();
  const TaskMeta& m = state_.task(id);
  const bool started = st.phase() == v1::TaskStatus::PHASE_EXITED;
  const bool ok = started && st.signal() == 0 && st.exit_code() == 0;
  const std::string how =
      !started        ? "start failed: " + st.message()
      : st.oom_killed() ? "OOM-killed, exit " + std::to_string(st.exit_code())
      : st.signal()     ? "signal " + std::to_string(st.signal())
                        : "exit " + std::to_string(st.exit_code());
  const std::string who = "task " + std::to_string(id) + " (" + state_.cluster().task(id).job + ")";
  const std::string& node_name = state_.node(node).name;

  v1::WalRecord r;
  v1::TaskExited* e = r.mutable_task_exited();
  e->set_task_id(id);
  e->set_started(started);
  e->set_exit_code(st.exit_code());
  e->set_signal(st.signal());
  e->set_oom_killed(st.oom_killed());
  e->set_message(st.message());

  // 1. The master stopped it to move it off a draining node: this exit is
  //    the move completing, not the task's own doing, so it goes back to
  //    pending whatever its policy and its backoff is untouched. (If the
  //    task happened to finish on its own in the instant between the stop
  //    being sent and arriving, it runs once more: at-least-once for that
  //    one race, never two copies at once.)
  if (m.moving) {
    e->set_outcome(v1::TaskExited::OUTCOME_MOVED);
    emit(std::move(r));
    needs_schedule_ = true;
    log(who + " stopped on draining " + node_name + " (" + how + "); moving it to another node");
    return;
  }
  // 2. Its restart policy says run it again: wait out the backoff. The
  //    deadline is decided here and carried in the record, so applying it
  //    (live or on replay) needs no clock.
  if (should_restart(m.policy, ok)) {
    const int64_t now_ms = wall_ms(now);
    // An attempt that stayed up reset_after was healthy, so the crash loop
    // (if any) starts over at the base delay.
    uint32_t consecutive = m.consecutive_restarts;
    if (now_ms - m.placed_at_ms >= options_.backoff.reset_after.count()) consecutive = 0;
    ++consecutive;
    const auto delay = backoff_delay(consecutive, options_.backoff);
    e->set_outcome(v1::TaskExited::OUTCOME_BACKOFF);
    e->set_consecutive_restarts(consecutive);
    e->set_restart_at_ms(now_ms + delay.count());
    emit(std::move(r));
    log(who + " exited on " + node_name + " (" + how + "); policy " +
        std::string(to_string(m.policy)) + ": restart #" + std::to_string(m.restarts) + " in " +
        std::to_string(delay.count()) + " ms");
    return;
  }
  // 3. Final.
  e->set_outcome(v1::TaskExited::OUTCOME_FINISHED);
  e->set_succeeded(ok);
  emit(std::move(r));
  log(who + " " + (ok ? "succeeded" : "failed") + " on " + node_name + ": " + how);
}

void MasterCore::reconcile_node(NodeId node, const std::set<TaskId>& reported,
                                v1::HeartbeatResponse* resp) {
  const bool draining = state_.cluster().node(node).state == NodeState::kDraining;
  const uint64_t epoch = state_.node(node).epoch;
  // Copy: requeues below remove tasks from the set being iterated.
  const std::set<TaskId> desired = state_.cluster().tasks_on(node);
  for (TaskId id : desired) {
    const TaskMeta& m = state_.task(id);
    if (draining) {
      // Graceful drain: stop first, reschedule after. A task the agent
      // reports running is told to stop (again on every heartbeat until
      // its exit arrives; stop is idempotent on the agent). A task it does
      // not report is not running there: the agent sends each heartbeat
      // only after applying the previous reply, and no start for this node
      // has been sent since the drain began, so no start is in flight. It
      // can move now.
      if (reported.count(id)) {
        if (!m.moving) {
          // Logged once, when the first stop goes out: from here on its
          // exit is a move, and a restarted master must know that too.
          v1::WalRecord r;
          r.mutable_task_moving()->set_task_id(id);
          emit(std::move(r));
        }
        v1::TaskInstance* s = resp->add_stop();
        s->set_task_id(id);
        s->set_attempt(m.attempt);
        s->set_epoch(epoch);
      } else {
        v1::WalRecord r;
        r.mutable_task_requeued()->set_task_id(id);
        r.mutable_task_requeued()->set_reason(v1::TaskRequeued::REASON_DRAIN);
        emit(std::move(r));
        needs_schedule_ = true;
        log("task " + std::to_string(id) + " (" + state_.cluster().task(id).job +
            ") was not running on draining " + state_.node(node).name + "; moving it");
      }
      continue;
    }
    // Every task placed here that the agent did not report running gets a
    // start. Exits applied above are no longer on this node, so a finished
    // task is never restarted by its own report.
    if (reported.count(id)) continue;
    const Task& t = state_.cluster().task(id);
    v1::TaskLaunch* l = resp->add_start();
    l->mutable_instance()->set_task_id(id);
    l->mutable_instance()->set_attempt(m.attempt);
    l->mutable_instance()->set_epoch(epoch);
    l->set_job(t.job);
    for (const std::string& a : m.argv) l->add_argv(a);
    to_proto(t.request, l->mutable_limits());
    l->set_image(m.image);
  }
}

void MasterCore::declare_dead(NodeId node, const HealthChange& c) {
  const size_t lost = state_.cluster().tasks_on(node).size();
  // The apply revokes the epoch first: from here on nothing this node
  // reports is believed, and its heartbeats are refused until it
  // registers again. Its tasks go back to pending.
  v1::WalRecord r;
  r.mutable_node_died()->set_node_id(node);
  emit(std::move(r));
  if (node < awaiting_report_.size() && awaiting_report_[node]) {
    // It never reported after the restart; it will not now.
    awaiting_report_[node] = false;
    --awaiting_count_;
  }
  needs_schedule_ = true;
  log("node " + state_.node(node).name + " declared dead after " + std::to_string(c.missed) +
      " missed heartbeats (" + std::to_string(ms(c.silence)) + " ms silent); epoch revoked, " +
      std::to_string(lost) + " task(s) back to pending");
}

TickResult MasterCore::tick(Clock::time_point now) {
  TickResult r;
  // 1. Silence into decisions.
  for (const HealthChange& c : detector_.evaluate(now)) {
    if (c.to == Health::kDead) {
      declare_dead(c.node, c);
      r.died.push_back(c.node);
    } else if (c.to == Health::kSuspect) {
      state_.set_suspect(c.node, true);
      r.suspected.push_back(c.node);
      log("node " + state_.node(c.node).name + " suspect: " + std::to_string(c.missed) +
          " missed heartbeats (" + std::to_string(ms(c.silence)) + " ms silent); no new tasks");
    }
  }
  // 2. Backoff timers that are over: the task is pending again.
  const int64_t now_ms = wall_ms(now);
  while (!state_.backoff().empty() && state_.backoff().begin()->first <= now_ms) {
    const TaskId id = state_.backoff().begin()->second;
    v1::WalRecord rec;
    rec.mutable_task_backoff_over()->set_task_id(id);
    emit(std::move(rec));
    r.restarted.push_back(id);
  }
  // 3. Report before schedule (m4): after a restart, no pass until every
  //    live node has reported or the window is over.
  if (recovering_) {
    if (awaiting_count_ == 0) {
      finish_recovery(now, "every live node reported");
    } else if (now >= recovery_deadline_) {
      finish_recovery(now, "report window over");
    } else {
      // Nothing to do until the window ends. Clear the flag, or the tick
      // thread, woken by it, would run again at once and keep spinning
      // with the lock held, starving the very heartbeats it waits for
      // (bench/master_failure.sh caught exactly that). The timer, or the
      // last report arriving, wakes it again.
      needs_schedule_ = false;
      return r;
    }
  }
  // 4. Give every pending task a node, if one fits.
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
  // UNSPECIFIED is an m2 client: same behavior as before (never).
  const std::optional<RestartPolicy> policy = from_proto(req.restart_policy());
  if (!policy) {  // a value from a newer client this master does not know
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

  // All or nothing: validation and quota for the whole batch before any
  // task is admitted, so a 5-task job over quota does not end up with 3
  // admitted, and so the one record below is known to apply.
  const SubmitResult check = state_.cluster().check_admission(spec, count);
  if (!check.ok) {
    return {check.over_quota ? grpc::StatusCode::RESOURCE_EXHAUSTED
                             : grpc::StatusCode::INVALID_ARGUMENT,
            check.reason};
  }
  const auto first = static_cast<uint32_t>(state_.cluster().tasks().size());
  v1::WalRecord r;
  v1::TasksSubmitted* ts = r.mutable_tasks_submitted();
  ts->set_first_task_id(first);
  ts->set_count(count);
  ts->set_job(req.job());
  ts->set_priority(req.priority());
  *ts->mutable_request() = req.request();
  *ts->mutable_required_labels() = req.required_labels();
  *ts->mutable_argv() = req.argv();
  ts->set_image(req.image());
  ts->set_restart_policy(to_proto(*policy));
  emit(std::move(r));
  for (uint32_t i = 0; i < count; ++i) resp->add_task_ids(first + i);
  needs_schedule_ = true;
  log("job " + spec.job + ": admitted " + std::to_string(count) +
      " task(s), priority " + std::to_string(spec.priority) + ", " +
      to_string(spec.request) + " each, restart " + std::string(to_string(*policy)));
  return grpc::Status::OK;
}

grpc::Status MasterCore::list(const v1::ListRequest& req, v1::ListResponse* resp,
                              Clock::time_point now) const {
  const ClusterState& cs = state_.cluster();
  for (const Node& n : cs.nodes()) {
    v1::NodeInfo* ni = resp->add_nodes();
    ni->set_id(n.id);
    ni->set_name(state_.node(n.id).name);
    ni->set_state(to_proto(n.state));
    to_proto(n.capacity, ni->mutable_capacity());
    to_proto(n.allocated, ni->mutable_allocated());
    ni->mutable_labels()->insert(n.labels.begin(), n.labels.end());
    ni->set_ms_since_heartbeat(ms(now - detector_.last_heard(n.id)));
    ni->set_running_tasks(static_cast<uint32_t>(cs.tasks_on(n.id).size()));
    ni->set_suspect(n.suspect);
    ni->set_epoch(state_.node(n.id).epoch);
  }
  const int64_t now_ms = wall_ms(now);
  for (const Task& t : cs.tasks()) {
    if (!req.job().empty() && t.job != req.job()) continue;
    const TaskMeta& m = state_.task(t.id);
    v1::TaskInfo* ti = resp->add_tasks();
    ti->set_id(t.id);
    ti->set_job(t.job);
    ti->set_priority(t.priority);
    ti->set_state(to_proto(t.state));
    to_proto(t.request, ti->mutable_request());
    if (m.last_node) ti->set_node(state_.node(*m.last_node).name);
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
      ti->set_restart_in_ms(std::max<int64_t>(0, m.restart_at_ms - now_ms));
    }
  }
  return grpc::Status::OK;
}

grpc::Status MasterCore::kill(const v1::KillRequest& req, v1::KillResponse* resp) {
  // Killing only changes desired state. The process is stopped by the
  // node's next heartbeat, which reports it running and gets it back in
  // the stop list because the task is no longer current.
  const ClusterState& cs = state_.cluster();
  std::vector<TaskId> targets;
  if (req.target_case() == v1::KillRequest::kTaskId) {
    if (req.task_id() >= cs.tasks().size()) {
      return {grpc::StatusCode::NOT_FOUND, "no task " + std::to_string(req.task_id())};
    }
    if (is_terminal(cs.task(req.task_id()).state)) {
      return {grpc::StatusCode::FAILED_PRECONDITION,
              "task " + std::to_string(req.task_id()) + " already " +
                  std::string(to_string(cs.task(req.task_id()).state))};
    }
    targets.push_back(req.task_id());
  } else if (req.target_case() == v1::KillRequest::kJob) {
    for (const Task& t : cs.tasks()) {
      if (t.job == req.job() && !is_terminal(t.state)) targets.push_back(t.id);
    }
  } else {
    return {grpc::StatusCode::INVALID_ARGUMENT, "kill needs a task id or a job"};
  }
  for (TaskId id : targets) {
    v1::WalRecord r;
    r.mutable_task_killed()->set_task_id(id);
    emit(std::move(r));
    resp->add_killed(id);
  }
  if (!targets.empty()) needs_schedule_ = true;
  log("killed " + std::to_string(resp->killed_size()) + " task(s)");
  return grpc::Status::OK;
}

grpc::Status MasterCore::drain(const v1::DrainRequest& req, v1::DrainResponse* resp) {
  const std::optional<NodeId> found = state_.find_node(req.node());
  if (!found) return {grpc::StatusCode::NOT_FOUND, "no node " + req.node()};
  const NodeId id = *found;
  if (state_.cluster().node(id).state == NodeState::kDead) {
    // Its tasks were already rescheduled when it died. Marking it draining
    // would also make `list` claim a dead machine is merely draining.
    return {grpc::StatusCode::FAILED_PRECONDITION,
            "node " + req.node() + " is dead; it has no tasks to move"};
  }
  // Only desired state changes here: the node takes no new tasks, and the
  // node half of the reconciler stops and moves its tasks over the next
  // heartbeats. Draining an already draining node is a no-op (no record)
  // that reports what is still moving.
  if (!state_.node(id).drain_requested ||
      state_.cluster().node(id).state != NodeState::kDraining) {
    v1::WalRecord r;
    r.mutable_node_draining()->set_node_id(id);
    emit(std::move(r));
  }
  for (TaskId t : state_.cluster().tasks_on(id)) resp->add_moving(t);
  log("draining " + req.node() + ": " + std::to_string(resp->moving_size()) +
      " task(s) to stop and move");
  return grpc::Status::OK;
}

ScheduleResult MasterCore::schedule(Clock::time_point now) {
  needs_schedule_ = false;
  // Every placement and eviction the pass decides becomes a record, applied
  // as it is made (LoggingActuator), so the pass sees its own decisions.
  // A task can be placed, preempted and placed again within one pass; each
  // placement is its own record and its own attempt.
  LoggingActuator act(*this, wall_ms(now));
  ScheduleResult r = scheduler_.schedule_pending(state_.cluster(), act);
  for (const PreemptionEvent& p : r.preemptions) {
    log("task " + std::to_string(p.victim) + " preempted on " + state_.node(p.node).name +
        " by task " + std::to_string(p.preemptor));
  }
  return r;
}

std::string MasterCore::check_invariants() const {
  // MasterState's checks (ClusterState's, metadata, backoff timers,
  // epochs), plus what involves the soft state:
  //   - the detector has one entry per node;
  //   - detector health agrees with node state: dead <-> kDead, suspect
  //     <-> Node::suspect;
  //   - the nodes still awaiting a first report after a restart are
  //     counted correctly and are not dead.
  std::ostringstream err;
  err << state_.check_invariants();
  const ClusterState& cs = state_.cluster();
  if (detector_.size() != cs.nodes().size()) {
    err << "detector has " << detector_.size() << " entries, state " << cs.nodes().size() << "\n";
    return err.str();
  }
  size_t awaiting = 0;
  for (const Node& n : cs.nodes()) {
    const Health h = detector_.health(n.id);
    if ((h == Health::kDead) != (n.state == NodeState::kDead)) {
      err << "node " << n.id << " health " << to_string(h) << ", state " << to_string(n.state)
          << " disagree\n";
    }
    if ((h == Health::kSuspect) != n.suspect) {
      err << "node " << n.id << " health " << to_string(h) << " but suspect flag " << n.suspect
          << "\n";
    }
    if (n.id < awaiting_report_.size() && awaiting_report_[n.id]) {
      ++awaiting;
      if (n.state == NodeState::kDead) err << "dead node " << n.id << " awaiting a report\n";
    }
  }
  if (awaiting != awaiting_count_) {
    err << "awaiting " << awaiting << " reports, counted " << awaiting_count_ << "\n";
  }
  return err.str();
}

}  // namespace fleet
