// Agent implementation: the register / reap / heartbeat loop and how a
// heartbeat reply is applied. See agent.h for the model.

#include "agent.h"

#include "log.h"

#include <cstdio>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

namespace fleet {
namespace {

std::chrono::system_clock::time_point deadline_after(std::chrono::milliseconds d) {
  return std::chrono::system_clock::now() + d;
}

}  // namespace

Agent::Agent(AgentOptions options, std::unique_ptr<Runtime> runtime)
    : options_(std::move(options)), runtime_(std::move(runtime)) {
  grpc::ChannelArguments args;
  // gRPC backs off reconnects exponentially up to 120 s by default; after
  // a master restart that could leave an agent silent for minutes. Cap it
  // near the heartbeat interval so the agent is back within a beat or two.
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 1000);
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
  stub_ = v1::Master::NewStub(grpc::CreateCustomChannel(
      options_.master_address, grpc::InsecureChannelCredentials(), args));
}

Agent::~Agent() {
  request_stop();
  join();
}

void Agent::start_background() {
  thread_ = std::thread([this] { run(); });
}

void Agent::request_stop() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    stop_ = true;
  }
  cv_.notify_all();
}

void Agent::join() {
  if (thread_.joinable()) thread_.join();
}

bool Agent::stop_requested() const {
  std::lock_guard<std::mutex> lock(mu_);
  return stop_;
}

Agent::Stats Agent::stats() const {
  std::lock_guard<std::mutex> lock(mu_);
  return stats_;
}

void Agent::publish_stats() {
  std::lock_guard<std::mutex> lock(mu_);
  stats_.registered = registered_;
  stats_.node_id = node_id_;
  stats_.running = runtime_->live_count();
  stats_.heartbeats_ok = hb_ok_;
  stats_.heartbeats_failed = hb_failed_;
  stats_.started = started_;
}

void Agent::log(const std::string& line) const {
  if (options_.log_events) {
    log_line("fleetlet[" + options_.name + "]", line);
  }
}

Agent::Key Agent::key_of(const v1::TaskInstance& inst) {
  return Key{inst.task_id(), inst.attempt(), inst.incarnation()};
}

std::string Agent::container_name(uint32_t task_id, uint32_t attempt,
                                  uint64_t incarnation) {
  // fleet-<task>-<attempt>-<first 8 hex digits of the incarnation>.
  // (task, attempt) alone is unique only within one master's lifetime: a
  // restarted master numbers tasks from 0 again, and Docker refuses a
  // second container with a name in use (a leftover from before the
  // restart would make every start fail). The incarnation is random per
  // registration, so adding it makes names unique across restarts of
  // either side, and keeps them readable in `docker ps`.
  char inc[17];
  std::snprintf(inc, sizeof(inc), "%016llx", static_cast<unsigned long long>(incarnation));
  return "fleet-" + std::to_string(task_id) + "-" + std::to_string(attempt) + "-" +
         std::string(inc, 8);
}

void Agent::run() {
  std::error_code ec;
  std::filesystem::create_directories(options_.work_dir, ec);
  auto next_beat = std::chrono::steady_clock::now();
  while (!stop_requested()) {
    collect_exits();
    const auto now = std::chrono::steady_clock::now();
    if (now >= next_beat) {
      const bool was_registered = registered_;
      if (!registered_) {
        do_register();
      } else {
        do_heartbeat();
      }
      // Scheduled from when this beat started, not from when the call
      // returned, so a slow call does not stretch the cadence. A heartbeat
      // that was refused as unknown/stale re-registers at once; a failed
      // registration (master down) waits a full interval before retrying.
      next_beat = (was_registered && !registered_)
                      ? now
                      : now + options_.heartbeat_interval;
    }
    publish_stats();
    // Interruptible sleep: request_stop() wakes us immediately.
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait_until(lock,
                   std::min(next_beat, std::chrono::steady_clock::now() +
                                           options_.poll_interval),
                   [this] { return stop_; });
  }
  runtime_->kill_all();
  log("stopped; killed remaining tasks");
  publish_stats();
}

void Agent::do_register() {
  v1::RegisterNodeRequest req;
  req.set_name(options_.name);
  req.mutable_capacity()->set_milli_cpu(options_.capacity.milli_cpu);
  req.mutable_capacity()->set_memory_mb(options_.capacity.memory_mb);
  req.mutable_labels()->insert(options_.labels.begin(), options_.labels.end());
  v1::RegisterNodeResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(deadline_after(options_.rpc_deadline));
  grpc::Status s = stub_->RegisterNode(&ctx, req, &resp);
  if (!s.ok()) {
    ++hb_failed_;
    log("register failed: " + s.error_message());
    return;
  }
  registered_ = true;
  node_id_ = resp.node_id();
  incarnation_ = resp.incarnation();
  if (resp.heartbeat_interval_ms() > 0) {
    options_.heartbeat_interval = std::chrono::milliseconds(resp.heartbeat_interval_ms());
  }
  log("registered as node " + std::to_string(node_id_));
}

void Agent::collect_exits() {
  for (const ExitInfo& e : runtime_->reap()) {
    auto h = by_handle_.find(e.handle);
    if (h == by_handle_.end()) continue;
    Instance& inst = instances_.at(h->second);
    // A runtime with a slow start (DockerRuntime) reports a start that
    // failed after start() returned as an exit with start_failed; to the
    // master it is the same START_FAILED as a synchronous failure.
    inst.phase = e.start_failed ? v1::TaskStatus::PHASE_START_FAILED
                                : v1::TaskStatus::PHASE_EXITED;
    inst.exit_code = e.exit_code;
    inst.signal = e.signal;
    inst.oom_killed = e.oom_killed;
    inst.message = e.message;
    log("task " + std::to_string(h->second.task_id) + " attempt " +
        std::to_string(h->second.attempt) +
        (e.start_failed ? " failed to start: " + e.message
         : e.oom_killed ? " OOM-killed (exit " + std::to_string(e.exit_code) + ")"
         : e.signal     ? " exited: signal " + std::to_string(e.signal)
                        : " exited: code " + std::to_string(e.exit_code)));
    by_handle_.erase(h);
  }
}

void Agent::do_heartbeat() {
  v1::HeartbeatRequest req;
  req.set_node_id(node_id_);
  req.set_incarnation(incarnation_);
  for (const auto& [key, inst] : instances_) {
    v1::TaskStatus* st = req.add_tasks();
    st->mutable_instance()->set_task_id(key.task_id);
    st->mutable_instance()->set_attempt(key.attempt);
    st->mutable_instance()->set_incarnation(key.incarnation);
    st->set_phase(inst.phase);
    st->set_exit_code(inst.exit_code);
    st->set_signal(inst.signal);
    st->set_oom_killed(inst.oom_killed);
    st->set_message(inst.message);
  }
  v1::HeartbeatResponse resp;
  grpc::ClientContext ctx;
  ctx.set_deadline(deadline_after(options_.rpc_deadline));
  grpc::Status s = stub_->Heartbeat(&ctx, req, &resp);
  if (!s.ok()) {
    ++hb_failed_;
    if (s.error_code() == grpc::StatusCode::NOT_FOUND ||
        s.error_code() == grpc::StatusCode::FAILED_PRECONDITION) {
      // The master does not know this registration (it restarted, or it
      // gave our node a newer incarnation). Register again; tasks keep
      // running and are reported under their old incarnation, which the
      // master will tell us to stop.
      registered_ = false;
    }
    log("heartbeat failed: " + s.error_message());
    return;
  }
  ++hb_ok_;
  // Order: forget, then stop, then start. Forget only touches exited
  // instances and start only adds keys not in the table, so the order is
  // not load-bearing; this one reads as "clean up, then act".
  for (const v1::TaskInstance& f : resp.forget()) {
    auto it = instances_.find(key_of(f));
    if (it != instances_.end() && it->second.phase != v1::TaskStatus::PHASE_RUNNING) {
      instances_.erase(it);
    }
  }
  for (const v1::TaskInstance& st : resp.stop()) {
    auto it = instances_.find(key_of(st));
    if (it != instances_.end() && it->second.phase == v1::TaskStatus::PHASE_RUNNING) {
      log("stopping task " + std::to_string(st.task_id()) + " attempt " +
          std::to_string(st.attempt()));
      runtime_->stop(it->second.handle);
    }
  }
  for (const v1::TaskLaunch& l : resp.start()) launch(l);
}

void Agent::launch(const v1::TaskLaunch& l) {
  const Key key = key_of(l.instance());
  if (instances_.count(key)) return;  // already have it: a re-sent start
  Instance inst;
  // The master validates job names, but a name becomes a file path here,
  // so check again rather than trust the network.
  if (!valid_job_name(l.job())) {
    inst.phase = v1::TaskStatus::PHASE_START_FAILED;
    inst.message = "invalid job name";
    instances_.emplace(key, inst);
    return;
  }
  // Same for the image: it lands in docker's argv right before the
  // command, where a leading '-' would be read as a flag.
  if (!l.image().empty() && !valid_image_ref(l.image())) {
    inst.phase = v1::TaskStatus::PHASE_START_FAILED;
    inst.message = "invalid image reference";
    instances_.emplace(key, inst);
    return;
  }
  LaunchSpec spec;
  spec.argv.assign(l.argv().begin(), l.argv().end());
  spec.limits = Resources{l.limits().milli_cpu(), l.limits().memory_mb()};
  spec.log_path = options_.work_dir / (l.job() + ".t" + std::to_string(key.task_id) +
                                       ".a" + std::to_string(key.attempt) + ".log");
  spec.image = l.image();
  spec.name = container_name(key.task_id, key.attempt, key.incarnation);
  StartResult r = runtime_->start(spec);
  if (r.ok) {
    inst.handle = r.handle;
    by_handle_[r.handle] = key;
    ++started_;
    log("started task " + std::to_string(key.task_id) + " (" + l.job() +
        ") attempt " + std::to_string(key.attempt) + ", log " + spec.log_path.string());
  } else {
    inst.phase = v1::TaskStatus::PHASE_START_FAILED;
    inst.message = r.error;
    log("start of task " + std::to_string(key.task_id) + " failed: " + r.error);
  }
  instances_.emplace(key, inst);
}

}  // namespace fleet
