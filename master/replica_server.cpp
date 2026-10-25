// ReplicaServer implementation: the Raft driver (persist, send, apply),
// leadership changes (build or drop the leader's MasterCore), the Master
// service with redirects and commit-and-confirm waits, and the tick
// thread. See replica_server.h for the model.

#include "replica_server.h"

#include <cstdlib>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

#include "fleet.grpc.pb.h"
#include "leader_redirect.h"
#include "log.h"

namespace fleet {
namespace {

[[noreturn]] void fatal(const std::string& why) {
  log_line("fleetmaster", "FATAL: " + why);
  std::abort();
}

// Runs f, and stops the process if it throws. RaftCore and MasterCore
// throw only when an invariant is broken (two leaders in a term, a record
// the state refuses), after possibly changing part of their state. Inside
// a gRPC handler an exception would be caught by gRPC and turned into an
// UNKNOWN reply, and the replica would run on with that half-changed
// state; stopping is the safe move, and the next start recovers from what
// the disk holds.
template <typename F>
auto guarded(F&& f) -> decltype(f()) {
  try {
    return f();
  } catch (const std::exception& e) {
    fatal(std::string("invariant broken: ") + e.what());
  }
}

// Deterministic serialization (map entries in key order), so equal
// states give equal bytes; Trim() flushes into `out` before it is read.
std::string canonical_bytes(const google::protobuf::Message& m) {
  std::string out;
  google::protobuf::io::StringOutputStream sos(&out);
  google::protobuf::io::CodedOutputStream cos(&sos);
  cos.SetSerializationDeterministic(true);
  m.SerializeToCodedStream(&cos);
  cos.Trim();
  return out;
}

v1::ReplicaStatusResponse::Role to_proto(raft::Role r) {
  switch (r) {
    case raft::Role::kFollower: return v1::ReplicaStatusResponse::ROLE_FOLLOWER;
    case raft::Role::kCandidate: return v1::ReplicaStatusResponse::ROLE_CANDIDATE;
    case raft::Role::kLeader: return v1::ReplicaStatusResponse::ROLE_LEADER;
  }
  return v1::ReplicaStatusResponse::ROLE_UNSPECIFIED;
}

}  // namespace

// The agent- and user-facing service. Every call goes through serve():
// redirect unless serving, run the MasterCore call, propose its records,
// and hold the reply until it is committed and confirmed.
class ReplicaServer::MasterService final : public v1::Master::Service {
 public:
  explicit MasterService(ReplicaServer* r) : r_(r) {}

  grpc::Status RegisterNode(grpc::ServerContext* ctx, const v1::RegisterNodeRequest* req,
                            v1::RegisterNodeResponse* resp) override {
    return serve(ctx, [&](MasterCore& c) { return c.register_node(*req, resp, Clock::now()); });
  }
  grpc::Status Heartbeat(grpc::ServerContext* ctx, const v1::HeartbeatRequest* req,
                         v1::HeartbeatResponse* resp) override {
    return serve(ctx, [&](MasterCore& c) { return c.heartbeat(*req, resp, Clock::now()); });
  }
  grpc::Status Submit(grpc::ServerContext* ctx, const v1::SubmitRequest* req,
                      v1::SubmitResponse* resp) override {
    return serve(ctx, [&](MasterCore& c) { return c.submit(*req, resp); });
  }
  grpc::Status List(grpc::ServerContext* ctx, const v1::ListRequest* req,
                    v1::ListResponse* resp) override {
    return serve(ctx, [&](MasterCore& c) { return c.list(*req, resp, Clock::now()); });
  }
  grpc::Status Kill(grpc::ServerContext* ctx, const v1::KillRequest* req,
                    v1::KillResponse* resp) override {
    return serve(ctx, [&](MasterCore& c) { return c.kill(*req, resp); });
  }
  grpc::Status Drain(grpc::ServerContext* ctx, const v1::DrainRequest* req,
                     v1::DrainResponse* resp) override {
    return serve(ctx, [&](MasterCore& c) { return c.drain(*req, resp); });
  }
  // Answered by every replica, leader or not: it is how an operator (and
  // the bench) sees who leads.
  grpc::Status ReplicaStatus(grpc::ServerContext*, const v1::ReplicaStatusRequest*,
                             v1::ReplicaStatusResponse* resp) override {
    const ReplicaStatusInfo s = r_->status();
    resp->set_id(s.id);
    resp->set_role(to_proto(s.role));
    resp->set_term(s.term);
    resp->set_leader_id(s.leader_id);
    resp->set_leader_address(s.leader_id ? r_->address_of(s.leader_id) : "");
    resp->set_commit_index(s.commit_index);
    resp->set_applied_index(s.applied_index);
    resp->set_last_index(s.last_index);
    resp->set_snapshot_index(s.snapshot_index);
    resp->set_lsn(s.lsn);
    resp->set_serving(s.serving);
    resp->set_isolated(s.isolated);
    resp->set_replicas(static_cast<uint32_t>(r_->options_.members.size()));
    return grpc::Status::OK;
  }

 private:
  template <typename F>
  grpc::Status serve(grpc::ServerContext* ctx, F&& f) {
    std::unique_lock<std::mutex> lock(r_->mu_);
    if (r_->stopping_) return {grpc::StatusCode::UNAVAILABLE, "replica stopping"};
    if (!r_->master_core_) return redirect_locked(ctx);
    const uint64_t term = r_->core_term_;
    const grpc::Status s = guarded([&] { return f(*r_->master_core_); });
    const bool wrote = guarded([&] { return r_->propose_records_locked(); });
    // The reply may depend on anything this leader has appended so far,
    // committed or not (its own records and earlier calls' still in
    // flight), so it waits for all of it; and on this replica still being
    // the leader, which the read round proves (replica_server.h). That
    // holds for errors too: "stale epoch" from a deposed leader could
    // fence a healthy agent.
    const uint64_t need_index = r_->raft_->last_index();
    const uint64_t need_round = r_->raft_->start_read_round(r_->now_ms());
    guarded([&] { r_->process_ready_locked(); });
    if (r_->master_core_ && r_->master_core_->needs_schedule()) r_->tick_cv_.notify_one();

    // Wait no longer than the caller will, less a margin for the reply to
    // travel, so it hears our redirect rather than its own deadline.
    // (In milliseconds: a call without a deadline reports one centuries
    // away, which would overflow a nanosecond duration.)
    auto wait = r_->options_.reply_timeout;
    const auto left = std::chrono::duration_cast<std::chrono::milliseconds>(
                          ctx->deadline() - std::chrono::system_clock::now()) -
                      std::chrono::milliseconds(50);
    if (left < wait) wait = std::max(left, std::chrono::milliseconds(0));
    const auto limit = std::chrono::steady_clock::now() + wait;
    const bool confirmed = r_->commit_cv_.wait_until(lock, limit, [&] {
      return r_->stopping_ || !r_->master_core_ || r_->core_term_ != term ||
             (r_->raft_->commit_index() >= need_index && r_->raft_->confirmed_round() >= need_round);
    });
    if (confirmed && !r_->stopping_ && r_->master_core_ && r_->core_term_ == term) return s;
    // Not confirmed. If the call changed state, those changes are in our
    // log and may still commit (if this replica is re-elected, or a new
    // leader already has them), or vanish: the outcome is unknown.
    const std::string why = !r_->master_core_ || r_->core_term_ != term
                                ? "lost leadership of term " + std::to_string(term)
                                : "could not confirm leadership with a majority in time";
    add_hint_locked(ctx);
    if (wrote) {
      return {grpc::StatusCode::ABORTED, why + "; the request may or may not take effect"};
    }
    return {grpc::StatusCode::UNAVAILABLE, why};
  }

  // Not serving: say who is, if known.
  grpc::Status redirect_locked(grpc::ServerContext* ctx) {
    add_hint_locked(ctx);
    const raft::RaftCore& c = *r_->raft_;
    if (c.role() == raft::Role::kLeader) {
      return {grpc::StatusCode::UNAVAILABLE,
              "leader of term " + std::to_string(c.term()) + " is not serving yet (its no-op is "
              "not committed); retry"};
    }
    if (c.leader_id() == 0) {
      return {grpc::StatusCode::UNAVAILABLE, "no leader known (election in progress); retry"};
    }
    return {grpc::StatusCode::UNAVAILABLE,
            "not the leader; replica " + std::to_string(c.leader_id()) + " at " +
                r_->address_of(c.leader_id()) + " is"};
  }
  void add_hint_locked(grpc::ServerContext* ctx) {
    const uint32_t leader = r_->raft_->leader_id();
    if (leader != 0) ctx->AddTrailingMetadata(kLeaderMetadataKey, r_->address_of(leader));
  }

  ReplicaServer* r_;
};

ReplicaServer::ReplicaServer(ReplicaOptions options) : options_(std::move(options)) {}

ReplicaServer::~ReplicaServer() { shutdown(); }

int64_t ReplicaServer::now_ms() const {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

std::string ReplicaServer::address_of(uint32_t id) const {
  const auto it = options_.members.find(id);
  return it == options_.members.end() ? std::string() : it->second;
}

void ReplicaServer::log(const std::string& line) const {
  if (options_.core.log_events) {
    log_line("fleetmaster[" + std::to_string(options_.id) + "]", line);
  }
}

bool ReplicaServer::start(std::string* error) {
  if (!options_.members.count(options_.id)) {
    if (error) *error = "replica id " + std::to_string(options_.id) + " is not in the member list";
    return false;
  }
  if (options_.state_dir.empty()) {
    if (error) *error = "a replica needs a state directory";
    return false;
  }
  // Recovery before the port opens, as in m4. No other thread exists yet.
  raft::PersistentState persisted;
  raft::RaftRecoveryInfo info;
  std::string err;
  storage_ = raft::RaftStorage::open(options_.state_dir, options_.storage, &persisted, &info, &err);
  if (!storage_) {
    if (error) *error = "recovery failed: " + err;
    return false;
  }
  log("state dir " + options_.state_dir.string() + ": term " + std::to_string(info.term) +
      ", voted for " + std::to_string(info.voted_for) + ", snapshot at index " +
      std::to_string(info.snapshot_index) + ", " + std::to_string(info.entries) +
      " log entries after it, " + std::to_string(info.dropped) + " dropped, torn tail " +
      std::to_string(info.torn_bytes) + " B; loaded in " + std::to_string(info.load_ms) + " ms");

  raft::RaftOptions ro;
  ro.id = options_.id;
  for (const auto& [id, addr] : options_.members) ro.members.push_back(id);
  ro.election_timeout_min_ms = options_.election_timeout_min.count();
  ro.election_timeout_max_ms = options_.election_timeout_max.count();
  ro.heartbeat_interval_ms = options_.raft_heartbeat.count();
  ro.max_entries_per_append = options_.max_entries_per_append;
  // Distinct per process start, so replicas restarted together do not draw
  // the same election timeouts.
  ro.seed = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()) ^
            (static_cast<uint64_t>(options_.id) << 48);
  try {
    raft_ = std::make_unique<raft::RaftCore>(ro, std::move(persisted), now_ms());
  } catch (const std::invalid_argument& e) {
    if (error) *error = e.what();
    return false;
  }
  logged_term_ = raft_->term();

  std::map<uint32_t, std::string> peers = options_.members;
  peers.erase(options_.id);
  raft::RaftHandler* handler = this;  // the base is private: only the transport calls it
  transport_ = std::make_unique<raft::RaftTransport>(options_.id, peers, handler, options_.transport);
  {
    // The first Ready restores the snapshot from disk, if there is one.
    std::lock_guard<std::mutex> lock(mu_);
    process_ready_locked();
  }

  service_ = std::make_unique<MasterService>(this);
  grpc::ServerBuilder builder;
  const std::string listen =
      options_.listen_address.empty() ? options_.members.at(options_.id) : options_.listen_address;
  builder.AddListeningPort(listen, grpc::InsecureServerCredentials(), &port_);
  builder.SetMaxReceiveMessageSize(options_.transport.max_message_bytes);
  builder.SetMaxSendMessageSize(options_.transport.max_message_bytes);
  builder.RegisterService(service_.get());
  builder.RegisterService(transport_->service());
  server_ = builder.BuildAndStart();
  if (!server_ || port_ == 0) {
    if (error) *error = "could not listen on " + listen;
    server_.reset();
    return false;
  }
  transport_->start();
  ticker_ = std::thread([this] { tick_loop(); });
  started_ = true;
  return true;
}

void ReplicaServer::shutdown() {
  {
    std::lock_guard<std::mutex> lock(mu_);
    stopping_ = true;
  }
  // Waiting replies and the tick thread see stopping_ and return, so the
  // gRPC server's shutdown below does not wait out their timeouts.
  commit_cv_.notify_all();
  tick_cv_.notify_all();
  if (server_) {
    server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    server_->Wait();
  }
  if (ticker_.joinable()) ticker_.join();
  if (transport_) transport_->stop();
  server_.reset();
}

// ------------------------------------------------------------ Raft handler

bool ReplicaServer::on_request_vote(const raft::v1::RequestVoteRequest& req,
                                    raft::v1::RequestVoteResponse* resp) {
  std::lock_guard<std::mutex> lock(mu_);
  if (stopping_) return false;
  guarded([&] {
    *resp = raft_->handle_request_vote(req, now_ms());
    process_ready_locked();  // the vote is on disk before the response is sent
  });
  return true;
}

bool ReplicaServer::on_append_entries(const raft::v1::AppendEntriesRequest& req,
                                      raft::v1::AppendEntriesResponse* resp) {
  std::lock_guard<std::mutex> lock(mu_);
  if (stopping_) return false;
  guarded([&] {
    *resp = raft_->handle_append_entries(req, now_ms());
    process_ready_locked();  // the entries are on disk before success is reported
  });
  return true;
}

bool ReplicaServer::on_install_snapshot(const raft::v1::InstallSnapshotRequest& req,
                                        raft::v1::InstallSnapshotResponse* resp) {
  std::lock_guard<std::mutex> lock(mu_);
  if (stopping_) return false;
  guarded([&] {
    *resp = raft_->handle_install_snapshot(req, now_ms());
    process_ready_locked();
  });
  return true;
}

void ReplicaServer::on_request_vote_response(uint32_t from,
                                             const raft::v1::RequestVoteResponse& resp) {
  std::lock_guard<std::mutex> lock(mu_);
  if (stopping_) return;
  guarded([&] {
    raft_->handle_request_vote_response(from, resp, now_ms());
    process_ready_locked();
  });
}

void ReplicaServer::on_append_entries_response(uint32_t from,
                                               const raft::v1::AppendEntriesResponse& resp) {
  std::lock_guard<std::mutex> lock(mu_);
  if (stopping_) return;
  guarded([&] {
    raft_->handle_append_entries_response(from, resp, now_ms());
    process_ready_locked();
  });
}

void ReplicaServer::on_install_snapshot_response(uint32_t from,
                                                 const raft::v1::InstallSnapshotResponse& resp) {
  std::lock_guard<std::mutex> lock(mu_);
  if (stopping_) return;
  guarded([&] {
    raft_->handle_install_snapshot_response(from, resp, now_ms());
    process_ready_locked();
  });
}

// ------------------------------------------------------------ the driver

void ReplicaServer::process_ready_locked() {
  while (true) {
    raft::Ready rd = raft_->take_ready();
    if (rd.empty()) break;
    // 1. Persist. Holding the lock across the sync, as m4 did: nothing
    //    else may see the core's new state until it is durable.
    std::string err;
    if (!storage_->persist(rd, &err)) fatal("raft storage: " + err);
    // 2. Send.
    for (raft::OutMessage& m : rd.messages) transport_->send(std::move(m));
    // 3. Apply.
    if (rd.restore) restore_locked(*rd.restore);
    for (const raft::v1::Entry& e : rd.committed) apply_locked(e);
    maybe_compact_locked();  // may make another Ready: loop
  }
  update_leadership_locked();
  commit_cv_.notify_all();
}

void ReplicaServer::apply_locked(const raft::v1::Entry& e) {
  if (e.index() != applied_index_ + 1) {
    fatal("applying raft entry " + std::to_string(e.index()) + " after " +
          std::to_string(applied_index_));
  }
  applied_index_ = e.index();
  if (e.type() != raft::v1::Entry::TYPE_COMMAND) return;  // the no-op
  v1::WalRecord rec;
  if (!rec.ParseFromString(e.command())) {
    fatal("committed raft entry " + std::to_string(e.index()) + " is not a WalRecord");
  }
  // The same apply() the leader ran when it decided. A refusal means
  // replicas would diverge (the state machine is not deterministic, or a
  // record was lost or reordered): stop rather than run on a state no
  // other replica has.
  if (std::string why = applied_.apply(rec); !why.empty()) {
    fatal("committed raft entry " + std::to_string(e.index()) + " does not apply: " + why);
  }
}

void ReplicaServer::restore_locked(const raft::SnapshotRef& s) {
  v1::Snapshot snap;
  if (!s.data || !snap.ParseFromString(*s.data)) {
    fatal("raft snapshot at index " + std::to_string(s.index) + " is not a fleet snapshot");
  }
  std::string err;
  std::optional<MasterState> st = MasterState::from_snapshot(snap, &err);
  if (!st) fatal("raft snapshot at index " + std::to_string(s.index) + ": " + err);
  applied_ = std::move(*st);
  applied_index_ = s.index;
  log("state machine restored from snapshot at index " + std::to_string(s.index) + " (lsn " +
      std::to_string(applied_.lsn()) + ", " + std::to_string(s.data->size()) + " bytes)");
}

void ReplicaServer::maybe_compact_locked() {
  if (options_.snapshot_every == 0 ||
      applied_index_ < raft_->snapshot_index() + options_.snapshot_every) {
    return;
  }
  // From applied_, never from the leader's speculative state: a snapshot
  // must hold committed entries only.
  v1::Snapshot snap;
  applied_.to_snapshot(&snap);
  auto data = std::make_shared<const std::string>(snap.SerializeAsString());
  raft_->compact(applied_index_, data);
  log("log compacted: snapshot at index " + std::to_string(applied_index_) + " (lsn " +
      std::to_string(applied_.lsn()) + ", " + std::to_string(data->size()) + " bytes)");
}

void ReplicaServer::update_leadership_locked() {
  const raft::RaftCore& c = *raft_;
  if (c.role() != logged_role_ || c.term() != logged_term_ || c.leader_id() != logged_leader_) {
    if (c.role() == raft::Role::kLeader && logged_role_ != raft::Role::kLeader) {
      log("became leader of term " + std::to_string(c.term()) + " (log at index " +
          std::to_string(c.last_index()) + ", commit " + std::to_string(c.commit_index()) + ")");
    } else if (c.role() == raft::Role::kCandidate) {
      log("election timeout: candidate for term " + std::to_string(c.term()));
    } else if (c.role() == raft::Role::kFollower && c.leader_id() != 0 &&
               (c.leader_id() != logged_leader_ || logged_role_ != raft::Role::kFollower)) {
      log("follower of replica " + std::to_string(c.leader_id()) + " in term " +
          std::to_string(c.term()));
    } else if (logged_role_ == raft::Role::kLeader && c.role() != raft::Role::kLeader) {
      log("stepped down: saw term " + std::to_string(c.term()));
    }
    logged_role_ = c.role();
    logged_term_ = c.term();
    logged_leader_ = c.leader_id();
  }
  const bool leader = c.role() == raft::Role::kLeader;
  if (master_core_ && (!leader || c.term() != core_term_)) {
    // Deposed. Its uncommitted decisions go with it; whatever of them a
    // majority has, the next leader commits through the log.
    master_core_.reset();
    log("no longer serving (leader of term " + std::to_string(core_term_) +
        "); discarded the state not yet committed");
  }
  if (leader && !master_core_ && applied_index_ >= c.term_start_index()) {
    // The no-op of this term is committed and applied: applied_ now holds
    // every entry any earlier leader committed. Serve from a copy of it,
    // in the report window (m4), since heartbeat history did not
    // replicate.
    MasterState copy = applied_;
    master_core_ = applied_.lsn() > 0
                       ? std::make_unique<MasterCore>(options_.core, std::move(copy), Clock::now())
                       : std::make_unique<MasterCore>(options_.core);
    core_term_ = c.term();
    log("serving as leader of term " + std::to_string(core_term_) + ": no-op at index " +
        std::to_string(c.term_start_index()) + " committed, state at lsn " +
        std::to_string(applied_.lsn()));
    tick_cv_.notify_one();
  }
}

bool ReplicaServer::propose_records_locked() {
  std::vector<v1::WalRecord> records = master_core_->take_records();
  if (records.empty()) return false;
  std::vector<std::string> commands;
  commands.reserve(records.size());
  for (const v1::WalRecord& r : records) commands.push_back(r.SerializeAsString());
  // master_core_ exists only while the core is the leader of core_term_
  // (every role change runs update_leadership_locked under this lock), so
  // the proposal cannot be refused.
  if (!raft_->propose(std::move(commands), now_ms())) fatal("proposal refused by the leader");
  return true;
}

void ReplicaServer::tick_loop() {
  std::unique_lock<std::mutex> lock(mu_);
  auto next_raft = std::chrono::steady_clock::now();
  auto next_master = next_raft;
  while (!stopping_) {
    // Only a serving leader has a master tick to wait for. (Waiting on a
    // master deadline that never advances made this loop spin with the
    // lock held, starving every handler: the first smoke test caught it.)
    const auto wake = master_core_ ? std::min(next_raft, next_master) : next_raft;
    tick_cv_.wait_until(lock, wake, [this] {
      return stopping_ || (master_core_ && master_core_->needs_schedule());
    });
    if (stopping_) break;
    const auto now = std::chrono::steady_clock::now();
    if (!master_core_) next_master = now;  // a new leader's core ticks at once
    if (now >= next_raft) {
      guarded([&] {
        raft_->tick(now_ms());
        process_ready_locked();
      });
      next_raft = now + options_.raft_tick;
    }
    if (master_core_ && (now >= next_master || master_core_->needs_schedule())) {
      // As in m4: failure detection, backoff timers, a scheduling pass,
      // under the lock. Its records are proposed; nothing waits on them
      // here, since nothing is sent to anyone because of them until they
      // commit (replies wait, and placements reach agents only in replies).
      guarded([&] {
        master_core_->tick(Clock::now());
        propose_records_locked();
        process_ready_locked();
      });
      next_master = now + options_.tick_interval;
    }
  }
}

// ------------------------------------------------------------ inspection

ReplicaStatusInfo ReplicaServer::status() {
  std::lock_guard<std::mutex> lock(mu_);
  ReplicaStatusInfo s;
  s.id = options_.id;
  if (!raft_) return s;
  s.role = raft_->role();
  s.term = raft_->term();
  s.leader_id = raft_->leader_id();
  s.commit_index = raft_->commit_index();
  s.applied_index = applied_index_;
  s.last_index = raft_->last_index();
  s.snapshot_index = raft_->snapshot_index();
  s.lsn = applied_.lsn();
  s.serving = master_core_ != nullptr;
  s.isolated = isolated_;
  return s;
}

void ReplicaServer::set_isolated(bool isolated) {
  std::set<uint32_t> peers;
  if (isolated) {
    for (const auto& [id, addr] : options_.members) {
      if (id != options_.id) peers.insert(id);
    }
  }
  {
    std::lock_guard<std::mutex> lock(mu_);
    isolated_ = isolated;
  }
  if (transport_) transport_->set_blocked(std::move(peers));
  log(isolated ? "ISOLATED from every other replica (fault injection)"
               : "isolation lifted (fault injection)");
}

std::string ReplicaServer::applied_state(uint64_t* applied_index) {
  std::lock_guard<std::mutex> lock(mu_);
  v1::Snapshot snap;
  applied_.to_snapshot(&snap);
  if (applied_index) *applied_index = applied_index_;
  return canonical_bytes(snap);
}

std::string ReplicaServer::check_leader_matches_applied() {
  std::lock_guard<std::mutex> lock(mu_);
  if (!master_core_ || raft_->commit_index() != raft_->last_index()) return {};
  const std::string inv = master_core_->check_invariants();
  if (!inv.empty()) return "leader invariants: " + inv;
  v1::Snapshot a, b;
  applied_.to_snapshot(&a);
  master_core_->durable_state().to_snapshot(&b);
  if (canonical_bytes(a) != canonical_bytes(b)) {
    return "leader's state (lsn " + std::to_string(b.lsn()) + ") differs from its applied state (lsn " +
           std::to_string(a.lsn()) + ") with nothing uncommitted";
  }
  return {};
}

raft::RaftStorageStats ReplicaServer::storage_stats() {
  std::lock_guard<std::mutex> lock(mu_);
  return storage_ ? storage_->stats() : raft::RaftStorageStats{};
}

}  // namespace fleet
