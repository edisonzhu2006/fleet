// RaftTransport implementation: the Raft gRPC service (incoming calls to
// the replica's handler) and one sender thread per peer (outgoing calls,
// responses back to the handler). See raft_transport.h.

#include "raft_transport.h"

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

namespace fleet::raft {

class RaftTransport::Service final : public v1::Raft::Service {
 public:
  explicit Service(RaftTransport* t) : t_(t) {}

  grpc::Status RequestVote(grpc::ServerContext*, const v1::RequestVoteRequest* req,
                           v1::RequestVoteResponse* resp) override {
    if (t_->blocked(req->candidate_id())) return blocked();
    return t_->handler_->on_request_vote(*req, resp) ? grpc::Status::OK : stopping();
  }
  grpc::Status AppendEntries(grpc::ServerContext*, const v1::AppendEntriesRequest* req,
                             v1::AppendEntriesResponse* resp) override {
    if (t_->blocked(req->leader_id())) return blocked();
    return t_->handler_->on_append_entries(*req, resp) ? grpc::Status::OK : stopping();
  }
  grpc::Status InstallSnapshot(grpc::ServerContext*, const v1::InstallSnapshotRequest* req,
                               v1::InstallSnapshotResponse* resp) override {
    if (t_->blocked(req->leader_id())) return blocked();
    return t_->handler_->on_install_snapshot(*req, resp) ? grpc::Status::OK : stopping();
  }

 private:
  static grpc::Status blocked() {
    return {grpc::StatusCode::UNAVAILABLE, "partitioned from this peer (fault injection)"};
  }
  static grpc::Status stopping() { return {grpc::StatusCode::UNAVAILABLE, "replica stopping"}; }
  RaftTransport* t_;
};

RaftTransport::RaftTransport(uint32_t self, std::map<uint32_t, std::string> peers,
                             RaftHandler* handler, RaftTransportOptions options)
    : self_(self), handler_(handler), options_(options), service_(std::make_unique<Service>(this)) {
  for (auto& [id, address] : peers) {
    if (id == self_) continue;
    auto p = std::make_unique<Peer>();
    p->id = id;
    p->address = address;
    grpc::ChannelArguments args;
    // gRPC backs off reconnects up to 120 s by default. A replica that
    // was down and restarts must hear the leader within its election
    // timeout (150 ms at the minimum), or it starts an election and
    // deposes a healthy leader. So reconnect every 20 to 100 ms.
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 20);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 20);
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 100);
    args.SetMaxReceiveMessageSize(options_.max_message_bytes);
    args.SetMaxSendMessageSize(options_.max_message_bytes);
    p->stub = v1::Raft::NewStub(
        grpc::CreateCustomChannel(address, grpc::InsecureChannelCredentials(), args));
    peers_.emplace(id, std::move(p));
  }
}

RaftTransport::~RaftTransport() { stop(); }

grpc::Service* RaftTransport::service() { return service_.get(); }

void RaftTransport::start() {
  if (started_) return;
  started_ = true;
  for (auto& [id, p] : peers_) {
    Peer* raw = p.get();
    p->thread = std::thread([this, raw] { run_peer(*raw); });
  }
}

void RaftTransport::stop() {
  for (auto& [id, p] : peers_) {
    {
      std::lock_guard<std::mutex> lock(p->mu);
      p->stop = true;
      p->queue.clear();
    }
    p->cv.notify_all();
  }
  for (auto& [id, p] : peers_) {
    if (p->thread.joinable()) p->thread.join();
  }
}

void RaftTransport::send(OutMessage m) {
  auto it = peers_.find(m.to);
  if (it == peers_.end()) return;
  Peer& p = *it->second;
  {
    std::lock_guard<std::mutex> lock(p.mu);
    if (p.stop) return;
    if (p.queue.size() >= options_.max_queue) {
      p.queue.pop_front();
      std::lock_guard<std::mutex> l2(mu_);
      ++stats_.overflow;
    }
    p.queue.push_back(std::move(m));
  }
  p.cv.notify_one();
}

void RaftTransport::set_blocked(std::set<uint32_t> peers) {
  std::lock_guard<std::mutex> lock(mu_);
  blocked_ = std::move(peers);
}

bool RaftTransport::blocked(uint32_t peer) const {
  std::lock_guard<std::mutex> lock(mu_);
  return blocked_.count(peer) > 0;
}

RaftTransportStats RaftTransport::stats() const {
  std::lock_guard<std::mutex> lock(mu_);
  return stats_;
}

void RaftTransport::run_peer(Peer& p) {
  while (true) {
    OutMessage m;
    {
      std::unique_lock<std::mutex> lock(p.mu);
      p.cv.wait(lock, [&] { return p.stop || !p.queue.empty(); });
      if (p.stop) return;
      m = std::move(p.queue.front());
      p.queue.pop_front();
    }
    deliver(p, m);
  }
}

void RaftTransport::deliver(Peer& p, OutMessage& m) {
  if (blocked(p.id)) {
    std::lock_guard<std::mutex> lock(mu_);
    ++stats_.blocked;
    return;
  }
  grpc::ClientContext ctx;
  const bool is_snapshot = std::holds_alternative<v1::InstallSnapshotRequest>(m.body);
  ctx.set_deadline(std::chrono::system_clock::now() +
                   (is_snapshot ? options_.snapshot_timeout : options_.rpc_timeout));
  grpc::Status s;
  // The response goes to the handler only if the call succeeded and the
  // peer is still not blocked (a partition that began mid-call drops the
  // reply too).
  if (auto* rv = std::get_if<v1::RequestVoteRequest>(&m.body)) {
    v1::RequestVoteResponse resp;
    s = p.stub->RequestVote(&ctx, *rv, &resp);
    if (s.ok() && !blocked(p.id)) handler_->on_request_vote_response(p.id, resp);
  } else if (auto* ae = std::get_if<v1::AppendEntriesRequest>(&m.body)) {
    v1::AppendEntriesResponse resp;
    s = p.stub->AppendEntries(&ctx, *ae, &resp);
    if (s.ok() && !blocked(p.id)) handler_->on_append_entries_response(p.id, resp);
  } else if (auto* is = std::get_if<v1::InstallSnapshotRequest>(&m.body)) {
    v1::InstallSnapshotResponse resp;
    s = p.stub->InstallSnapshot(&ctx, *is, &resp);
    if (s.ok() && !blocked(p.id)) handler_->on_install_snapshot_response(p.id, resp);
  }
  std::lock_guard<std::mutex> lock(mu_);
  if (s.ok()) {
    ++stats_.sent;
  } else {
    ++stats_.failed;
  }
}

}  // namespace fleet::raft
