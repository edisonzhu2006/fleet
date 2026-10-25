#pragma once
// RaftTransport: carries RaftCore's messages between replicas as gRPC
// calls (m4b). The thin layer around the pure core.
//
// WHY THIS EXISTS. RaftCore emits requests (OutMessage) and returns
// responses; it never touches a socket. This class turns each OutMessage
// into a unary gRPC call to the peer's Raft service (proto/raft.proto) and
// hands the response back to the replica, and on the receiving side turns
// each incoming call into a handle_*() on the replica. Raft assumes the
// network may lose, delay, duplicate and reorder messages, so the
// transport promises nothing: a failed call is dropped (the leader's next
// heartbeat retransmits), and calls are not retried here.
//
// ONE SENDER THREAD PER PEER, with its own queue, so a slow or dead peer
// delays only its own messages. Calls to one peer are made one at a time,
// in order; the core keeps at most one AppendEntries in flight per peer
// besides heartbeats, so the queue stays short. If it overflows (a peer
// that answers slowly for a long time), the oldest message is dropped:
// Raft treats that as loss.
//
// FAULT INJECTION (the partition test). set_blocked(ids) makes this
// replica drop every message to those peers and refuse every call from
// them (UNAVAILABLE), which from both sides looks like a network
// partition between them. fleetmaster toggles it on SIGUSR1/SIGUSR2.
//
// THREADING. send() and set_blocked() may be called from any thread.
// Handler callbacks run on gRPC server threads (requests) and on the
// sender threads (responses); the replica serializes them with its lock.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include <grpcpp/impl/service_type.h>

#include "raft.grpc.pb.h"
#include "raft_core.h"

namespace fleet::raft {

// What the replica does with traffic. Request handlers return false to
// refuse the call (the replica is shutting down); the transport then
// answers UNAVAILABLE. A request handler must persist whatever its
// response depends on before returning (see RaftCore's output contract).
class RaftHandler {
 public:
  virtual ~RaftHandler() = default;
  virtual bool on_request_vote(const v1::RequestVoteRequest& req,
                               v1::RequestVoteResponse* resp) = 0;
  virtual bool on_append_entries(const v1::AppendEntriesRequest& req,
                                 v1::AppendEntriesResponse* resp) = 0;
  virtual bool on_install_snapshot(const v1::InstallSnapshotRequest& req,
                                   v1::InstallSnapshotResponse* resp) = 0;
  virtual void on_request_vote_response(uint32_t from, const v1::RequestVoteResponse& resp) = 0;
  virtual void on_append_entries_response(uint32_t from,
                                          const v1::AppendEntriesResponse& resp) = 0;
  virtual void on_install_snapshot_response(uint32_t from,
                                            const v1::InstallSnapshotResponse& resp) = 0;
};

struct RaftTransportOptions {
  // Deadline per call. Well under the election timeout's scale would be
  // wrong for snapshots (MB), so they get their own.
  std::chrono::milliseconds rpc_timeout{500};
  std::chrono::milliseconds snapshot_timeout{10000};
  size_t max_queue = 256;
  // gRPC's default receive limit is 4 MB; snapshots are sent whole.
  int max_message_bytes = 64 << 20;
};

struct RaftTransportStats {
  uint64_t sent = 0;     // calls that returned a response
  uint64_t failed = 0;   // calls that failed (peer down, deadline)
  uint64_t blocked = 0;  // dropped by set_blocked
  uint64_t overflow = 0; // dropped because a peer's queue was full
};

class RaftTransport {
 public:
  // peers: every other replica's id and address (not this one's).
  RaftTransport(uint32_t self, std::map<uint32_t, std::string> peers, RaftHandler* handler,
                RaftTransportOptions options = {});
  ~RaftTransport();  // stop()

  RaftTransport(const RaftTransport&) = delete;
  RaftTransport& operator=(const RaftTransport&) = delete;

  // The Raft service, to register on the replica's gRPC server.
  grpc::Service* service();
  void start();  // starts the sender threads
  void stop();   // stops and joins them; queued messages are dropped. Idempotent.

  void send(OutMessage m);
  void set_blocked(std::set<uint32_t> peers);
  bool blocked(uint32_t peer) const;
  RaftTransportStats stats() const;

 private:
  class Service;
  struct Peer {
    uint32_t id = 0;
    std::string address;
    std::unique_ptr<v1::Raft::Stub> stub;
    std::mutex mu;
    std::condition_variable cv;
    std::deque<OutMessage> queue;  // guarded by mu
    bool stop = false;             // guarded by mu
    std::thread thread;
  };
  void run_peer(Peer& p);
  void deliver(Peer& p, OutMessage& m);

  uint32_t self_;
  RaftHandler* handler_;
  RaftTransportOptions options_;
  std::map<uint32_t, std::unique_ptr<Peer>> peers_;
  std::unique_ptr<Service> service_;
  mutable std::mutex mu_;          // guards blocked_ and stats_
  std::set<uint32_t> blocked_;
  RaftTransportStats stats_;
  bool started_ = false;
};

}  // namespace fleet::raft
