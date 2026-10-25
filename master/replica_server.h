#pragma once
// ReplicaServer: one fleetmaster replica of a Raft-replicated control plane
// (m4b). Every state change the master decides is a Raft log entry; every
// replica applies the committed entries, in order, to its own copy of the
// master's state; only the leader decides, schedules and answers agents
// and fleetctl.
//
// WHY THIS EXISTS. m4's master survives its own crash, but not losing its
// machine or disk. With 3 replicas that agree on every change through
// Raft, losing any one loses nothing, the way Borgmaster runs 5
// Paxos-replicated replicas. The m4 pieces carry over unchanged:
// MasterCore still decides and turns each decision into a v1::WalRecord,
// and MasterState::apply is still the one deterministic function that
// changes durable state. What changed is where a record goes: into the
// Raft log (proto/raft.proto Entry, one record per entry) instead of a
// local file, and "durable" now means "committed on a majority".
//
// TWO COPIES OF THE STATE ON THE LEADER, ONE ON A FOLLOWER.
//   applied_      every replica: MasterState with exactly the committed
//                 entries applied, in log order. The replicated state
//                 machine. Snapshots for log compaction come from it.
//   master_core_  the leader only: a MasterCore built from a copy of
//                 applied_ when this replica's no-op commits. Like m4's
//                 master it applies its own decisions at once (the
//                 scheduler must see its earlier placements within a pass),
//                 so it runs ahead of the log by whatever is uncommitted.
// When the replica stops being the leader, master_core_ is thrown away,
// uncommitted speculation and all; whatever of it the next leader commits
// arrives through the log like any other entry. Speculating and
// discarding is safe because nothing that depends on an uncommitted entry
// ever leaves the process (next paragraph).
//
// THE ORDERING RULE, REPLICATED. m4's rule was "no reply before the
// records behind it are durable". Here: a reply to an agent or fleetctl
// is sent only once (1) the log is committed through everything the
// leader had appended when the reply was computed (the reply may depend
// on any of it, not just its own call's records), and (2) a majority has
// acknowledged an AppendEntries sent after the call arrived (ReadIndex,
// raft_core.h), which proves no newer leader existed then. A replica cut
// off from the others therefore cannot answer at all: its calls time out
// or fail, and the agents and fleetctl move to the replica that can. Reads
// (List) go through the same path: leader-only, linearizable.
//
// A NEW LEADER (Raft and m4 together). Raft's leader appends a no-op;
// this replica builds master_core_ only once that no-op is committed and
// applied, so its state includes every entry any earlier leader
// committed. master_core_ then starts in m4's report window: it accepts
// heartbeats but schedules nothing until every live node has reported (or
// the window ends), and reconciles each node's first report against the
// log exactly as after a restart. Heartbeat history is soft state and did
// not replicate, so every node starts suspect with a fresh silence clock.
//
// THREADS. One mutex (mu_) guards the Raft core, the storage, both states
// and the waiting replies. gRPC server threads run the Master service
// (agents, fleetctl) and the Raft service (peers); the transport's sender
// threads deliver peer responses; one tick thread drives Raft's timers
// (every raft_tick) and MasterCore::tick (every tick_interval, or at once
// when a call made scheduling work). Every input to the Raft core is
// followed, under the same lock, by process_ready_locked(): persist (fsync),
// hand messages to the transport, apply committed entries, build or drop
// master_core_. Replies wait on a condition variable, outside the lock.

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <grpcpp/server.h>

#include "master_core.h"
#include "master_state.h"
#include "raft_core.h"
#include "raft_storage.h"
#include "raft_transport.h"

namespace fleet {

struct ReplicaOptions {
  uint32_t id = 0;
  // Every replica, this one included: id -> address. The member set is
  // fixed (no membership changes; NOTES.md m4b).
  std::map<uint32_t, std::string> members;
  // Where this replica listens (Master and Raft services share the port).
  // Empty: members[id].
  std::string listen_address;
  std::filesystem::path state_dir;  // required: raft_state, raft_log, raft_snapshot
  raft::RaftStorageOptions storage;
  // Raft timing (raft_core.h). Elections need heartbeat << election min.
  std::chrono::milliseconds election_timeout_min{150};
  std::chrono::milliseconds election_timeout_max{300};
  std::chrono::milliseconds raft_heartbeat{50};
  // How often the Raft core's timers are checked.
  std::chrono::milliseconds raft_tick{10};
  // MasterCore's tick (failure detection, backoff timers, scheduling), as
  // in m2-m4.
  std::chrono::milliseconds tick_interval{100};
  // Longest a reply waits for its commit and its read round (also capped
  // by the caller's deadline, less a margin, so the caller gets our
  // UNAVAILABLE or ABORTED with a hint instead of its own deadline error).
  std::chrono::milliseconds reply_timeout{1000};
  // Compact the log into a snapshot of applied_ every this many applied
  // entries (0: never).
  uint64_t snapshot_every = 10000;
  size_t max_entries_per_append = 256;
  MasterOptions core;
  raft::RaftTransportOptions transport;
};

struct ReplicaStatusInfo {
  uint32_t id = 0;
  raft::Role role = raft::Role::kFollower;
  uint64_t term = 0;
  uint32_t leader_id = 0;
  uint64_t commit_index = 0;
  uint64_t applied_index = 0;
  uint64_t last_index = 0;
  uint64_t snapshot_index = 0;
  uint64_t lsn = 0;       // MasterState LSN of applied_
  bool serving = false;   // leader with its no-op applied
  bool isolated = false;  // fault injection
};

class ReplicaServer final : private raft::RaftHandler {
 public:
  explicit ReplicaServer(ReplicaOptions options);
  ~ReplicaServer() override;  // shutdown()

  ReplicaServer(const ReplicaServer&) = delete;
  ReplicaServer& operator=(const ReplicaServer&) = delete;

  // Recovers the Raft state from state_dir, then serves. False with
  // *error if the files are corrupt or the address cannot be bound.
  bool start(std::string* error);
  // Stops serving, fails waiting replies, joins every thread. Idempotent.
  // Writes nothing: for the files, a shutdown is a crash.
  void shutdown();
  int port() const { return port_; }

  ReplicaStatusInfo status();
  // Fault injection (the partition test): drop all Raft traffic to and
  // from every other replica while true.
  void set_isolated(bool isolated);

  // Tests: the applied state as a canonical snapshot serialization, and
  // the index it reflects, so replicas can be compared byte for byte.
  std::string applied_state(uint64_t* applied_index);
  // Tests: on a serving leader with nothing uncommitted, MasterCore's
  // speculative state must equal applied_ exactly. "" if so (or not
  // applicable), else why not.
  std::string check_leader_matches_applied();
  raft::RaftStorageStats storage_stats();

 private:
  class MasterService;

  // raft::RaftHandler: every one locks, feeds the core, processes the
  // Ready, and (for requests) returns only after it is persisted.
  bool on_request_vote(const raft::v1::RequestVoteRequest& req,
                       raft::v1::RequestVoteResponse* resp) override;
  bool on_append_entries(const raft::v1::AppendEntriesRequest& req,
                         raft::v1::AppendEntriesResponse* resp) override;
  bool on_install_snapshot(const raft::v1::InstallSnapshotRequest& req,
                           raft::v1::InstallSnapshotResponse* resp) override;
  void on_request_vote_response(uint32_t from, const raft::v1::RequestVoteResponse& resp) override;
  void on_append_entries_response(uint32_t from,
                                  const raft::v1::AppendEntriesResponse& resp) override;
  void on_install_snapshot_response(uint32_t from,
                                    const raft::v1::InstallSnapshotResponse& resp) override;

  // Carries out the core's Readies until none is left (see THREADS), then
  // brings master_core_ in line with the role. Aborts on a disk error or
  // on a committed record that does not apply: the first is fsyncgate
  // (m4), the second means the replicated state machine is not
  // deterministic, and running on would diverge.
  void process_ready_locked();
  void apply_locked(const raft::v1::Entry& e);
  void restore_locked(const raft::SnapshotRef& s);
  void maybe_compact_locked();
  void update_leadership_locked();
  // MasterCore's new records into the Raft log. True if there were any.
  bool propose_records_locked();
  void tick_loop();
  int64_t now_ms() const;
  std::string address_of(uint32_t id) const;
  void log(const std::string& line) const;

  ReplicaOptions options_;
  std::mutex mu_;
  std::condition_variable tick_cv_;    // wakes the tick thread
  std::condition_variable commit_cv_;  // wakes replies waiting for a commit or a round
  std::unique_ptr<raft::RaftStorage> storage_;
  std::unique_ptr<raft::RaftCore> raft_;
  std::unique_ptr<raft::RaftTransport> transport_;
  MasterState applied_;               // committed entries only
  uint64_t applied_index_ = 0;        // Raft index applied_ reflects
  std::unique_ptr<MasterCore> master_core_;  // leader only, built after its no-op applies
  uint64_t core_term_ = 0;            // the term master_core_ was built in
  bool isolated_ = false;
  bool stopping_ = false;
  // For log lines on role changes.
  raft::Role logged_role_ = raft::Role::kFollower;
  uint64_t logged_term_ = 0;
  uint32_t logged_leader_ = 0;
  std::unique_ptr<MasterService> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread ticker_;
  int port_ = 0;
  bool started_ = false;
};

}  // namespace fleet
