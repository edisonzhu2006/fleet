#pragma once
// RaftCore: Raft (Ongaro and Ousterhout, 2014) as a pure state machine:
// messages and clock readings in; messages, things to persist and
// committed entries out. No threads, no sockets, no files, no clock (m4b).
//
// WHY A PURE CORE. Consensus bugs live in interleavings: a vote that
// arrives after a timeout, a reply from an earlier term, a duplicated
// AppendEntries that would truncate a log if handled naively. A core that
// only reacts to explicit inputs (each with an explicit `now_ms`) can be
// driven by a deterministic simulation (tests/raft_sim_test.cpp: a fake
// network that drops, delays, duplicates and partitions; a fake clock;
// crashes and restarts; thousands of seeds) that checks Raft's safety
// properties after every step and replays any failure from its seed. The
// gRPC transport (raft_transport.h) and the disk (raft_storage.h) are thin
// layers that feed it inputs and carry out its outputs. This is the shape
// of etcd's raft library, for the same reason.
//
// FIGURE 2, EXACTLY. Persistent state: currentTerm, votedFor, log[].
// Volatile: commitIndex, lastApplied; on leaders nextIndex[], matchIndex[].
// RequestVote and AppendEntries receivers, and the rules for servers, are
// implemented as the figure states them; comments in raft_core.cpp quote
// the rule each block implements. On top of the figure, only what the
// paper itself adds: randomized election timeouts (5.2), the leader's
// no-op at the start of its term (8), snapshots with InstallSnapshot (7,
// Figure 13), the conflict-index shortcut for finding nextIndex (end of
// 5.3), and ReadIndex rounds (dissertation 6.4) so the leader can answer
// clients without serving a stale view. Not implemented: membership
// changes (the member set is fixed at start), pre-vote, leadership
// transfer, check-quorum. NOTES.md m4b says what each would add.
//
// THE OUTPUT CONTRACT (take_ready()). After every input, the driver calls
// take_ready() and carries out the Ready in this order:
//   1. persist hard_state, then the snapshot (and the log it implies),
//      then the log changes, and make them durable (fsync);
//   2. send `messages`;
//   3. apply: install `restore` into the state machine if set, then apply
//      `committed` in order.
// A response returned by a handle_*() call is also only sent after step 1
// of the Ready that follows it. That is Figure 2's "updated on stable
// storage before responding to RPCs": a vote or an acknowledged entry is
// never forgotten by a crash. Because the driver persists before it sends
// anything, the core may treat its own log as durable the moment it
// appends: a leader counts itself toward the majority for its new entries
// without waiting, since no other server can learn of them before they
// are on its disk.
//
// TIME. Every input carries now_ms from a clock that never goes backwards
// (the steady clock in production, a counter in the simulation). The core
// reads no clock itself, so the simulation controls all timing.
//
// ERRORS. A situation Raft's safety argument says cannot happen (two
// leaders in one term, an entry at or below the commit index being
// overwritten, a committed snapshot entry conflicting with the leader)
// throws std::logic_error: continuing would be worse than stopping, and
// the simulation turns it into a failing seed. Malformed input from a peer
// is refused without throwing.
//
// NOT THREAD-SAFE: the driver serializes every call (one mutex).

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <random>
#include <set>
#include <string>
#include <variant>
#include <vector>

#include "raft.pb.h"

namespace fleet::raft {

struct RaftOptions {
  uint32_t id = 0;                 // this server, 1..N
  std::vector<uint32_t> members;   // every server's id, this one included
  // Randomized election timeout (paper 5.2): each follower waits a
  // uniformly random time in [min, max] without hearing from a leader
  // before it starts an election, so in most elections one server times
  // out first and wins before the others wake. The paper's suggestion is
  // 150 to 300 ms; it needs heartbeat_interval << min.
  int64_t election_timeout_min_ms = 150;
  int64_t election_timeout_max_ms = 300;
  int64_t heartbeat_interval_ms = 50;
  // Cap on entries per AppendEntries, so catching up a far-behind follower
  // is a series of bounded messages rather than one huge one.
  size_t max_entries_per_append = 256;
  // A follower that needs a snapshot gets it at most this often while one
  // is unacknowledged (a snapshot is MB; heartbeats carry on meanwhile).
  int64_t snapshot_retry_ms = 1000;
  uint64_t seed = 1;  // election-timeout randomness; the simulation fixes it
};

// A snapshot of the state machine through `index` (whose entry had
// `term`). The bytes are opaque to Raft (a serialized fleet.v1.Snapshot in
// fleet) and shared, because the leader keeps them to send to followers.
struct SnapshotRef {
  uint64_t index = 0;  // 0: no snapshot
  uint64_t term = 0;
  std::shared_ptr<const std::string> data;
};

// What the core is rebuilt from after a restart (RaftStorage::open).
struct PersistentState {
  uint64_t current_term = 0;
  uint32_t voted_for = 0;  // 0: none
  SnapshotRef snapshot;
  std::vector<v1::Entry> entries;  // consecutive, from snapshot.index + 1
};

enum class Role : uint8_t { kFollower, kCandidate, kLeader };
const char* to_string(Role r);

// A request to send to another server. Responses are not messages: each
// handle_*() returns its response, and the driver sends it back (after
// persisting).
struct OutMessage {
  uint32_t to = 0;
  std::variant<v1::RequestVoteRequest, v1::AppendEntriesRequest, v1::InstallSnapshotRequest> body;
};

// Everything one or more inputs produced. See THE OUTPUT CONTRACT above.
struct Ready {
  // 1. Persist, before anything else in this Ready (or a response
  //    returned since the last take_ready) leaves the process.
  std::optional<v1::HardState> hard_state;
  // A new snapshot (this server compacted, or installed the leader's):
  // write it, then replace the whole persisted log with `entries`.
  std::optional<SnapshotRef> snapshot;
  // Without a snapshot: delete persisted entries with index >=
  // truncate_from (if set), then append `entries`.
  std::optional<uint64_t> truncate_from;
  std::vector<v1::Entry> entries;
  // 2. Send.
  std::vector<OutMessage> messages;
  // 3. Apply: replace the state machine with `restore` (if set), then
  //    apply `committed` in index order.
  std::optional<SnapshotRef> restore;
  std::vector<v1::Entry> committed;

  bool empty() const {
    return !hard_state && !snapshot && !truncate_from && entries.empty() && messages.empty() &&
           !restore && committed.empty();
  }
};

class RaftCore {
 public:
  // Starts as a follower with a fresh election timer. If `state` has a
  // snapshot, the first Ready carries it in `restore`, so the driver
  // rebuilds its state machine through the same path as an install.
  RaftCore(RaftOptions options, PersistentState state, int64_t now_ms);

  // Election and heartbeat timers. Call often (every few ms); it does
  // nothing unless a timer is due.
  void tick(int64_t now_ms);

  // Requests from other servers. Each returns the response to send back
  // once the following Ready's persistence is done.
  v1::RequestVoteResponse handle_request_vote(const v1::RequestVoteRequest& req, int64_t now_ms);
  v1::AppendEntriesResponse handle_append_entries(const v1::AppendEntriesRequest& req,
                                                  int64_t now_ms);
  v1::InstallSnapshotResponse handle_install_snapshot(const v1::InstallSnapshotRequest& req,
                                                      int64_t now_ms);
  // Responses to requests this server sent. Any of them may be late,
  // duplicated or from an earlier term; each handler copes.
  void handle_request_vote_response(uint32_t from, const v1::RequestVoteResponse& resp,
                                    int64_t now_ms);
  void handle_append_entries_response(uint32_t from, const v1::AppendEntriesResponse& resp,
                                      int64_t now_ms);
  void handle_install_snapshot_response(uint32_t from, const v1::InstallSnapshotResponse& resp,
                                        int64_t now_ms);

  // Leader only: appends the commands to the log (one entry each) and
  // starts replicating them. Returns the index of the last one, or nullopt
  // if this server is not the leader (nothing is appended).
  std::optional<uint64_t> propose(std::vector<std::string> commands, int64_t now_ms);

  // ReadIndex (dissertation 6.4). Leader only: starts a new read round and
  // sends AppendEntries carrying it to every follower at once. A reply
  // computed now may be released when confirmed_round() >= the returned
  // round (a majority, this server included, has answered an
  // AppendEntries sent after this call without rejecting this leader's
  // term) and commit_index() covers everything the reply depends on. Then
  // no other leader can have been elected before the call, so the reply
  // is not stale. 0 if not leader.
  uint64_t start_read_round(int64_t now_ms);
  uint64_t confirmed_round() const;

  // The state machine has been snapshotted through `index` (which must be
  // above snapshot_index() and at most applied_index()). Drops the log
  // prefix; the next Ready persists the snapshot.
  void compact(uint64_t index, std::shared_ptr<const std::string> data);

  // The outputs accumulated since the last call. See the class comment.
  Ready take_ready();

  // Inspection (the driver, fleetctl status, and the tests).
  uint32_t id() const { return options_.id; }
  Role role() const { return role_; }
  uint64_t term() const { return current_term_; }
  uint32_t voted_for() const { return voted_for_; }
  uint32_t leader_id() const { return leader_id_; }  // 0: unknown
  uint64_t commit_index() const { return commit_index_; }
  // Highest index handed out in a Ready (committed, or covered by a
  // restored snapshot): Figure 2's lastApplied, from the core's side.
  uint64_t applied_index() const { return applied_; }
  uint64_t last_index() const { return snapshot_.index + log_.size(); }
  uint64_t last_term() const { return term_at(last_index()); }
  uint64_t snapshot_index() const { return snapshot_.index; }
  uint64_t snapshot_term() const { return snapshot_.term; }
  const SnapshotRef& snapshot() const { return snapshot_; }
  // The log after the snapshot: entries()[k] has index snapshot_index()+1+k.
  const std::vector<v1::Entry>& entries() const { return log_; }
  // Term of the entry at index, for snapshot_index() <= index <=
  // last_index() (0 at index 0); 0 outside that range.
  uint64_t term_at(uint64_t index) const;
  // Leader: index of the no-op that opened its term. The driver serves
  // clients only once this is applied (see raft.proto Entry.TYPE_NOOP).
  uint64_t term_start_index() const { return term_start_index_; }
  uint64_t match_index(uint32_t peer) const { return progress_.at(peer).match; }
  uint64_t next_index(uint32_t peer) const { return progress_.at(peer).next; }

 private:
  // A leader's view of one follower (Figure 2's nextIndex and matchIndex,
  // plus flow control and the read-round acknowledgment).
  struct Progress {
    uint64_t next = 1;
    uint64_t match = 0;
    // An AppendEntries or InstallSnapshot is outstanding: new proposals
    // wait for its response (or the next heartbeat) instead of each
    // sending their own copy of the log tail.
    bool inflight = false;
    int64_t snapshot_sent_ms = 0;
    bool snapshot_sent = false;
    uint64_t acked_round = 0;
  };

  size_t majority() const { return options_.members.size() / 2 + 1; }
  bool is_member(uint32_t id) const;
  const v1::Entry& entry(uint64_t index) const { return log_[index - snapshot_.index - 1]; }

  void reset_election_timer(int64_t now_ms);
  // A higher term was seen (or a candidate lost to a leader of its term):
  // become a follower. Updates the term and clears the vote when `term`
  // is higher. Leaves the election timer alone unless this server was the
  // leader (which has none running): a vote request from a server with a
  // stale log must not keep postponing everyone's elections.
  void step_down(uint64_t term, int64_t now_ms);
  void become_candidate(int64_t now_ms);
  void become_leader(int64_t now_ms);

  void send_append(uint32_t peer, int64_t now_ms);
  void broadcast_append(int64_t now_ms);
  void maybe_advance_commit();
  void commit_to(uint64_t index);

  void append_entry(v1::Entry e);
  void truncate_from(uint64_t index);
  void mark_unstable(uint64_t index);

  RaftOptions options_;
  std::mt19937_64 rng_;

  // Figure 2, persistent on all servers.
  uint64_t current_term_ = 0;
  uint32_t voted_for_ = 0;
  std::vector<v1::Entry> log_;  // after snapshot_
  SnapshotRef snapshot_;

  // Figure 2, volatile on all servers.
  uint64_t commit_index_ = 0;
  uint64_t applied_ = 0;

  Role role_ = Role::kFollower;
  uint32_t leader_id_ = 0;
  int64_t election_deadline_ms_ = 0;
  std::set<uint32_t> votes_;  // candidate: who granted

  // Leader.
  std::map<uint32_t, Progress> progress_;  // every other member
  int64_t heartbeat_deadline_ms_ = 0;
  uint64_t term_start_index_ = 0;
  uint64_t round_ = 0;

  // Not yet handed out by take_ready().
  Ready pending_;
  bool hard_state_dirty_ = false;
  bool snapshot_dirty_ = false;
  std::optional<uint64_t> unstable_from_;  // lowest log index changed
};

}  // namespace fleet::raft
