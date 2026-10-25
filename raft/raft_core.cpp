// RaftCore implementation. Each block quotes the Figure 2 (or Figure 13)
// rule it implements, so the code can be read against the paper line by
// line. See raft_core.h for the input/output contract.

#include "raft_core.h"

#include <algorithm>
#include <functional>
#include <stdexcept>

namespace fleet::raft {

const char* to_string(Role r) {
  switch (r) {
    case Role::kFollower: return "follower";
    case Role::kCandidate: return "candidate";
    case Role::kLeader: return "leader";
  }
  return "?";
}

RaftCore::RaftCore(RaftOptions options, PersistentState state, int64_t now_ms)
    : options_(std::move(options)),
      // The id is mixed in so that servers given the same seed (the
      // simulation gives each run one seed) still draw different timeouts.
      rng_(options_.seed * 0x9E3779B97F4A7C15ull + options_.id),
      current_term_(state.current_term),
      voted_for_(state.voted_for),
      log_(std::move(state.entries)),
      snapshot_(std::move(state.snapshot)) {
  if (options_.id == 0 || !is_member(options_.id)) {
    throw std::invalid_argument("raft: id must be a nonzero member");
  }
  std::set<uint32_t> seen;
  for (uint32_t m : options_.members) {
    if (m == 0 || !seen.insert(m).second) throw std::invalid_argument("raft: bad member list");
  }
  if (options_.election_timeout_min_ms <= options_.heartbeat_interval_ms ||
      options_.election_timeout_max_ms < options_.election_timeout_min_ms) {
    throw std::invalid_argument("raft: need heartbeat < election timeout min <= max");
  }
  for (size_t k = 0; k < log_.size(); ++k) {
    if (log_[k].index() != snapshot_.index + 1 + k) {
      throw std::invalid_argument("raft: persisted entries are not consecutive after the snapshot");
    }
  }
  for (uint32_t m : options_.members) {
    if (m != options_.id) progress_[m] = Progress{};
  }
  // Volatile state starts where the snapshot ends: everything in it was
  // committed and applied; the rest of the log is relearned from the
  // leader (commitIndex is not persisted, Figure 2).
  commit_index_ = snapshot_.index;
  applied_ = snapshot_.index;
  if (snapshot_.index > 0) pending_.restore = snapshot_;
  reset_election_timer(now_ms);
}

bool RaftCore::is_member(uint32_t id) const {
  return std::find(options_.members.begin(), options_.members.end(), id) !=
         options_.members.end();
}

uint64_t RaftCore::term_at(uint64_t index) const {
  if (index == snapshot_.index) return snapshot_.term;
  if (index < snapshot_.index || index > last_index()) return 0;
  return entry(index).term();
}

void RaftCore::reset_election_timer(int64_t now_ms) {
  const auto span = static_cast<uint64_t>(options_.election_timeout_max_ms -
                                          options_.election_timeout_min_ms + 1);
  election_deadline_ms_ =
      now_ms + options_.election_timeout_min_ms + static_cast<int64_t>(rng_() % span);
}

// ------------------------------------------------------------ log edits

void RaftCore::mark_unstable(uint64_t index) {
  if (!unstable_from_ || index < *unstable_from_) unstable_from_ = index;
}

void RaftCore::append_entry(v1::Entry e) {
  if (e.index() != last_index() + 1) throw std::logic_error("raft: append out of order");
  mark_unstable(e.index());
  log_.push_back(std::move(e));
}

void RaftCore::truncate_from(uint64_t index) {
  // Never below the commit index: a committed entry is on a majority and
  // in every future leader's log (Leader Completeness), so a leader asking
  // to overwrite one means the safety argument has been broken somewhere.
  if (index <= commit_index_) {
    throw std::logic_error("raft: asked to delete committed entry " + std::to_string(index) +
                           " (commit index " + std::to_string(commit_index_) + ")");
  }
  log_.resize(index - snapshot_.index - 1);
  mark_unstable(index);
}

// ------------------------------------------------------------ roles

void RaftCore::step_down(uint64_t term, int64_t now_ms) {
  // "If RPC request or response contains term T > currentTerm: set
  // currentTerm = T, convert to follower."
  if (term > current_term_) {
    current_term_ = term;
    voted_for_ = 0;
    hard_state_dirty_ = true;
    leader_id_ = 0;
  }
  if (role_ == Role::kLeader) reset_election_timer(now_ms);
  role_ = Role::kFollower;
  votes_.clear();
}

void RaftCore::become_candidate(int64_t now_ms) {
  // "On conversion to candidate, start election: increment currentTerm,
  // vote for self, reset election timer, send RequestVote RPCs to all
  // other servers."
  role_ = Role::kCandidate;
  ++current_term_;
  voted_for_ = options_.id;
  hard_state_dirty_ = true;
  leader_id_ = 0;
  votes_ = {options_.id};
  reset_election_timer(now_ms);
  if (votes_.size() >= majority()) {  // a one-server cluster
    become_leader(now_ms);
    return;
  }
  for (const auto& [peer, p] : progress_) {
    v1::RequestVoteRequest req;
    req.set_term(current_term_);
    req.set_candidate_id(options_.id);
    req.set_last_log_index(last_index());
    req.set_last_log_term(last_term());
    pending_.messages.push_back(OutMessage{peer, std::move(req)});
  }
}

void RaftCore::become_leader(int64_t now_ms) {
  role_ = Role::kLeader;
  leader_id_ = options_.id;
  votes_.clear();
  // "Reinitialized after election": nextIndex to leader last log index +
  // 1, matchIndex to 0.
  for (auto& [peer, p] : progress_) p = Progress{last_index() + 1, 0, false, 0, false, 0};
  // The no-op (paper section 8): an entry of this term, so that as soon as
  // it commits, everything before it is committed too (Figure 8: earlier
  // terms' entries can only be committed indirectly, by committing one of
  // the current term). It is also how a new leader learns its commit
  // index, which it needs before it can serve reads.
  v1::Entry noop;
  noop.set_term(current_term_);
  noop.set_index(last_index() + 1);
  noop.set_type(v1::Entry::TYPE_NOOP);
  term_start_index_ = noop.index();
  append_entry(std::move(noop));
  // "Upon election: send initial empty AppendEntries RPCs (heartbeat) to
  // each server; repeat during idle periods to prevent election timeouts."
  // Here the first one already carries the no-op.
  broadcast_append(now_ms);
  heartbeat_deadline_ms_ = now_ms + options_.heartbeat_interval_ms;
  maybe_advance_commit();  // a one-server cluster commits at once
}

void RaftCore::tick(int64_t now_ms) {
  if (role_ == Role::kLeader) {
    if (now_ms >= heartbeat_deadline_ms_) {
      // Every heartbeat carries whatever the follower is missing, so it is
      // also the retransmission of any AppendEntries the network lost.
      broadcast_append(now_ms);
      heartbeat_deadline_ms_ = now_ms + options_.heartbeat_interval_ms;
    }
    return;
  }
  // "Followers: if election timeout elapses without receiving
  // AppendEntries RPC from current leader or granting vote to candidate:
  // convert to candidate." "Candidates: if election timeout elapses:
  // start new election."
  if (now_ms >= election_deadline_ms_) become_candidate(now_ms);
}

// ------------------------------------------------------------ RequestVote

v1::RequestVoteResponse RaftCore::handle_request_vote(const v1::RequestVoteRequest& req,
                                                      int64_t now_ms) {
  if (req.term() > current_term_) step_down(req.term(), now_ms);
  v1::RequestVoteResponse resp;
  // "1. Reply false if term < currentTerm."
  // "2. If votedFor is null or candidateId, and candidate's log is at
  // least as up-to-date as receiver's log, grant vote." Up-to-date (5.4.1):
  // the later last term wins; with equal last terms, the longer log.
  const bool log_ok = req.last_log_term() > last_term() ||
                      (req.last_log_term() == last_term() && req.last_log_index() >= last_index());
  const bool grant = req.term() == current_term_ && is_member(req.candidate_id()) &&
                     (voted_for_ == 0 || voted_for_ == req.candidate_id()) && log_ok;
  if (grant) {
    voted_for_ = req.candidate_id();
    hard_state_dirty_ = true;  // persisted before this response is sent
    reset_election_timer(now_ms);
  }
  resp.set_term(current_term_);
  resp.set_vote_granted(grant);
  return resp;
}

void RaftCore::handle_request_vote_response(uint32_t from, const v1::RequestVoteResponse& resp,
                                            int64_t now_ms) {
  if (resp.term() > current_term_) {
    step_down(resp.term(), now_ms);
    return;
  }
  // A grant counts only for the election it answers: the voter moved to
  // our term before granting, so a grant from an earlier election carries
  // that earlier term and is ignored. A set, so a duplicate counts once.
  if (role_ != Role::kCandidate || resp.term() != current_term_ || !resp.vote_granted() ||
      !progress_.count(from)) {
    return;
  }
  votes_.insert(from);
  // "If votes received from majority of servers: become leader."
  if (votes_.size() >= majority()) become_leader(now_ms);
}

// ------------------------------------------------------------ AppendEntries

void RaftCore::send_append(uint32_t peer, int64_t now_ms) {
  Progress& p = progress_.at(peer);
  if (p.next <= snapshot_.index) {
    // The entries this follower needs next were compacted away: only the
    // snapshot can bring it up to date (Figure 13). At most one per
    // snapshot_retry_ms while unacknowledged; in between, a heartbeat at
    // the snapshot's index keeps its election timer quiet.
    if (!p.snapshot_sent || now_ms - p.snapshot_sent_ms >= options_.snapshot_retry_ms) {
      v1::InstallSnapshotRequest req;
      req.set_term(current_term_);
      req.set_leader_id(options_.id);
      req.set_last_included_index(snapshot_.index);
      req.set_last_included_term(snapshot_.term);
      req.set_data(*snapshot_.data);
      req.set_round(round_);
      p.snapshot_sent = true;
      p.snapshot_sent_ms = now_ms;
      p.inflight = true;
      pending_.messages.push_back(OutMessage{peer, std::move(req)});
      return;
    }
  }
  // "If last log index >= nextIndex for a follower: send AppendEntries RPC
  // with log entries starting at nextIndex."
  const uint64_t prev = std::max(p.next, snapshot_.index + 1) - 1;
  v1::AppendEntriesRequest req;
  req.set_term(current_term_);
  req.set_leader_id(options_.id);
  req.set_prev_log_index(prev);
  req.set_prev_log_term(term_at(prev));
  req.set_leader_commit(commit_index_);
  req.set_round(round_);
  const uint64_t end = std::min(last_index(), prev + options_.max_entries_per_append);
  for (uint64_t i = prev + 1; i <= end; ++i) *req.add_entries() = entry(i);
  p.inflight = true;
  pending_.messages.push_back(OutMessage{peer, std::move(req)});
}

void RaftCore::broadcast_append(int64_t now_ms) {
  for (auto& [peer, p] : progress_) send_append(peer, now_ms);
}

v1::AppendEntriesResponse RaftCore::handle_append_entries(const v1::AppendEntriesRequest& req,
                                                          int64_t now_ms) {
  v1::AppendEntriesResponse resp;
  resp.set_request_term(req.term());
  resp.set_round(req.round());
  // "1. Reply false if term < currentTerm."
  if (req.term() < current_term_) {
    resp.set_term(current_term_);
    resp.set_success(false);
    return resp;
  }
  if (req.term() == current_term_ && role_ == Role::kLeader) {
    // Election Safety says at most one leader per term.
    throw std::logic_error("raft: two leaders in term " + std::to_string(current_term_));
  }
  // A current leader exists: a candidate of the same term steps down
  // ("If AppendEntries RPC received from new leader: convert to
  // follower"), a higher term is adopted.
  if (req.term() > current_term_ || role_ != Role::kFollower) step_down(req.term(), now_ms);
  leader_id_ = req.leader_id();
  reset_election_timer(now_ms);
  resp.set_term(current_term_);
  resp.set_success(false);

  const uint64_t prev = req.prev_log_index();
  // Well-formed: entries consecutive after prev, terms not decreasing and
  // not above the leader's. Our own leader never sends anything else; a
  // malformed request is refused with no hint (conflict_index 0).
  uint64_t expect = prev + 1;
  uint64_t last_t = req.prev_log_term();
  for (const v1::Entry& e : req.entries()) {
    if (e.index() != expect++ || e.term() < last_t || e.term() > req.term()) return resp;
    last_t = e.term();
  }

  // "2. Reply false if log doesn't contain an entry at prevLogIndex whose
  // term matches prevLogTerm." With the conflict-index shortcut (5.3).
  if (prev > last_index()) {
    resp.set_conflict_index(last_index() + 1);
    return resp;
  }
  if (prev >= snapshot_.index && term_at(prev) != req.prev_log_term()) {
    if (prev == snapshot_.index) {
      // Our snapshot holds only committed entries, which every leader has.
      throw std::logic_error("raft: leader's log conflicts with a committed snapshot entry");
    }
    const uint64_t t = term_at(prev);
    uint64_t first = prev;
    while (first - 1 > snapshot_.index && term_at(first - 1) == t) --first;
    resp.set_conflict_index(first);
    return resp;
  }
  // (prev < snapshot_.index: everything through the snapshot is committed,
  // so it matches the leader's log; the entries at or below it are skipped
  // below.)

  // "3. If an existing entry conflicts with a new one (same index but
  // different terms), delete the existing entry and all that follow it.
  // 4. Append any new entries not already in the log." Only an actual
  // conflict truncates: a late or duplicated request carrying fewer
  // entries than we have must not cut off entries we already acknowledged.
  for (const v1::Entry& e : req.entries()) {
    if (e.index() <= snapshot_.index) continue;
    if (e.index() <= last_index()) {
      if (term_at(e.index()) == e.term()) continue;
      truncate_from(e.index());
    }
    append_entry(e);
  }
  const uint64_t last_new = prev + static_cast<uint64_t>(req.entries_size());
  // "5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit,
  // index of last new entry)."
  if (req.leader_commit() > commit_index_) commit_to(std::min(req.leader_commit(), last_new));
  resp.set_success(true);
  resp.set_match_index(std::max(last_new, snapshot_.index));
  return resp;
}

void RaftCore::handle_append_entries_response(uint32_t from, const v1::AppendEntriesResponse& resp,
                                              int64_t now_ms) {
  if (resp.term() > current_term_) {
    step_down(resp.term(), now_ms);
    return;
  }
  if (role_ != Role::kLeader || resp.request_term() != current_term_ || !progress_.count(from)) {
    return;  // not leader any more, or a reply to an earlier term
  }
  Progress& p = progress_.at(from);
  // It answered a request of our term without rejecting it: as of that
  // request's round, this follower still recognized us.
  p.acked_round = std::max(p.acked_round, resp.round());
  p.inflight = false;
  if (resp.success()) {
    if (resp.match_index() > last_index()) return;  // malformed
    // "If successful: update nextIndex and matchIndex for follower."
    // max(): a reordered, older success must not move them back.
    p.match = std::max(p.match, resp.match_index());
    p.next = std::max(p.next, p.match + 1);
    maybe_advance_commit();
    if (p.next <= last_index()) send_append(from, now_ms);
    return;
  }
  if (resp.conflict_index() == 0) return;  // refused as malformed: no hint
  // "If AppendEntries fails because of log inconsistency: decrement
  // nextIndex and retry", jumping straight to the follower's hint. Never
  // below match + 1 (known to match) and never up (a stale failure only
  // means it once lacked something).
  p.next = std::max(p.match + 1, std::min(p.next, resp.conflict_index()));
  // A follower that needs the snapshot, which is already on its way: wait
  // for it (or for the retry at a heartbeat). Sending now would be another
  // AppendEntries at the snapshot index, rejected again, and the two would
  // bounce rejections back and forth until the snapshot lands.
  if (p.next <= snapshot_.index && p.snapshot_sent &&
      now_ms - p.snapshot_sent_ms < options_.snapshot_retry_ms) {
    return;
  }
  send_append(from, now_ms);
}

void RaftCore::maybe_advance_commit() {
  if (role_ != Role::kLeader) return;
  // "If there exists an N such that N > commitIndex, a majority of
  // matchIndex[i] >= N, and log[N].term == currentTerm: set commitIndex =
  // N." The majority-th highest match index (ours is our last index) is
  // the largest N a majority has. Terms only grow along the log, so if
  // that entry is from an earlier term, so is every entry below it: none
  // can be committed by counting (Figure 8), and we wait for an entry of
  // our own term (the no-op at least) to reach a majority.
  std::vector<uint64_t> m{last_index()};
  for (const auto& [peer, p] : progress_) m.push_back(p.match);
  std::sort(m.begin(), m.end(), std::greater<>());
  const uint64_t n = m[majority() - 1];
  if (n > commit_index_ && term_at(n) == current_term_) commit_to(n);
}

void RaftCore::commit_to(uint64_t index) {
  if (index <= commit_index_) return;
  if (index > last_index()) throw std::logic_error("raft: commit beyond the log");
  commit_index_ = index;
  // "If commitIndex > lastApplied: increment lastApplied, apply
  // log[lastApplied] to state machine."
  for (uint64_t i = applied_ + 1; i <= commit_index_; ++i) pending_.committed.push_back(entry(i));
  applied_ = commit_index_;
}

// ------------------------------------------------------------ InstallSnapshot

v1::InstallSnapshotResponse RaftCore::handle_install_snapshot(
    const v1::InstallSnapshotRequest& req, int64_t now_ms) {
  v1::InstallSnapshotResponse resp;
  resp.set_request_term(req.term());
  resp.set_round(req.round());
  resp.set_last_included_index(req.last_included_index());
  // Figure 13: "1. Reply immediately if term < currentTerm."
  if (req.term() < current_term_) {
    resp.set_term(current_term_);
    return resp;
  }
  if (req.term() == current_term_ && role_ == Role::kLeader) {
    throw std::logic_error("raft: two leaders in term " + std::to_string(current_term_));
  }
  if (req.term() > current_term_ || role_ != Role::kFollower) step_down(req.term(), now_ms);
  leader_id_ = req.leader_id();
  reset_election_timer(now_ms);
  resp.set_term(current_term_);

  const uint64_t idx = req.last_included_index();
  // Nothing to do if we already have (and committed) everything it
  // covers; replacing a newer state machine with an older one would undo
  // applied entries.
  if (idx <= commit_index_) return resp;
  // "6. If existing log entry has same index and term as snapshot's last
  // included entry, retain log entries following it and reply. 7. Discard
  // the entire log. 8. Reset state machine using snapshot contents."
  if (idx <= last_index() && term_at(idx) == req.last_included_term()) {
    log_.erase(log_.begin(), log_.begin() + static_cast<std::ptrdiff_t>(idx - snapshot_.index));
  } else {
    log_.clear();
  }
  snapshot_ = SnapshotRef{idx, req.last_included_term(),
                          std::make_shared<const std::string>(req.data())};
  snapshot_dirty_ = true;
  commit_index_ = idx;
  applied_ = idx;
  // Entries queued for applying are all at or below the old commit index,
  // so the snapshot already contains them.
  pending_.committed.clear();
  pending_.restore = snapshot_;
  return resp;
}

void RaftCore::handle_install_snapshot_response(uint32_t from,
                                                const v1::InstallSnapshotResponse& resp,
                                                int64_t now_ms) {
  if (resp.term() > current_term_) {
    step_down(resp.term(), now_ms);
    return;
  }
  if (role_ != Role::kLeader || resp.request_term() != current_term_ || !progress_.count(from) ||
      resp.last_included_index() > last_index()) {
    return;
  }
  Progress& p = progress_.at(from);
  p.acked_round = std::max(p.acked_round, resp.round());
  p.inflight = false;
  p.snapshot_sent = false;
  // It now has everything through the snapshot (or had it already).
  p.match = std::max(p.match, resp.last_included_index());
  p.next = std::max(p.next, p.match + 1);
  maybe_advance_commit();
  if (p.next <= last_index()) send_append(from, now_ms);
}

// ------------------------------------------------------------ driver API

std::optional<uint64_t> RaftCore::propose(std::vector<std::string> commands, int64_t now_ms) {
  if (role_ != Role::kLeader) return std::nullopt;
  // "If command received from client: append entry to local log, respond
  // after entry applied to state machine."
  for (std::string& c : commands) {
    v1::Entry e;
    e.set_term(current_term_);
    e.set_index(last_index() + 1);
    e.set_type(v1::Entry::TYPE_COMMAND);
    e.set_command(std::move(c));
    append_entry(std::move(e));
  }
  for (auto& [peer, p] : progress_) {
    if (!p.inflight) send_append(peer, now_ms);
  }
  maybe_advance_commit();  // a one-server cluster
  return last_index();
}

uint64_t RaftCore::start_read_round(int64_t now_ms) {
  if (role_ != Role::kLeader) return 0;
  ++round_;
  // Sent now rather than at the next heartbeat, regardless of in-flight
  // requests, so a read waits one round trip, not up to a heartbeat.
  broadcast_append(now_ms);
  return round_;
}

uint64_t RaftCore::confirmed_round() const {
  if (role_ != Role::kLeader) return 0;
  std::vector<uint64_t> r{round_};  // this server always agrees with itself
  for (const auto& [peer, p] : progress_) r.push_back(p.acked_round);
  std::sort(r.begin(), r.end(), std::greater<>());
  return r[majority() - 1];
}

void RaftCore::compact(uint64_t index, std::shared_ptr<const std::string> data) {
  if (index <= snapshot_.index || index > applied_) {
    throw std::invalid_argument("raft: compact index must be above the snapshot and applied");
  }
  const uint64_t term = term_at(index);
  log_.erase(log_.begin(), log_.begin() + static_cast<std::ptrdiff_t>(index - snapshot_.index));
  snapshot_ = SnapshotRef{index, term, std::move(data)};
  snapshot_dirty_ = true;
}

Ready RaftCore::take_ready() {
  Ready r = std::move(pending_);
  pending_ = Ready{};
  if (hard_state_dirty_) {
    v1::HardState hs;
    hs.set_current_term(current_term_);
    hs.set_voted_for(voted_for_);
    r.hard_state = hs;
    hard_state_dirty_ = false;
  }
  if (snapshot_dirty_) {
    // A new snapshot changes which prefix the log file holds: hand over
    // the whole remaining log, which the storage writes in place of the
    // old one. Rare (once per many entries, or a lagging follower), so
    // rewriting is simpler than describing the edit.
    r.snapshot = snapshot_;
    r.entries = log_;
    snapshot_dirty_ = false;
  } else if (unstable_from_) {
    r.truncate_from = *unstable_from_;
    for (uint64_t i = std::max(*unstable_from_, snapshot_.index + 1); i <= last_index(); ++i) {
      r.entries.push_back(entry(i));
    }
  }
  unstable_from_.reset();
  return r;
}

}  // namespace fleet::raft
