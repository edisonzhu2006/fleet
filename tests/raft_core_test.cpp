// Unit tests for RaftCore (m4b): one Figure 2 rule per test, driven by
// hand with an explicit clock, so each rule can be read next to the paper.
// The randomized, all-rules-at-once version is raft_sim_test.cpp.

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "raft_core.h"

namespace fleet::raft {
namespace {

RaftOptions opts(uint32_t id, std::vector<uint32_t> members) {
  RaftOptions o;
  o.id = id;
  o.members = std::move(members);
  o.election_timeout_min_ms = 150;
  o.election_timeout_max_ms = 300;
  o.heartbeat_interval_ms = 50;
  o.seed = 7;
  return o;
}

v1::Entry entry(uint64_t term, uint64_t index, const std::string& cmd = "x") {
  v1::Entry e;
  e.set_term(term);
  e.set_index(index);
  e.set_type(v1::Entry::TYPE_COMMAND);
  e.set_command(cmd);
  return e;
}

PersistentState with_log(uint64_t term, std::vector<v1::Entry> entries) {
  PersistentState s;
  s.current_term = term;
  s.entries = std::move(entries);
  return s;
}

template <typename T>
std::vector<T> messages_of(const Ready& r) {
  std::vector<T> out;
  for (const OutMessage& m : r.messages) {
    if (const T* p = std::get_if<T>(&m.body)) out.push_back(*p);
  }
  return out;
}

// Makes `c` (a 3-server member) leader of the next term by timing out and
// receiving one vote. Returns the Ready of the election.
Ready elect(RaftCore& c, uint32_t voter, int64_t now = 1000) {
  c.tick(now);
  EXPECT_EQ(c.role(), Role::kCandidate);
  v1::RequestVoteResponse yes;
  yes.set_term(c.term());
  yes.set_vote_granted(true);
  c.handle_request_vote_response(voter, yes, now);
  EXPECT_EQ(c.role(), Role::kLeader);
  return c.take_ready();
}

v1::AppendEntriesResponse ack(const RaftCore& leader, uint64_t match, uint64_t round = 0) {
  v1::AppendEntriesResponse r;
  r.set_term(leader.term());
  r.set_request_term(leader.term());
  r.set_success(true);
  r.set_match_index(match);
  r.set_round(round);
  return r;
}

TEST(RaftCore, SingleServerElectsItselfAndCommitsAtOnce) {
  RaftCore c(opts(1, {1}), {}, 0);
  c.tick(299);
  c.tick(301);  // past any timeout in [150, 300]
  ASSERT_EQ(c.role(), Role::kLeader);
  Ready r = c.take_ready();
  ASSERT_TRUE(r.hard_state);
  EXPECT_EQ(r.hard_state->current_term(), 1u);
  EXPECT_EQ(r.hard_state->voted_for(), 1u);
  ASSERT_EQ(r.entries.size(), 1u);
  EXPECT_EQ(r.entries[0].type(), v1::Entry::TYPE_NOOP);  // the term's no-op
  ASSERT_EQ(r.committed.size(), 1u);
  const auto idx = c.propose({"a", "b"}, 302);
  ASSERT_TRUE(idx);
  EXPECT_EQ(*idx, 3u);
  r = c.take_ready();
  EXPECT_EQ(r.entries.size(), 2u);
  EXPECT_EQ(r.committed.size(), 2u);
  EXPECT_EQ(c.commit_index(), 3u);
  EXPECT_EQ(c.start_read_round(303), c.confirmed_round());  // a majority of one
}

TEST(RaftCore, ElectionTimeoutStartsAnElectionThatIsPersistedBeforeVotesGoOut) {
  RaftCore c(opts(1, {1, 2, 3}), {}, 0);
  c.tick(149);
  EXPECT_EQ(c.role(), Role::kFollower);  // the timeout is at least 150 ms
  c.tick(301);
  ASSERT_EQ(c.role(), Role::kCandidate);
  EXPECT_EQ(c.term(), 1u);
  EXPECT_EQ(c.voted_for(), 1u);
  Ready r = c.take_ready();
  // The vote for itself is in the same Ready as the requests, and the
  // driver persists before it sends.
  ASSERT_TRUE(r.hard_state);
  EXPECT_EQ(r.hard_state->voted_for(), 1u);
  const auto rv = messages_of<v1::RequestVoteRequest>(r);
  ASSERT_EQ(rv.size(), 2u);
  EXPECT_EQ(rv[0].term(), 1u);
  EXPECT_EQ(rv[0].candidate_id(), 1u);
}

TEST(RaftCore, GrantsOneVotePerTermAndPersistsItWithTheResponse) {
  RaftCore c(opts(3, {1, 2, 3}), {}, 0);
  v1::RequestVoteRequest a;
  a.set_term(5);
  a.set_candidate_id(1);
  EXPECT_TRUE(c.handle_request_vote(a, 10).vote_granted());
  Ready r = c.take_ready();
  ASSERT_TRUE(r.hard_state);
  EXPECT_EQ(r.hard_state->current_term(), 5u);
  EXPECT_EQ(r.hard_state->voted_for(), 1u);
  EXPECT_TRUE(c.handle_request_vote(a, 11).vote_granted());  // same candidate again: yes
  v1::RequestVoteRequest b = a;
  b.set_candidate_id(2);
  EXPECT_FALSE(c.handle_request_vote(b, 12).vote_granted());  // another, same term: no
  b.set_term(4);
  const auto resp = c.handle_request_vote(b, 13);
  EXPECT_FALSE(resp.vote_granted());  // an older term: no, and learn ours
  EXPECT_EQ(resp.term(), 5u);
  b.set_term(6);
  EXPECT_TRUE(c.handle_request_vote(b, 14).vote_granted());  // a new term: a new vote
}

TEST(RaftCore, AVoteInATermAlreadyAdoptedIsPersistedToo) {
  // The vote is usually persisted along with the term change it comes
  // with. Here the term arrives first (a refused candidate with a stale
  // log), and the vote later, on its own: it must still be written, or a
  // crash and restart would let this server vote again in term 5.
  RaftCore c(opts(3, {1, 2, 3}), with_log(1, {entry(1, 1)}), 0);
  v1::RequestVoteRequest stale;
  stale.set_term(5);
  stale.set_candidate_id(1);  // last log term 0: less up to date
  EXPECT_FALSE(c.handle_request_vote(stale, 1).vote_granted());
  Ready r = c.take_ready();
  ASSERT_TRUE(r.hard_state);
  EXPECT_EQ(r.hard_state->voted_for(), 0u);
  v1::RequestVoteRequest good = stale;
  good.set_candidate_id(2);
  good.set_last_log_term(1);
  good.set_last_log_index(1);
  EXPECT_TRUE(c.handle_request_vote(good, 2).vote_granted());
  r = c.take_ready();
  ASSERT_TRUE(r.hard_state);
  EXPECT_EQ(r.hard_state->current_term(), 5u);
  EXPECT_EQ(r.hard_state->voted_for(), 2u);
}

TEST(RaftCore, RefusesCandidatesWithLessUpToDateLogs) {
  // Our log: (1,1) (2,2). Last term 2, last index 2.
  RaftCore c(opts(3, {1, 2, 3}), with_log(2, {entry(1, 1), entry(2, 2)}), 0);
  v1::RequestVoteRequest req;
  req.set_term(3);
  req.set_candidate_id(1);
  req.set_last_log_term(1);
  req.set_last_log_index(9);  // longer, but an older last term
  EXPECT_FALSE(c.handle_request_vote(req, 1).vote_granted());
  req.set_term(4);
  req.set_last_log_term(2);
  req.set_last_log_index(1);  // same last term, shorter
  EXPECT_FALSE(c.handle_request_vote(req, 2).vote_granted());
  req.set_term(5);
  req.set_last_log_index(2);  // same last term, same length: as up to date
  EXPECT_TRUE(c.handle_request_vote(req, 3).vote_granted());
}

TEST(RaftCore, NewLeaderAppendsANoOpAndCommitsItWithOneFollower) {
  RaftCore c(opts(1, {1, 2, 3}), {}, 0);
  Ready r = elect(c, 2);
  ASSERT_EQ(r.entries.size(), 1u);
  EXPECT_EQ(r.entries[0].type(), v1::Entry::TYPE_NOOP);
  EXPECT_EQ(c.term_start_index(), 1u);
  const auto ae = messages_of<v1::AppendEntriesRequest>(r);
  ASSERT_EQ(ae.size(), 2u);  // heartbeats carrying the no-op, to both followers
  EXPECT_EQ(ae[0].entries_size(), 1);
  EXPECT_EQ(c.commit_index(), 0u);
  c.handle_append_entries_response(2, ack(c, 1), 1001);
  EXPECT_EQ(c.commit_index(), 1u);  // leader + one follower = majority of 3
}

TEST(RaftCore, RejectsAppendWithAConflictHint) {
  // Follower log: (1,1) (1,2) (2,3) (2,4).
  RaftCore c(opts(2, {1, 2, 3}),
             with_log(2, {entry(1, 1), entry(1, 2), entry(2, 3), entry(2, 4)}), 0);
  v1::AppendEntriesRequest req;
  req.set_term(3);
  req.set_leader_id(1);
  req.set_prev_log_index(7);  // beyond our log
  req.set_prev_log_term(3);
  auto resp = c.handle_append_entries(req, 1);
  EXPECT_FALSE(resp.success());
  EXPECT_EQ(resp.conflict_index(), 5u);  // our last index + 1
  req.set_prev_log_index(4);
  req.set_prev_log_term(3);  // we have term 2 there
  resp = c.handle_append_entries(req, 2);
  EXPECT_FALSE(resp.success());
  EXPECT_EQ(resp.conflict_index(), 3u);  // first index of our term 2
  EXPECT_EQ(c.leader_id(), 1u);          // it is the leader, even if our logs differ
}

TEST(RaftCore, ConflictingEntriesAreTruncatedAndReplaced) {
  RaftCore c(opts(2, {1, 2, 3}), with_log(2, {entry(1, 1), entry(2, 2), entry(2, 3), entry(2, 4)}),
             0);
  v1::AppendEntriesRequest req;
  req.set_term(3);
  req.set_leader_id(1);
  req.set_prev_log_index(1);
  req.set_prev_log_term(1);
  *req.add_entries() = entry(3, 2, "new");
  req.set_leader_commit(1);
  const auto resp = c.handle_append_entries(req, 1);
  ASSERT_TRUE(resp.success());
  EXPECT_EQ(resp.match_index(), 2u);
  EXPECT_EQ(c.last_index(), 2u);
  EXPECT_EQ(c.term_at(2), 3u);
  Ready r = c.take_ready();
  ASSERT_TRUE(r.truncate_from);
  EXPECT_EQ(*r.truncate_from, 2u);  // the disk drops 2..4, then appends the new 2
  ASSERT_EQ(r.entries.size(), 1u);
  EXPECT_EQ(r.entries[0].command(), "new");
  ASSERT_EQ(r.committed.size(), 1u);  // leaderCommit 1
}

TEST(RaftCore, ALateShorterAppendDoesNotTruncate) {
  RaftCore c(opts(2, {1, 2, 3}), {}, 0);
  v1::AppendEntriesRequest longer;
  longer.set_term(1);
  longer.set_leader_id(1);
  for (uint64_t i = 1; i <= 3; ++i) *longer.add_entries() = entry(1, i);
  ASSERT_TRUE(c.handle_append_entries(longer, 1).success());
  (void)c.take_ready();
  v1::AppendEntriesRequest shorter = longer;  // the same request's first entry only,
  shorter.clear_entries();                    // delivered late
  *shorter.add_entries() = entry(1, 1);
  const auto resp = c.handle_append_entries(shorter, 2);
  EXPECT_TRUE(resp.success());
  EXPECT_EQ(c.last_index(), 3u);  // nothing cut off
  EXPECT_FALSE(c.take_ready().truncate_from);
}

TEST(RaftCore, LeaderDoesNotCommitAnEarlierTermsEntryByCounting) {
  // Figure 8. Server 1 holds (1,1) (2,2), the (2,2) never committed, and
  // becomes leader of term 3. A follower acknowledging index 2 gives index
  // 2 a majority, but it is from term 2, so it must not commit by count:
  // only the term-3 no-op reaching a majority commits it (indirectly).
  RaftCore c(opts(1, {1, 2, 3}), with_log(2, {entry(1, 1), entry(2, 2)}), 0);
  (void)elect(c, 2);
  ASSERT_EQ(c.term(), 3u);
  EXPECT_EQ(c.term_start_index(), 3u);
  c.handle_append_entries_response(2, ack(c, 2), 1001);
  EXPECT_EQ(c.commit_index(), 0u);
  c.handle_append_entries_response(2, ack(c, 3), 1002);
  EXPECT_EQ(c.commit_index(), 3u);
  EXPECT_EQ(c.take_ready().committed.size(), 3u);
}

TEST(RaftCore, RepliesToAnEarlierTermAreIgnoredAndHigherTermsDepose) {
  RaftCore c(opts(1, {1, 2, 3}), {}, 0);
  (void)elect(c, 2);
  (void)c.propose({"a", "b"}, 1001);
  auto old = ack(c, 3);
  old.set_request_term(c.term() - 1);  // delayed reply to a request from term 0
  c.handle_append_entries_response(2, old, 1002);
  EXPECT_EQ(c.match_index(2), 0u);
  EXPECT_EQ(c.commit_index(), 0u);
  // Reordered: a newer success, then an older one; match only goes up.
  c.handle_append_entries_response(2, ack(c, 3), 1003);
  c.handle_append_entries_response(2, ack(c, 1), 1004);
  EXPECT_EQ(c.match_index(2), 3u);
  auto higher = ack(c, 0);
  higher.set_term(c.term() + 1);
  higher.set_success(false);
  c.handle_append_entries_response(3, higher, 1005);
  EXPECT_EQ(c.role(), Role::kFollower);
  EXPECT_EQ(c.term(), 2u);
  EXPECT_FALSE(c.propose({"c"}, 1006));  // not the leader any more
}

TEST(RaftCore, CandidateStepsDownForALeaderOfItsTerm) {
  RaftCore c(opts(1, {1, 2, 3}), {}, 0);
  c.tick(1000);
  ASSERT_EQ(c.role(), Role::kCandidate);
  v1::AppendEntriesRequest hb;
  hb.set_term(c.term());
  hb.set_leader_id(2);
  EXPECT_TRUE(c.handle_append_entries(hb, 1001).success());
  EXPECT_EQ(c.role(), Role::kFollower);
  EXPECT_EQ(c.leader_id(), 2u);
}

TEST(RaftCore, ReadRoundNeedsAMajorityToAnswerAfterItStarted) {
  RaftCore c(opts(1, {1, 2, 3}), {}, 0);
  (void)elect(c, 2);
  c.handle_append_entries_response(2, ack(c, 1, 0), 1001);
  const uint64_t round = c.start_read_round(1002);
  EXPECT_GT(round, 0u);
  EXPECT_LT(c.confirmed_round(), round);
  const auto sent = messages_of<v1::AppendEntriesRequest>(c.take_ready());
  ASSERT_EQ(sent.size(), 2u);  // sent at once, not at the next heartbeat
  EXPECT_EQ(sent[0].round(), round);
  c.handle_append_entries_response(3, ack(c, 0, round - 1), 1003);  // answer to an older round
  EXPECT_LT(c.confirmed_round(), round);
  auto fail = ack(c, 0, round);
  fail.set_success(false);  // a log mismatch still accepts our term
  fail.set_conflict_index(1);
  c.handle_append_entries_response(3, fail, 1004);
  EXPECT_EQ(c.confirmed_round(), round);
}

TEST(RaftCore, CompactionHandsTheSnapshotAndTheRemainingLogToStorage) {
  RaftCore c(opts(1, {1}), {}, 0);
  c.tick(301);
  (void)c.propose({"a", "b", "c"}, 302);
  (void)c.take_ready();
  ASSERT_EQ(c.applied_index(), 4u);
  c.compact(3, std::make_shared<const std::string>("state@3"));
  Ready r = c.take_ready();
  ASSERT_TRUE(r.snapshot);
  EXPECT_EQ(r.snapshot->index, 3u);
  EXPECT_EQ(r.snapshot->term, 1u);
  ASSERT_EQ(r.entries.size(), 1u);  // only entry 4 is left in the log
  EXPECT_EQ(r.entries[0].index(), 4u);
  EXPECT_FALSE(r.restore);  // our own state machine already has it
  EXPECT_THROW(c.compact(5, nullptr), std::invalid_argument);  // not applied yet
}

TEST(RaftCore, LaggingFollowerGetsTheSnapshotAndKeepsAMatchingSuffix) {
  // Leader: snapshot through 3 (term 1), then entry 4.
  PersistentState ls;
  ls.current_term = 1;
  ls.snapshot = {3, 1, std::make_shared<const std::string>("S3")};
  ls.entries = {entry(1, 4)};
  RaftCore leader(opts(1, {1, 2, 3}), ls, 0);
  (void)leader.take_ready();  // the restore of its own snapshot
  (void)elect(leader, 3);
  // Follower 2 is empty, so after one failed AppendEntries the leader
  // must fall back to the snapshot.
  v1::AppendEntriesResponse miss;
  miss.set_term(leader.term());
  miss.set_request_term(leader.term());
  miss.set_success(false);
  miss.set_conflict_index(1);
  leader.handle_append_entries_response(2, miss, 1001);
  const auto snaps = messages_of<v1::InstallSnapshotRequest>(leader.take_ready());
  ASSERT_EQ(snaps.size(), 1u);
  EXPECT_EQ(snaps[0].last_included_index(), 3u);
  EXPECT_EQ(snaps[0].data(), "S3");

  // A follower holding (1,1)..(1,5): its entry 3 matches the snapshot's
  // last entry, so it keeps 4 and 5 (Figure 13, step 6).
  RaftCore f(opts(2, {1, 2, 3}),
             with_log(1, {entry(1, 1), entry(1, 2), entry(1, 3), entry(1, 4), entry(1, 5)}), 0);
  const auto resp = f.handle_install_snapshot(snaps[0], 5);
  EXPECT_EQ(resp.last_included_index(), 3u);
  EXPECT_EQ(f.snapshot_index(), 3u);
  EXPECT_EQ(f.last_index(), 5u);
  Ready r = f.take_ready();
  ASSERT_TRUE(r.restore);  // the state machine is replaced by the snapshot
  EXPECT_EQ(*r.restore->data, "S3");
  ASSERT_TRUE(r.snapshot);  // and it is persisted with the kept suffix
  EXPECT_EQ(r.entries.size(), 2u);
  EXPECT_EQ(f.commit_index(), 3u);

  // A follower whose entry 3 has another term discards its whole log.
  RaftCore g(opts(3, {1, 2, 3}), with_log(2, {entry(1, 1), entry(2, 2), entry(2, 3)}), 0);
  (void)g.handle_install_snapshot(snaps[0], 6);
  EXPECT_EQ(g.last_index(), 3u);
  EXPECT_TRUE(g.entries().empty());

  leader.handle_install_snapshot_response(2, resp, 1002);
  EXPECT_EQ(leader.match_index(2), 3u);
  EXPECT_EQ(leader.next_index(2), 4u);
}

TEST(RaftCore, RejectionsWhileASnapshotIsInFlightDoNotPingPong) {
  // While the snapshot to a follower is unacknowledged, the leader's
  // heartbeats to it are AppendEntries at the snapshot index, which a
  // follower without the snapshot rejects. Answering each rejection with
  // another AppendEntries (which the follower rejects again) would bounce
  // messages between the two as fast as the network allows until the
  // snapshot lands; the snapshot is already on its way, so the leader waits.
  PersistentState ls;
  ls.current_term = 1;
  ls.snapshot = {3, 1, std::make_shared<const std::string>("S3")};
  RaftCore leader(opts(1, {1, 2, 3}), ls, 0);
  (void)leader.take_ready();
  (void)elect(leader, 3);
  v1::AppendEntriesResponse miss;
  miss.set_term(leader.term());
  miss.set_request_term(leader.term());
  miss.set_success(false);
  miss.set_conflict_index(1);
  leader.handle_append_entries_response(2, miss, 1001);
  ASSERT_EQ(messages_of<v1::InstallSnapshotRequest>(leader.take_ready()).size(), 1u);
  // The next heartbeat (snapshot still unacknowledged, retry not due) is
  // an AppendEntries at the snapshot index...
  leader.tick(1100);
  Ready hb = leader.take_ready();
  EXPECT_TRUE(messages_of<v1::InstallSnapshotRequest>(hb).empty());
  ASSERT_FALSE(messages_of<v1::AppendEntriesRequest>(hb).empty());
  // ...which follower 2 rejects; that rejection must send nothing to it.
  leader.handle_append_entries_response(2, miss, 1101);
  for (const OutMessage& m : leader.take_ready().messages) EXPECT_NE(m.to, 2u);
  // Once the retry interval has passed, a heartbeat resends the snapshot
  // (the first copy may have been lost).
  leader.tick(1000 + 1000 + 50);
  bool resent = false;
  for (const OutMessage& m : leader.take_ready().messages) {
    resent |= m.to == 2 && std::holds_alternative<v1::InstallSnapshotRequest>(m.body);
  }
  EXPECT_TRUE(resent);
}

TEST(RaftCore, ASnapshotOlderThanTheCommitIndexChangesNothing) {
  RaftCore f(opts(2, {1, 2, 3}), {}, 0);
  v1::AppendEntriesRequest req;
  req.set_term(1);
  req.set_leader_id(1);
  for (uint64_t i = 1; i <= 5; ++i) *req.add_entries() = entry(1, i);
  req.set_leader_commit(5);
  ASSERT_TRUE(f.handle_append_entries(req, 1).success());
  (void)f.take_ready();
  v1::InstallSnapshotRequest is;
  is.set_term(1);
  is.set_leader_id(1);
  is.set_last_included_index(3);
  is.set_last_included_term(1);
  is.set_data("old");
  (void)f.handle_install_snapshot(is, 2);
  Ready r = f.take_ready();
  EXPECT_FALSE(r.restore);  // applying it would undo entries 4 and 5
  EXPECT_EQ(f.snapshot_index(), 0u);
  EXPECT_EQ(f.commit_index(), 5u);
}

TEST(RaftCore, MalformedAppendIsRefusedWithoutAHint) {
  RaftCore f(opts(2, {1, 2, 3}), {}, 0);
  v1::AppendEntriesRequest req;
  req.set_term(1);
  req.set_leader_id(1);
  *req.add_entries() = entry(1, 1);
  *req.add_entries() = entry(1, 3);  // a gap
  const auto resp = f.handle_append_entries(req, 1);
  EXPECT_FALSE(resp.success());
  EXPECT_EQ(resp.conflict_index(), 0u);
  EXPECT_EQ(f.last_index(), 0u);
}

TEST(RaftCore, RestartRestoresTheSnapshotThroughTheFirstReady) {
  PersistentState s;
  s.current_term = 4;
  s.voted_for = 2;
  s.snapshot = {10, 3, std::make_shared<const std::string>("S10")};
  s.entries = {entry(4, 11)};
  RaftCore c(opts(1, {1, 2, 3}), s, 0);
  EXPECT_EQ(c.term(), 4u);
  EXPECT_EQ(c.voted_for(), 2u);
  EXPECT_EQ(c.commit_index(), 10u);  // the snapshot is committed; 11 is not known to be
  Ready r = c.take_ready();
  ASSERT_TRUE(r.restore);
  EXPECT_EQ(r.restore->index, 10u);
  EXPECT_FALSE(r.hard_state);  // nothing changed, nothing to write
  s.entries = {entry(4, 12)};  // a gap after the snapshot
  EXPECT_THROW({ RaftCore bad(opts(1, {1, 2, 3}), s, 0); }, std::invalid_argument);
}

}  // namespace
}  // namespace fleet::raft
