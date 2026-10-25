// Deterministic simulation of Raft clusters (m4b): RaftCore driven by a
// fake network and a fake clock, thousands of seeds, Raft's safety
// properties checked after every event.
//
// WHY A SIMULATION. Consensus bugs hide in interleavings that a test over
// real sockets almost never produces: a vote granted just before a crash,
// a reply from two terms ago, a duplicated AppendEntries that arrives
// after a longer one. Because RaftCore reads no clock and does no I/O, one
// seeded random generator here decides every delay, loss, duplicate,
// partition, crash and client request, so any failure replays exactly
// from its seed (printed with it): FLEET_RAFT_SEED=<seed> runs one seed,
// FLEET_RAFT_TRACE=1 prints its events, FLEET_RAFT_SEED_BASE=<n> runs the
// same suite on seeds shifted by n (for longer sweeps).
//
// THE MODEL. Each server is a RaftCore plus a FakeDisk (what has been
// persisted: hard state, snapshot, log, written from each Ready exactly as
// RaftStorage writes them) plus a state machine (the list of applied
// commands). The network delivers each message after a random delay
// (reordering), drops some, duplicates some, delays a few by hundreds of
// ms (stale messages), and drops everything between servers on different
// sides of a partition. Servers crash (losing everything but the disk)
// and restart from the disk. Clients propose commands and ReadIndex reads
// at whichever server believes it is the leader. Servers compact their
// logs into snapshots, so lagging servers are caught up by
// InstallSnapshot. Responses are sent only after the Ready that follows
// the handler has been persisted, as the real replica does.
//
// THE CHECKS (four of the Raft paper's Figure 3 properties, then this
// implementation's own):
//   Election Safety: at most one leader per term.
//   Log Matching: if two logs have an entry with the same index and term,
//     they are identical in every entry up to it.
//   Leader Completeness: a server that becomes leader holds every entry
//     committed before its election.
//   State Machine Safety: every server applies the same entry at each
//     index, in index order, with no gaps (a snapshot restore jumps
//     forward to exactly its index).
//   Durability: after a server's outputs are persisted (before anything
//     it produced is sent), its disk holds exactly its term, vote,
//     snapshot index and log, so a crash at that point forgets nothing
//     the others may have heard.
//   Commit monotonic: a server's commit index never decreases (within one
//     run of the process; it is volatile).
//   Reads: a confirmed ReadIndex read was not served by a stale leader (no
//     leader of a higher term was elected before the read began) and
//     covers every entry committed before it began.
//   Liveness, at the end: after healing every fault, one leader emerges
//     and a new command is applied by every server.

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <map>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

#include "raft_core.h"

namespace fleet::raft {
namespace {

// ------------------------------------------------------------ the disk

// What a server has persisted, updated from each Ready the way
// RaftStorage::persist updates its files.
struct FakeDisk {
  PersistentState st;

  void persist(const Ready& r) {
    if (r.hard_state) {
      st.current_term = r.hard_state->current_term();
      st.voted_for = r.hard_state->voted_for();
    }
    if (r.snapshot) {
      st.snapshot = *r.snapshot;
      st.entries = r.entries;
      return;
    }
    if (r.truncate_from) {
      while (!st.entries.empty() && st.entries.back().index() >= *r.truncate_from) {
        st.entries.pop_back();
      }
    }
    for (const v1::Entry& e : r.entries) {
      const uint64_t expect = st.snapshot.index + st.entries.size() + 1;
      if (e.index() != expect) {
        throw std::logic_error("Ready appends entry " + std::to_string(e.index()) +
                               " where the disk expects " + std::to_string(expect));
      }
      st.entries.push_back(e);
    }
  }
};

// ------------------------------------------------------------ the state machine

// The applied commands, in order, with their indices. Its snapshot is the
// same list as text; restoring parses it back.
struct StateMachine {
  uint64_t applied = 0;
  std::vector<std::pair<uint64_t, std::string>> commands;

  std::string encode() const {
    std::ostringstream out;
    out << applied << "\n";
    for (const auto& [i, c] : commands) out << i << " " << c << "\n";
    return out.str();
  }
  static StateMachine decode(const std::string& s) {
    StateMachine sm;
    std::istringstream in(s);
    in >> sm.applied;
    uint64_t i = 0;
    std::string c;
    while (in >> i >> c) sm.commands.emplace_back(i, c);
    return sm;
  }
};

// ------------------------------------------------------------ the network

using Body = std::variant<v1::RequestVoteRequest, v1::RequestVoteResponse,
                          v1::AppendEntriesRequest, v1::AppendEntriesResponse,
                          v1::InstallSnapshotRequest, v1::InstallSnapshotResponse>;

struct Packet {
  int64_t at = 0;
  uint64_t seq = 0;  // ties broken by send order, so runs are deterministic
  uint32_t from = 0;
  uint32_t to = 0;
  Body body;
};
struct Later {
  bool operator()(const Packet& a, const Packet& b) const {
    return a.at != b.at ? a.at > b.at : a.seq > b.seq;
  }
};

struct SimConfig {
  int nodes = 3;
  int64_t chaos_ms = 6000;  // faults on
  int64_t settle_ms = 1000; // faults off before the liveness check
  double drop = 0.05;
  double dup = 0.02;
  int64_t min_delay = 1;
  int64_t max_delay = 15;
  double long_delay = 0.01;  // probability of a 100 to 600 ms delay
  double crash_per_ms = 0.0002;
  // A server that has just granted a vote (persisted, response sent)
  // crashes at once: the case that loses a vote if votes were not durable,
  // letting it vote twice in one term after a quick restart.
  double crash_after_vote = 0.0;
  double partition_per_ms = 0.0005;
  uint64_t compact_every = 20;  // 0: never
  size_t max_entries = 8;
  double propose_per_ms = 0.05;
  double read_per_ms = 0.01;
};

struct Committed {
  uint64_t term = 0;
  int type = 0;
  std::string command;
};

struct PendingRead {
  uint32_t node = 0;
  uint64_t incarnation = 0;
  uint64_t term = 0;
  uint64_t round = 0;
  int64_t started = 0;
  uint64_t need_index = 0;
  uint64_t committed_before = 0;
};

class Sim {
 public:
  Sim(uint64_t seed, SimConfig cfg) : seed_(seed), cfg_(cfg), rng_(seed) {
    trace_ = std::getenv("FLEET_RAFT_TRACE") != nullptr;
    for (int i = 1; i <= cfg_.nodes; ++i) members_.push_back(static_cast<uint32_t>(i));
    nodes_.resize(static_cast<size_t>(cfg_.nodes));
    group_.assign(static_cast<size_t>(cfg_.nodes) + 1, 0);
    for (uint32_t id : members_) start(id);
  }

  // Runs the whole scenario. Returns "" or the first violation found.
  std::string run() {
    try {
      const int64_t end_chaos = cfg_.chaos_ms;
      for (; now_ < end_chaos && failure_.empty(); ++now_) step(true);
      heal();
      const int64_t settle = now_ + cfg_.settle_ms;
      for (; now_ < settle && failure_.empty(); ++now_) step(false);
      if (failure_.empty()) check_liveness();
    } catch (const std::exception& e) {
      fail(std::string("exception: ") + e.what());
    }
    if (failure_.empty()) check_log_matching();
    return failure_;
  }

  size_t elections() const { return elected_.size(); }
  size_t committed() const { return committed_.size(); }
  uint64_t snapshots_installed() const { return installs_; }
  uint64_t reads_confirmed() const { return reads_ok_; }
  uint64_t crashes() const { return crashes_; }

 private:
  struct Node {
    std::unique_ptr<RaftCore> core;  // null while crashed
    FakeDisk disk;
    StateMachine sm;
    int64_t restart_at = -1;
    uint64_t incarnation = 0;
    uint64_t last_commit = 0;
  };

  Node& node(uint32_t id) { return nodes_[id - 1]; }
  bool chance(double p) { return std::uniform_real_distribution<double>(0, 1)(rng_) < p; }
  int64_t uniform(int64_t lo, int64_t hi) {
    return std::uniform_int_distribution<int64_t>(lo, hi)(rng_);
  }

  void trace(const std::string& s) {
    if (trace_) std::fprintf(stderr, "[%6lld] %s\n", static_cast<long long>(now_), s.c_str());
  }
  void fail(const std::string& why) {
    if (failure_.empty()) {
      failure_ = "seed " + std::to_string(seed_) + " at t=" + std::to_string(now_) + " ms: " + why +
                 "  (replay: FLEET_RAFT_SEED=" + std::to_string(seed_) +
                 " FLEET_RAFT_TRACE=1 fleet_tests --gtest_filter='RaftSim*')\n" + dump();
    }
  }
  // Every server's Raft state, for failure messages.
  std::string dump() {
    std::ostringstream out;
    for (uint32_t id : members_) {
      Node& n = node(id);
      out << "  node " << id << ": ";
      if (!n.core) {
        out << "down\n";
        continue;
      }
      const RaftCore& c = *n.core;
      out << to_string(c.role()) << " term " << c.term() << " vote " << c.voted_for() << " leader "
          << c.leader_id() << " commit " << c.commit_index() << " applied " << n.sm.applied
          << " last " << c.last_index() << " (term " << c.last_term() << ") snapshot "
          << c.snapshot_index();
      if (c.role() == Role::kLeader) {
        for (uint32_t p : members_) {
          if (p != id) out << " | peer " << p << " next " << c.next_index(p) << " match " << c.match_index(p);
        }
      }
      out << "\n";
    }
    return out.str();
  }

  void start(uint32_t id) {
    Node& n = node(id);
    RaftOptions o;
    o.id = id;
    o.members = members_;
    o.election_timeout_min_ms = 150;
    o.election_timeout_max_ms = 300;
    o.heartbeat_interval_ms = 50;
    o.max_entries_per_append = cfg_.max_entries;
    o.snapshot_retry_ms = 300;
    o.seed = seed_ * 1000 + n.incarnation;
    n.core = std::make_unique<RaftCore>(o, n.disk.st, now_);
    n.sm = StateMachine{};
    n.last_commit = n.core->commit_index();
    ++n.incarnation;
    trace("node " + std::to_string(id) + " starts at term " + std::to_string(n.disk.st.current_term));
    process(id);  // the restored snapshot, if any
  }

  void crash(uint32_t id) {
    Node& n = node(id);
    n.core.reset();
    n.restart_at = now_ + uniform(20, 800);
    ++crashes_;
    trace("node " + std::to_string(id) + " crashes");
  }

  void heal() {
    trace("heal: partitions off, every node up, no loss");
    std::fill(group_.begin(), group_.end(), 0);
    for (uint32_t id : members_) {
      if (!node(id).core) start(id);
    }
    healed_ = true;
  }

  bool connected(uint32_t a, uint32_t b) const { return group_[a] == group_[b]; }

  void send(uint32_t from, uint32_t to, Body body) {
    if (!healed_ && chance(cfg_.drop)) return;
    const int copies = (!healed_ && chance(cfg_.dup)) ? 2 : 1;
    for (int c = 0; c < copies; ++c) {
      int64_t delay = uniform(cfg_.min_delay, cfg_.max_delay);
      if (!healed_ && chance(cfg_.long_delay)) delay = uniform(100, 600);
      net_.push(Packet{now_ + delay, seq_++, from, to, body});
    }
  }

  // Carries out a server's Ready: persist, send, apply (in that order),
  // then compact if due. Repeats until nothing is left (a compaction makes
  // another Ready).
  void process(uint32_t id) {
    Node& n = node(id);
    while (n.core) {
      Ready r = n.core->take_ready();
      if (r.empty()) break;
      n.disk.persist(r);
      for (OutMessage& m : r.messages) {
        std::visit([&](auto& req) { send(id, m.to, req); }, m.body);
      }
      if (r.restore) restore(id, *r.restore);
      for (const v1::Entry& e : r.committed) apply(id, e);
      if (cfg_.compact_every > 0 &&
          n.sm.applied >= n.core->snapshot_index() + cfg_.compact_every) {
        n.core->compact(n.sm.applied, std::make_shared<const std::string>(n.sm.encode()));
      }
    }
    if (n.core) check_node(id);
  }

  void restore(uint32_t id, const SnapshotRef& s) {
    Node& n = node(id);
    n.sm = StateMachine::decode(*s.data);
    ++installs_;
    if (n.sm.applied != s.index) {
      fail("node " + std::to_string(id) + " restored a snapshot claiming index " +
           std::to_string(n.sm.applied) + " for snapshot index " + std::to_string(s.index));
    }
    for (const auto& [i, c] : n.sm.commands) {
      auto it = committed_.find(i);
      if (it == committed_.end() || it->second.command != c) {
        fail("node " + std::to_string(id) + " restored a snapshot with command '" + c +
             "' at index " + std::to_string(i) + " that was never committed there");
      }
    }
  }

  void apply(uint32_t id, const v1::Entry& e) {
    Node& n = node(id);
    // State Machine Safety, part 1: in order, no gaps.
    if (e.index() != n.sm.applied + 1) {
      fail("node " + std::to_string(id) + " applied index " + std::to_string(e.index()) +
           " after " + std::to_string(n.sm.applied));
      return;
    }
    n.sm.applied = e.index();
    if (e.type() == v1::Entry::TYPE_COMMAND) n.sm.commands.emplace_back(e.index(), e.command());
    // Part 2: the same entry at this index on every server.
    auto [it, inserted] = committed_.try_emplace(
        e.index(), Committed{e.term(), static_cast<int>(e.type()), e.command()});
    if (!inserted && (it->second.term != e.term() || it->second.type != e.type() ||
                      it->second.command != e.command())) {
      fail("State Machine Safety: node " + std::to_string(id) + " applied (term " +
           std::to_string(e.term()) + ", '" + e.command() + "') at index " +
           std::to_string(e.index()) + " where another applied (term " +
           std::to_string(it->second.term) + ", '" + it->second.command + "')");
    }
    max_committed_ = std::max(max_committed_, e.index());
  }

  // Checks that hold for one server after every event.
  void check_node(uint32_t id) {
    Node& n = node(id);
    const RaftCore& c = *n.core;
    if (c.commit_index() < n.last_commit) {
      fail("node " + std::to_string(id) + " commit index went from " +
           std::to_string(n.last_commit) + " to " + std::to_string(c.commit_index()));
    }
    n.last_commit = c.commit_index();
    // Durability: once a server's Readies are persisted (process() has
    // just done that, and only then may anything be sent), the disk holds
    // exactly the core's term, vote, snapshot and log. A field the core
    // changed without marking it for persistence (a vote granted in a term
    // already adopted, say) shows up here at once, without needing the
    // rare crash that would turn it into a double vote. The log is
    // compared by length and last entry here, entry by entry in
    // check_log_matching.
    const PersistentState& d = n.disk.st;
    const bool same_log =
        d.entries.size() == c.entries().size() &&
        (d.entries.empty() || (d.entries.back().index() == c.entries().back().index() &&
                               d.entries.back().term() == c.entries().back().term()));
    if (d.current_term != c.term() || d.voted_for != c.voted_for() ||
        d.snapshot.index != c.snapshot_index() || !same_log) {
      fail("Durability: node " + std::to_string(id) + " persisted term " +
           std::to_string(d.current_term) + " vote " + std::to_string(d.voted_for) +
           " snapshot " + std::to_string(d.snapshot.index) + " and " +
           std::to_string(d.entries.size()) + " entries, but its core has term " +
           std::to_string(c.term()) + " vote " + std::to_string(c.voted_for()) + " snapshot " +
           std::to_string(c.snapshot_index()) + " and " + std::to_string(c.entries().size()) +
           " entries");
    }
    if (c.role() != Role::kLeader) return;
    // Election Safety.
    auto [it, first] = elected_.try_emplace(c.term(), std::make_pair(id, now_));
    if (!first) {
      if (it->second.first != id) {
        fail("Election Safety: nodes " + std::to_string(it->second.first) + " and " +
             std::to_string(id) + " are both leaders of term " + std::to_string(c.term()));
      }
      return;
    }
    trace("node " + std::to_string(id) + " elected leader of term " + std::to_string(c.term()));
    // Leader Completeness: every entry committed before this election.
    for (const auto& [i, ce] : committed_) {
      if (i <= c.snapshot_index()) continue;  // inside its snapshot of committed state
      if (i > c.last_index() || c.term_at(i) != ce.term ||
          c.entries()[i - c.snapshot_index() - 1].command() != ce.command) {
        fail("Leader Completeness: new leader " + std::to_string(id) + " of term " +
             std::to_string(c.term()) + " lacks committed entry " + std::to_string(i));
        return;
      }
    }
  }

  // Log Matching between every pair of running servers.
  void check_log_matching() {
    // Durability, entry by entry (check_node compares only the ends).
    for (uint32_t id : members_) {
      const Node& n = node(id);
      if (!n.core) continue;
      const std::vector<v1::Entry>& disk = n.disk.st.entries;
      const std::vector<v1::Entry>& mem = n.core->entries();
      for (size_t k = 0; k < disk.size() && k < mem.size(); ++k) {
        if (disk[k].index() != mem[k].index() || disk[k].term() != mem[k].term() ||
            disk[k].command() != mem[k].command()) {
          fail("Durability: node " + std::to_string(id) + " has entry " +
               std::to_string(mem[k].index()) + " in memory unlike the one on its disk");
          return;
        }
      }
    }
    for (uint32_t a : members_) {
      for (uint32_t b : members_) {
        if (a >= b || !node(a).core || !node(b).core) continue;
        const RaftCore& x = *node(a).core;
        const RaftCore& y = *node(b).core;
        const uint64_t lo = std::max(x.snapshot_index(), y.snapshot_index()) + 1;
        const uint64_t hi = std::min(x.last_index(), y.last_index());
        uint64_t top = 0;  // highest common index with equal terms
        for (uint64_t i = lo; i <= hi; ++i) {
          if (x.term_at(i) == y.term_at(i)) top = i;
        }
        for (uint64_t i = lo; i <= top; ++i) {
          const v1::Entry& ex = x.entries()[i - x.snapshot_index() - 1];
          const v1::Entry& ey = y.entries()[i - y.snapshot_index() - 1];
          if (ex.term() != ey.term() || ex.command() != ey.command() || ex.type() != ey.type()) {
            fail("Log Matching: nodes " + std::to_string(a) + " and " + std::to_string(b) +
                 " agree at index " + std::to_string(top) + " but differ at " + std::to_string(i));
            return;
          }
        }
      }
    }
  }

  void deliver(const Packet& p) {
    Node& to = node(p.to);
    if (!to.core || !connected(p.from, p.to)) return;
    RaftCore& c = *to.core;
    std::visit(
        [&](const auto& m) {
          using T = std::decay_t<decltype(m)>;
          if constexpr (std::is_same_v<T, v1::RequestVoteRequest>) {
            auto resp = c.handle_request_vote(m, now_);
            process(p.to);  // persisted before the response leaves
            send(p.to, p.from, resp);
            if (resp.vote_granted() && !healed_ && chance(cfg_.crash_after_vote)) {
              crash(p.to);
              node(p.to).restart_at = now_ + uniform(1, 60);  // back within the same election
            }
          } else if constexpr (std::is_same_v<T, v1::AppendEntriesRequest>) {
            auto resp = c.handle_append_entries(m, now_);
            process(p.to);
            send(p.to, p.from, resp);
          } else if constexpr (std::is_same_v<T, v1::InstallSnapshotRequest>) {
            auto resp = c.handle_install_snapshot(m, now_);
            process(p.to);
            send(p.to, p.from, resp);
          } else if constexpr (std::is_same_v<T, v1::RequestVoteResponse>) {
            c.handle_request_vote_response(p.from, m, now_);
            process(p.to);
          } else if constexpr (std::is_same_v<T, v1::AppendEntriesResponse>) {
            c.handle_append_entries_response(p.from, m, now_);
            process(p.to);
          } else {
            c.handle_install_snapshot_response(p.from, m, now_);
            process(p.to);
          }
        },
        p.body);
  }

  void step(bool chaos) {
    while (!net_.empty() && net_.top().at <= now_ && failure_.empty()) {
      Packet p = net_.top();
      net_.pop();
      deliver(p);
    }
    for (uint32_t id : members_) {
      Node& n = node(id);
      if (!n.core) {
        if (n.restart_at >= 0 && now_ >= n.restart_at) start(id);
        continue;
      }
      n.core->tick(now_);
      process(id);
    }
    if (chaos) faults();
    clients();
    check_reads();
    if (now_ % 250 == 0) check_log_matching();
  }

  void faults() {
    for (uint32_t id : members_) {
      if (node(id).core && chance(cfg_.crash_per_ms)) crash(id);
    }
    if (chance(cfg_.partition_per_ms)) {
      // Either heal, or split into two random sides (often isolating the
      // current leader, the interesting case).
      if (chance(0.4)) {
        std::fill(group_.begin(), group_.end(), 0);
        trace("partition healed");
      } else {
        const uint32_t leader = current_leader();
        for (uint32_t id : members_) group_[id] = chance(0.5) ? 1 : 0;
        if (leader && chance(0.5)) {
          for (uint32_t id : members_) group_[id] = 0;
          group_[leader] = 1;
        }
        std::string sides;
        for (uint32_t id : members_) sides += std::to_string(id) + ":" + std::to_string(group_[id]) + " ";
        trace("partition " + sides);
      }
    }
  }

  uint32_t current_leader() {
    uint32_t best = 0;
    uint64_t term = 0;
    for (uint32_t id : members_) {
      Node& n = node(id);
      if (n.core && n.core->role() == Role::kLeader && n.core->term() >= term) {
        best = id;
        term = n.core->term();
      }
    }
    return best;
  }

  void clients() {
    if (chance(cfg_.propose_per_ms)) {
      // Any server that believes it leads: stale leaders included, whose
      // entries must then never be applied anywhere.
      std::vector<uint32_t> leaders;
      for (uint32_t id : members_) {
        if (node(id).core && node(id).core->role() == Role::kLeader) leaders.push_back(id);
      }
      if (!leaders.empty()) {
        const uint32_t id = leaders[static_cast<size_t>(uniform(0, static_cast<int64_t>(leaders.size()) - 1))];
        std::vector<std::string> cmds;
        const int k = static_cast<int>(uniform(1, 3));
        for (int i = 0; i < k; ++i) cmds.push_back("c" + std::to_string(next_command_++));
        node(id).core->propose(std::move(cmds), now_);
        process(id);
      }
    }
    if (chance(cfg_.read_per_ms)) {
      for (uint32_t id : members_) {
        Node& n = node(id);
        if (!n.core || n.core->role() != Role::kLeader) continue;
        PendingRead r;
        r.node = id;
        r.incarnation = n.incarnation;
        r.term = n.core->term();
        // Half the reads wait for what the leader has committed (classic
        // ReadIndex: max of its commit index and its term's no-op, which
        // it must have committed first); half for its whole log, as the
        // replica's replies do (they may depend on uncommitted entries).
        r.need_index = chance(0.5) ? std::max(n.core->commit_index(), n.core->term_start_index())
                                   : n.core->last_index();
        r.started = now_;
        r.committed_before = max_committed_;
        r.round = n.core->start_read_round(now_);
        reads_.push_back(r);
        process(id);
      }
    }
  }

  void check_reads() {
    std::vector<PendingRead> keep;
    for (const PendingRead& r : reads_) {
      Node& n = node(r.node);
      if (!n.core || n.incarnation != r.incarnation || n.core->role() != Role::kLeader ||
          n.core->term() != r.term) {
        continue;  // abandoned: the reply would have been an error
      }
      if (n.core->confirmed_round() < r.round || n.core->commit_index() < r.need_index) {
        keep.push_back(r);
        continue;
      }
      ++reads_ok_;
      for (const auto& [term, who] : elected_) {
        if (term > r.term && who.second < r.started) {
          fail("stale read: node " + std::to_string(r.node) + " confirmed a read of term " +
               std::to_string(r.term) + " begun at " + std::to_string(r.started) +
               " ms, but node " + std::to_string(who.first) + " was elected for term " +
               std::to_string(term) + " at " + std::to_string(who.second) + " ms");
        }
      }
      if (r.need_index < r.committed_before) {
        fail("read at node " + std::to_string(r.node) + " covers index " +
             std::to_string(r.need_index) + " but " + std::to_string(r.committed_before) +
             " was committed before it began");
      }
    }
    reads_ = std::move(keep);
  }

  // After every fault is healed: a leader emerges and a new command
  // reaches every server's state machine. Proposed again (as a new
  // command) if leadership moves before it commits, since a deposed
  // leader's uncommitted entry may rightly be overwritten. The bound is
  // generous because catch-up speed depends on the seed: with one entry
  // per AppendEntries and one in flight per follower, replicating a
  // backlog of hundreds of entries takes hundreds of round trips.
  void check_liveness() {
    // FLEET_RAFT_LIVENESS_MS shortens the bound for mutation runs, where a
    // broken core can spend the whole minute on every seed.
    const char* bound = std::getenv("FLEET_RAFT_LIVENESS_MS");
    const int64_t limit = now_ + (bound ? std::atoll(bound) : 60000);
    int attempt = 0;
    while (now_ < limit && failure_.empty()) {
      const uint32_t leader = current_leader();
      if (!leader) {
        step(false);
        ++now_;
        continue;
      }
      const uint64_t term = node(leader).core->term();
      const std::string cmd = "final" + std::to_string(seed_) + "." + std::to_string(attempt++);
      const uint64_t index = *node(leader).core->propose({cmd}, now_);
      process(leader);
      while (now_ < limit && failure_.empty()) {
        step(false);
        ++now_;
        bool all = true;
        for (uint32_t id : members_) all = all && node(id).sm.applied >= index;
        if (all) {
          const auto it = committed_.find(index);
          if (it != committed_.end() && it->second.command == cmd) return;  // live
          break;  // another leader's entry won that index: try again
        }
        const Node& l = node(leader);
        if (!l.core || l.core->role() != Role::kLeader || l.core->term() != term) break;
      }
    }
    if (failure_.empty()) {
      fail("liveness: no command proposed after healing reached every server in time");
    }
  }

  uint64_t seed_;
  SimConfig cfg_;
  std::mt19937_64 rng_;
  bool trace_ = false;
  bool healed_ = false;
  int64_t now_ = 0;
  uint64_t seq_ = 0;
  std::vector<uint32_t> members_;
  std::vector<Node> nodes_;
  std::vector<int> group_;  // partition side per id (index 0 unused)
  std::priority_queue<Packet, std::vector<Packet>, Later> net_;
  std::map<uint64_t, std::pair<uint32_t, int64_t>> elected_;  // term -> (leader, when)
  std::map<uint64_t, Committed> committed_;
  uint64_t max_committed_ = 0;
  std::vector<PendingRead> reads_;
  uint64_t next_command_ = 0;
  uint64_t installs_ = 0;
  uint64_t reads_ok_ = 0;
  uint64_t crashes_ = 0;
  std::string failure_;
};

// Per-seed parameters, themselves drawn from the seed, so the seeds cover
// gentle and hostile networks, frequent and rare compaction, small and
// large AppendEntries batches.
SimConfig config_for(uint64_t seed, int nodes) {
  std::mt19937_64 r(seed ^ 0xA5A5A5A5ull);
  auto pick = [&](double lo, double hi) { return std::uniform_real_distribution<double>(lo, hi)(r); };
  SimConfig c;
  c.nodes = nodes;
  c.drop = pick(0.0, 0.2);
  c.dup = pick(0.0, 0.05);
  c.max_delay = static_cast<int64_t>(pick(2, 40));
  c.long_delay = pick(0.0, 0.03);
  c.crash_per_ms = pick(0.0, 0.0006);
  c.crash_after_vote = pick(0.0, 0.3);
  c.partition_per_ms = pick(0.0, 0.002);
  const uint64_t compact_choices[] = {0, 3, 10, 40};
  c.compact_every = compact_choices[r() % 4];
  c.max_entries = 1 + r() % 16;
  return c;
}

struct SuiteTotals {
  size_t elections = 0, committed = 0;
  uint64_t installs = 0, reads = 0, crashes = 0;
};

// Runs seeds [first, first + count); returns the failures.
std::vector<std::string> run_seeds(uint64_t first, uint64_t count, int nodes, SuiteTotals* t) {
  std::vector<std::string> failures;
  for (uint64_t s = first; s < first + count; ++s) {
    Sim sim(s, config_for(s, nodes));
    std::string f = sim.run();
    if (!f.empty()) failures.push_back(f);
    t->elections += sim.elections();
    t->committed += sim.committed();
    t->installs += sim.snapshots_installed();
    t->reads += sim.reads_confirmed();
    t->crashes += sim.crashes();
  }
  return failures;
}

// 20 blocks x 100 seeds for 3 servers and 10 x 50 for 5: 2,500 seeds,
// split so ctest runs the blocks in parallel.
constexpr uint64_t kSeedsPerBlock3 = 100;
constexpr uint64_t kSeedsPerBlock5 = 50;

class RaftSim3 : public ::testing::TestWithParam<int> {};
class RaftSim5 : public ::testing::TestWithParam<int> {};

void run_block(int block, uint64_t per_block, int nodes, uint64_t base) {
  if (const char* one = std::getenv("FLEET_RAFT_SEED")) {
    if (block != 0) GTEST_SKIP() << "FLEET_RAFT_SEED runs one seed, in block 0";
    SuiteTotals t;
    for (const std::string& f : run_seeds(std::strtoull(one, nullptr, 10), 1, nodes, &t)) {
      ADD_FAILURE() << f;
    }
    return;
  }
  SuiteTotals t;
  // FLEET_RAFT_SEED_BASE shifts every block to fresh seeds, for sweeps
  // beyond the default 2,500 (NOTES.md m4b: ctest rerun with bases 0,
  // 10,000,000, ...). The default run always covers the same seeds.
  uint64_t shift = 0;
  if (const char* b = std::getenv("FLEET_RAFT_SEED_BASE")) shift = std::strtoull(b, nullptr, 10);
  const uint64_t first = base + shift + static_cast<uint64_t>(block) * per_block;
  const std::vector<std::string> failures = run_seeds(first, per_block, nodes, &t);
  for (size_t i = 0; i < failures.size() && i < 5; ++i) ADD_FAILURE() << failures[i];
  EXPECT_TRUE(failures.empty()) << failures.size() << " of " << per_block << " seeds failed";
  // The faults must actually have happened, or the test proves little.
  EXPECT_GT(t.elections, per_block);
  EXPECT_GT(t.committed, per_block * 10);
  std::printf("[ RaftSim ] %d servers, seeds %llu..%llu: %zu elections, %zu entries committed, "
              "%llu snapshot restores, %llu reads confirmed, %llu crashes\n",
              nodes, static_cast<unsigned long long>(first),
              static_cast<unsigned long long>(first + per_block - 1), t.elections, t.committed,
              static_cast<unsigned long long>(t.installs), static_cast<unsigned long long>(t.reads),
              static_cast<unsigned long long>(t.crashes));
}

TEST_P(RaftSim3, SafetyHoldsUnderLossReorderingPartitionsAndCrashes) {
  run_block(GetParam(), kSeedsPerBlock3, 3, 0);
}
TEST_P(RaftSim5, SafetyHoldsUnderLossReorderingPartitionsAndCrashes) {
  run_block(GetParam(), kSeedsPerBlock5, 5, 1'000'000);
}
INSTANTIATE_TEST_SUITE_P(Seeds, RaftSim3, ::testing::Range(0, 20));
INSTANTIATE_TEST_SUITE_P(Seeds, RaftSim5, ::testing::Range(0, 10));

}  // namespace
}  // namespace fleet::raft
