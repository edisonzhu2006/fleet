#pragma once
// FailureDetector: decides from heartbeat silence whether a node is alive,
// suspect or dead (m3).
//
// WHY THIS EXISTS. A master cannot tell a crashed machine from a slow one
// or from a broken network: all it ever observes is that heartbeats stop
// arriving. This class turns that one observation into a decision, and
// nothing else: it knows node ids and times, not tasks, epochs or gRPC.
// MasterCore acts on what it decides (reschedule, fence). Keeping it
// separate and clock-free (every call takes `now`) is what lets the tests
// drive the whole state machine with a fake clock, millisecond by
// millisecond, without sleeping.
//
// THE RULE. Agents heartbeat every `interval`. The number of heartbeats a
// node has missed is the whole intervals of silence since it was last
// heard from:
//
//     missed = floor((now - last_heard) / interval)
//
//     alive   --missed >= suspect_after-->  suspect
//     suspect --heartbeat----------------->  alive
//     alive or suspect --missed >= dead_after--> dead
//     dead    --reset() (a new registration)--> alive
//
// Dead is absorbing: a heartbeat from a dead node does not revive it. By
// the time a node is declared dead the master has rescheduled its tasks,
// so if it was only partitioned and comes back, it must not quietly resume
// as if nothing happened; MasterCore fences it (its epoch is revoked) and
// it has to register again, which calls reset(). With the defaults (500 ms
// heartbeats, 3 and 6 misses) a node is suspect after 1.5 s of silence and
// dead after 3 s.
//
// WHY COUNTED MISSES AND NOT PHI-ACCRUAL. A phi-accrual detector
// (Hayashibara et al., used by Cassandra and Akka) learns the distribution
// of inter-arrival times and reports a suspicion level, which adapts to a
// jittery network. fleet's agents run on one fixed cadence that the master
// itself sets at registration, so "6 missed beats" is a threshold an
// operator can reason about directly and the time to detect follows from
// two flags (NOTES.md m3 measures it). The cost: a fixed threshold cannot
// adapt, so it is tuned between false positives (a GC pause or a slow
// link declares a healthy node dead) and detection latency.
//
// Not thread-safe; MasterCore calls it under the master's lock.

#include <chrono>
#include <cstdint>
#include <string_view>
#include <vector>

#include "model.h"

namespace fleet {

using Clock = std::chrono::steady_clock;

enum class Health : uint8_t { kAlive, kSuspect, kDead };
std::string_view to_string(Health h);

struct FailureDetectorOptions {
  std::chrono::milliseconds interval{500};  // the agents' heartbeat cadence
  uint32_t suspect_after = 3;               // missed beats before suspect
  uint32_t dead_after = 6;                  // missed beats before dead
};

struct HealthChange {
  NodeId node = 0;
  Health from = Health::kAlive;
  Health to = Health::kAlive;
  uint32_t missed = 0;              // misses counted when it changed
  Clock::duration silence{};        // now - last_heard at that moment
};

class FailureDetector {
 public:
  // Throws std::invalid_argument unless interval > 0 and
  // 1 <= suspect_after < dead_after: suspicion must come strictly before
  // death, or the suspect state could never be observed.
  explicit FailureDetector(FailureDetectorOptions options = {});

  // A node registered (new, or a known node registering again): alive and
  // heard from now. Grows the table when `node` is new; ids are dense.
  void reset(NodeId node, Clock::time_point now);

  // A heartbeat that MasterCore accepted (current epoch) arrived. Returns
  // the change if it cleared suspicion (suspect -> alive). A dead node
  // stays dead and its last_heard is not updated; see the class comment.
  std::vector<HealthChange> heard_from(NodeId node, Clock::time_point now);

  // Applies the silence up to `now` to every node and returns the state
  // changes, in node id order. A node can go straight from alive to dead
  // if evaluate() was not called in between (the master's tick stalled);
  // that is reported as one change.
  std::vector<HealthChange> evaluate(Clock::time_point now);

  Health health(NodeId node) const { return nodes_.at(node).health; }
  Clock::time_point last_heard(NodeId node) const { return nodes_.at(node).last_heard; }
  uint32_t missed(NodeId node, Clock::time_point now) const;
  size_t size() const { return nodes_.size(); }
  const FailureDetectorOptions& options() const { return options_; }

 private:
  struct Entry {
    Health health = Health::kAlive;
    Clock::time_point last_heard{};
  };
  FailureDetectorOptions options_;
  std::vector<Entry> nodes_;  // indexed by NodeId
};

}  // namespace fleet
