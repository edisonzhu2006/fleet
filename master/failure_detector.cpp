// FailureDetector implementation: the alive / suspect / dead state machine
// over heartbeat silence. See failure_detector.h for the rule and why.

#include "failure_detector.h"

#include <stdexcept>

namespace fleet {

std::string_view to_string(Health h) {
  switch (h) {
    case Health::kAlive: return "alive";
    case Health::kSuspect: return "suspect";
    case Health::kDead: return "dead";
  }
  return "?";
}

FailureDetector::FailureDetector(FailureDetectorOptions options) : options_(options) {
  if (options_.interval.count() <= 0 || options_.suspect_after < 1 ||
      options_.dead_after <= options_.suspect_after) {
    throw std::invalid_argument(
        "failure detector needs interval > 0 and 1 <= suspect_after < dead_after");
  }
}

void FailureDetector::reset(NodeId node, Clock::time_point now) {
  if (node >= nodes_.size()) nodes_.resize(node + 1);
  nodes_[node] = Entry{Health::kAlive, now};
}

uint32_t FailureDetector::missed(NodeId node, Clock::time_point now) const {
  const Clock::duration silence = now - nodes_.at(node).last_heard;
  // A heartbeat stamped after `now` (callers pass times taken at slightly
  // different moments) is zero silence, not a negative count.
  if (silence.count() <= 0) return 0;
  // Integer division of durations is the floor of whole intervals missed.
  // Clamped so a node silent for years does not overflow the uint32.
  const auto n = silence / options_.interval;
  return n > 1'000'000 ? 1'000'000u : static_cast<uint32_t>(n);
}

std::vector<HealthChange> FailureDetector::heard_from(NodeId node, Clock::time_point now) {
  Entry& e = nodes_.at(node);
  if (e.health == Health::kDead) return {};  // only reset() revives
  std::vector<HealthChange> out;
  if (e.health == Health::kSuspect) {
    out.push_back({node, Health::kSuspect, Health::kAlive, missed(node, now),
                   now - e.last_heard});
    e.health = Health::kAlive;
  }
  // Never move last_heard backwards: a delayed handler holding an older
  // `now` must not make a node look more silent than it is.
  if (now > e.last_heard) e.last_heard = now;
  return out;
}

std::vector<HealthChange> FailureDetector::evaluate(Clock::time_point now) {
  std::vector<HealthChange> out;
  for (NodeId id = 0; id < nodes_.size(); ++id) {
    Entry& e = nodes_[id];
    if (e.health == Health::kDead) continue;
    const uint32_t m = missed(id, now);
    Health next = e.health;
    if (m >= options_.dead_after) {
      next = Health::kDead;
    } else if (m >= options_.suspect_after) {
      next = Health::kSuspect;
    }
    // Suspect only ever clears through heard_from(): silence that keeps
    // growing cannot make a node less suspect.
    if (next != e.health) {
      out.push_back({id, e.health, next, m, now - e.last_heard});
      e.health = next;
    }
  }
  return out;
}

}  // namespace fleet
