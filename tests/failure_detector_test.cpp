// Tests for FailureDetector, the alive / suspect / dead state machine
// (m3), driven entirely by a fake clock: every call takes an explicit
// time point, so the tests step time in milliseconds and never sleep.

#include <gtest/gtest.h>

#include <stdexcept>

#include "failure_detector.h"

namespace fleet {
namespace {

using namespace std::chrono_literals;

const Clock::time_point kT0{};  // the fake clock's origin

FailureDetector make(uint32_t suspect = 3, uint32_t dead = 6) {
  return FailureDetector({500ms, suspect, dead});
}

TEST(FailureDetector, MissedIsWholeIntervalsOfSilence) {
  FailureDetector fd = make();
  fd.reset(0, kT0);
  EXPECT_EQ(fd.missed(0, kT0), 0u);
  EXPECT_EQ(fd.missed(0, kT0 + 499ms), 0u);
  EXPECT_EQ(fd.missed(0, kT0 + 500ms), 1u);
  EXPECT_EQ(fd.missed(0, kT0 + 1499ms), 2u);
  EXPECT_EQ(fd.missed(0, kT0 + 1500ms), 3u);
  // A time before the last heartbeat (a caller that read its clock a
  // little earlier) is zero silence, not a huge unsigned count.
  fd.heard_from(0, kT0 + 2000ms);
  EXPECT_EQ(fd.missed(0, kT0 + 1000ms), 0u);
}

TEST(FailureDetector, SuspectAtThreeMissedDeadAtSixWithAFakeClock) {
  FailureDetector fd = make();
  fd.reset(0, kT0);
  // Step the clock 10 ms at a time and record exactly when each change
  // happens: 3 x 500 ms and 6 x 500 ms after the last heartbeat.
  Clock::time_point suspect_at{}, dead_at{};
  for (auto t = kT0; t <= kT0 + 4000ms; t += 10ms) {
    for (const HealthChange& c : fd.evaluate(t)) {
      EXPECT_EQ(c.node, 0u);
      if (c.to == Health::kSuspect) {
        EXPECT_EQ(c.from, Health::kAlive);
        EXPECT_EQ(c.missed, 3u);
        suspect_at = t;
      } else if (c.to == Health::kDead) {
        EXPECT_EQ(c.from, Health::kSuspect);
        EXPECT_EQ(c.missed, 6u);
        EXPECT_EQ(c.silence, 3000ms);
        dead_at = t;
      } else {
        ADD_FAILURE() << "unexpected change to " << to_string(c.to);
      }
    }
  }
  EXPECT_EQ(suspect_at, kT0 + 1500ms);
  EXPECT_EQ(dead_at, kT0 + 3000ms);
  EXPECT_EQ(fd.health(0), Health::kDead);
}

TEST(FailureDetector, RegularHeartbeatsKeepANodeAliveForever) {
  FailureDetector fd = make();
  fd.reset(0, kT0);
  // Heartbeats every 500 ms, each arriving 480 ms late (just under one
  // interval of jitter): never even suspect.
  for (int i = 1; i <= 200; ++i) {
    const auto beat = kT0 + i * 500ms + 480ms;
    EXPECT_TRUE(fd.evaluate(beat - 1ms).empty());
    fd.heard_from(0, beat);
  }
  EXPECT_EQ(fd.health(0), Health::kAlive);
}

TEST(FailureDetector, HeartbeatClearsSuspicion) {
  FailureDetector fd = make();
  fd.reset(0, kT0);
  ASSERT_EQ(fd.evaluate(kT0 + 2000ms).size(), 1u);
  ASSERT_EQ(fd.health(0), Health::kSuspect);
  const auto changes = fd.heard_from(0, kT0 + 2100ms);
  ASSERT_EQ(changes.size(), 1u);
  EXPECT_EQ(changes[0].from, Health::kSuspect);
  EXPECT_EQ(changes[0].to, Health::kAlive);
  EXPECT_EQ(changes[0].silence, 2100ms);
  EXPECT_EQ(fd.health(0), Health::kAlive);
  // The silence clock restarted at the heartbeat: dead only 3 s after it.
  EXPECT_TRUE(fd.evaluate(kT0 + 2100ms + 1499ms).empty());
  EXPECT_EQ(fd.evaluate(kT0 + 2100ms + 1500ms).size(), 1u);  // suspect again
  EXPECT_EQ(fd.evaluate(kT0 + 2100ms + 3000ms).size(), 1u);  // dead
  EXPECT_EQ(fd.health(0), Health::kDead);
  // A second heartbeat on an alive node reports no change.
  fd.reset(1, kT0);
  EXPECT_TRUE(fd.heard_from(1, kT0 + 100ms).empty());
}

TEST(FailureDetector, DeadIsAbsorbingUntilReset) {
  FailureDetector fd = make();
  fd.reset(0, kT0);
  fd.evaluate(kT0 + 3000ms);
  ASSERT_EQ(fd.health(0), Health::kDead);
  // A heartbeat from a dead node (it was partitioned, not crashed) does
  // not revive it and does not move its last-heard time.
  EXPECT_TRUE(fd.heard_from(0, kT0 + 3500ms).empty());
  EXPECT_EQ(fd.health(0), Health::kDead);
  EXPECT_EQ(fd.last_heard(0), kT0);
  EXPECT_TRUE(fd.evaluate(kT0 + 9000ms).empty());  // reported once, not every tick
  // Only a new registration does.
  fd.reset(0, kT0 + 9000ms);
  EXPECT_EQ(fd.health(0), Health::kAlive);
  EXPECT_EQ(fd.missed(0, kT0 + 9000ms), 0u);
}

TEST(FailureDetector, AStalledEvaluatorGoesStraightFromAliveToDead) {
  FailureDetector fd = make();
  fd.reset(0, kT0);
  // Nothing evaluated for 10 s (say, the master's tick thread stalled):
  // one change, alive -> dead, carrying the full silence.
  const auto changes = fd.evaluate(kT0 + 10s);
  ASSERT_EQ(changes.size(), 1u);
  EXPECT_EQ(changes[0].from, Health::kAlive);
  EXPECT_EQ(changes[0].to, Health::kDead);
  EXPECT_EQ(changes[0].missed, 20u);
}

TEST(FailureDetector, NodesAreIndependentAndReportedInIdOrder) {
  FailureDetector fd = make();
  for (NodeId n = 0; n < 4; ++n) fd.reset(n, kT0);
  fd.heard_from(1, kT0 + 2000ms);
  fd.heard_from(3, kT0 + 2000ms);
  const auto changes = fd.evaluate(kT0 + 3200ms);
  ASSERT_EQ(changes.size(), 2u);
  EXPECT_EQ(changes[0].node, 0u);
  EXPECT_EQ(changes[1].node, 2u);
  EXPECT_EQ(fd.health(0), Health::kDead);
  EXPECT_EQ(fd.health(1), Health::kAlive);
  EXPECT_EQ(fd.health(2), Health::kDead);
  EXPECT_EQ(fd.health(3), Health::kAlive);
}

TEST(FailureDetector, LastHeardNeverMovesBackwards) {
  FailureDetector fd = make();
  fd.reset(0, kT0);
  fd.heard_from(0, kT0 + 1000ms);
  fd.heard_from(0, kT0 + 400ms);  // a handler that read its clock earlier
  EXPECT_EQ(fd.last_heard(0), kT0 + 1000ms);
}

TEST(FailureDetector, RejectsThresholdsThatCannotWork) {
  EXPECT_THROW(FailureDetector({500ms, 0, 6}), std::invalid_argument);
  EXPECT_THROW(FailureDetector({500ms, 6, 6}), std::invalid_argument);
  EXPECT_THROW(FailureDetector({500ms, 7, 6}), std::invalid_argument);
  EXPECT_THROW(FailureDetector({0ms, 3, 6}), std::invalid_argument);
  EXPECT_NO_THROW(FailureDetector({500ms, 1, 2}));
}

}  // namespace
}  // namespace fleet
