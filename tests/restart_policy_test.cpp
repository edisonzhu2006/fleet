// Tests for the restart policies and their exponential backoff schedule
// (master/restart_policy.h, m3). Pure functions: no clock, no master.

#include <gtest/gtest.h>

#include <vector>

#include "restart_policy.h"

namespace fleet {
namespace {

using namespace std::chrono_literals;

TEST(RestartPolicy, WhichExitsAreRestarted) {
  EXPECT_FALSE(should_restart(RestartPolicy::kNever, true));
  EXPECT_FALSE(should_restart(RestartPolicy::kNever, false));
  EXPECT_FALSE(should_restart(RestartPolicy::kOnFailure, true));
  EXPECT_TRUE(should_restart(RestartPolicy::kOnFailure, false));
  EXPECT_TRUE(should_restart(RestartPolicy::kAlways, true));
  EXPECT_TRUE(should_restart(RestartPolicy::kAlways, false));
}

TEST(RestartPolicy, ParsesTheFleetctlNames) {
  EXPECT_EQ(parse_restart_policy("never"), RestartPolicy::kNever);
  EXPECT_EQ(parse_restart_policy("on-failure"), RestartPolicy::kOnFailure);
  EXPECT_EQ(parse_restart_policy("always"), RestartPolicy::kAlways);
  EXPECT_FALSE(parse_restart_policy("sometimes"));
  for (RestartPolicy p : {RestartPolicy::kNever, RestartPolicy::kOnFailure, RestartPolicy::kAlways}) {
    EXPECT_EQ(parse_restart_policy(to_string(p)), p);
  }
}

TEST(Backoff, DoublesFromTheBaseAndStopsAtTheCap) {
  const BackoffOptions o{1000ms, 60000ms, 60000ms};
  std::vector<std::chrono::milliseconds> got;
  for (uint32_t n = 1; n <= 9; ++n) got.push_back(backoff_delay(n, o));
  const std::vector<std::chrono::milliseconds> want = {
      1000ms, 2000ms, 4000ms, 8000ms, 16000ms, 32000ms, 60000ms, 60000ms, 60000ms};
  EXPECT_EQ(got, want);
}

TEST(Backoff, EdgeCases) {
  const BackoffOptions o{1000ms, 60000ms, 60000ms};
  EXPECT_EQ(backoff_delay(0, o), 1000ms);  // treated as the first restart
  // A huge count neither overflows nor loops for long: the doubling stops
  // at the cap.
  EXPECT_EQ(backoff_delay(4'000'000'000u, o), 60000ms);
  // A cap that is not a power-of-two multiple of the base is still exact.
  EXPECT_EQ(backoff_delay(3, BackoffOptions{300ms, 1000ms, 0ms}), 1000ms);
  EXPECT_EQ(backoff_delay(2, BackoffOptions{300ms, 1000ms, 0ms}), 600ms);
  // base == max: constant delay; base 0: restart at once, every time.
  EXPECT_EQ(backoff_delay(5, BackoffOptions{500ms, 500ms, 0ms}), 500ms);
  EXPECT_EQ(backoff_delay(5, BackoffOptions{0ms, 1000ms, 0ms}), 0ms);
}

}  // namespace
}  // namespace fleet
