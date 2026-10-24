#pragma once
// Restart policies and their backoff schedule (m3).
//
// WHY THIS EXISTS. When a task's process exits on its own, the master has
// to decide whether that was the end of it. Three policies, as in
// Kubernetes' restartPolicy and Borg's job specs:
//   never       any exit is final (batch work that must not rerun)
//   on-failure  rerun unless it exited 0 (batch work that should finish)
//   always      rerun on any exit (a service that should always be up)
// A rerun does not happen at once: a task that crashes on start would
// otherwise be restarted in a tight loop, burning a placement, a fork and
// a log file per heartbeat, forever. So consecutive restarts wait an
// exponentially growing delay, capped:
//
//     delay(n) = min(max, base * 2^(n-1))   for the n-th restart in a row
//
// and the count goes back to zero once an attempt has stayed up for
// `reset_after`, so a service that crashes once a day restarts after
// `base`, not after the cap. Defaults: 1 s, doubling, capped at 60 s,
// reset after 60 s up. (Kubernetes' CrashLoopBackOff uses 10 s doubling to
// 5 min, reset after 10 min; the shape is the same, the numbers here are
// smaller so tests and benches run in seconds.)
//
// WHAT IS NOT AN EXIT. Preemption, drain and node death take the task
// away from a node; the task did nothing wrong. Those always reschedule
// it at once, whatever its policy, and never touch its backoff count.
// Only the task's own exit (or a failure to start) goes through here.
//
// Pure functions, header-only: tests/restart_policy_test.cpp checks the
// schedule directly.

#include <chrono>
#include <cstdint>
#include <optional>
#include <string_view>

namespace fleet {

enum class RestartPolicy : uint8_t { kNever, kOnFailure, kAlways };

inline std::string_view to_string(RestartPolicy p) {
  switch (p) {
    case RestartPolicy::kNever: return "never";
    case RestartPolicy::kOnFailure: return "on-failure";
    case RestartPolicy::kAlways: return "always";
  }
  return "?";
}

inline std::optional<RestartPolicy> parse_restart_policy(std::string_view s) {
  if (s == "never") return RestartPolicy::kNever;
  if (s == "on-failure") return RestartPolicy::kOnFailure;
  if (s == "always") return RestartPolicy::kAlways;
  return std::nullopt;
}

// Should an attempt that exited (succeeded = exit code 0, no signal, it
// did start) be run again?
inline bool should_restart(RestartPolicy p, bool succeeded) {
  switch (p) {
    case RestartPolicy::kNever: return false;
    case RestartPolicy::kOnFailure: return !succeeded;
    case RestartPolicy::kAlways: return true;
  }
  return false;
}

struct BackoffOptions {
  std::chrono::milliseconds base{1000};
  std::chrono::milliseconds max{60000};
  std::chrono::milliseconds reset_after{60000};
};

// Delay before the n-th consecutive restart, n >= 1 (n == 0 is treated as
// 1). Doubles by repeated comparison instead of computing base << (n-1),
// which would overflow for large n; the loop runs at most ~log2(max/base)
// times because it stops at the cap.
inline std::chrono::milliseconds backoff_delay(uint32_t n, const BackoffOptions& o) {
  std::chrono::milliseconds d = o.base;
  for (uint32_t i = 1; i < n && d < o.max; ++i) d *= 2;
  return d < o.max ? d : o.max;
}

}  // namespace fleet
