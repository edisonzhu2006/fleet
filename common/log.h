#pragma once
// Timestamped event lines for fleetmaster and fleetlet.
//
// WHY THIS EXISTS. bench/local_cluster.sh measures how long a placement
// takes to become a running process by matching the master's "placed"
// line against the agent's "started" line, so both binaries stamp their
// lines from the same clock (wall time, millisecond resolution; both run
// on one machine, so the clocks agree).

#include <chrono>
#include <cstdio>
#include <ctime>
#include <string>

namespace fleet {

// "HH:MM:SS.mmm" local time.
inline std::string log_timestamp() {
  const auto now = std::chrono::system_clock::now();
  const std::time_t secs = std::chrono::system_clock::to_time_t(now);
  const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now.time_since_epoch()).count() % 1000;
  std::tm tm {};
  localtime_r(&secs, &tm);
  char buf[32];
  std::snprintf(buf, sizeof(buf), "%02d:%02d:%02d.%03d", tm.tm_hour, tm.tm_min, tm.tm_sec,
                static_cast<int>(ms));
  return buf;
}

// One line to stderr: "<time> <who>: <line>". A single fprintf call, so
// lines from different threads do not interleave mid-line.
inline void log_line(const std::string& who, const std::string& line) {
  std::fprintf(stderr, "%s %s: %s\n", log_timestamp().c_str(), who.c_str(), line.c_str());
}

}  // namespace fleet
