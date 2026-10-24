#pragma once
// Helpers shared by the m4 durability tests (wal_test.cpp,
// recovery_test.cpp, master_restart_test.cpp): replay a record list into
// a fresh MasterState, compare two states through their snapshots, and a
// per-test scratch directory under /private/tmp/fleet.

#include <gtest/gtest.h>

#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>
#include <google/protobuf/util/message_differencer.h>

#include "master_state.h"
#include "wal.pb.h"

namespace fleet::testing {

// A fresh directory for this test, removed first if a previous run left it.
inline std::filesystem::path scratch_dir(const std::string& tag) {
  const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
  std::filesystem::path p = std::filesystem::path("/private/tmp/fleet") /
                            (tag + "-" + std::to_string(::getpid()) + "-" +
                             info->test_suite_name() + "." + info->name());
  std::filesystem::remove_all(p);
  std::filesystem::create_directories(p);
  return p;
}

// Applies records in order to `state`; returns the first error.
inline std::string apply_all(MasterState& state, const std::vector<v1::WalRecord>& records) {
  for (const v1::WalRecord& r : records) {
    if (std::string e = state.apply(r); !e.empty()) return e;
  }
  return {};
}

// Empty if the two states have equal snapshots, otherwise the field-level
// differences. Snapshots hold all durable state, and derived structures
// are covered separately by check_invariants(), so equal snapshots plus
// clean invariants means equal states.
// Deterministic serialization (map entries sorted by key), so two equal
// messages give equal bytes.
inline std::string canonical_bytes(const google::protobuf::Message& m) {
  std::string out;
  google::protobuf::io::StringOutputStream sos(&out);
  google::protobuf::io::CodedOutputStream cos(&sos);
  cos.SetSerializationDeterministic(true);
  m.SerializeToCodedStream(&cos);
  cos.Trim();
  return out;
}

inline std::string state_diff(const MasterState& a, const MasterState& b) {
  v1::Snapshot sa, sb;
  a.to_snapshot(&sa);
  b.to_snapshot(&sb);
  // Fast path: compare canonical bytes; only a mismatch pays for the
  // field-by-field report.
  if (canonical_bytes(sa) == canonical_bytes(sb)) return {};
  google::protobuf::util::MessageDifferencer d;
  std::string diff;
  d.ReportDifferencesToString(&diff);
  return d.Compare(sa, sb) ? std::string() : (diff.empty() ? "differ" : diff);
}

}  // namespace fleet::testing

#include "master_test_helpers.h"

namespace fleet::testing {

// Options for durability tests: the fake steady clock doubles as the wall
// clock (offset 0), and backoffs are short.
inline MasterOptions durable_test_options() {
  MasterOptions o;
  o.wall_offset_ms = 0;
  o.backoff = {std::chrono::milliseconds(100), std::chrono::milliseconds(400),
               std::chrono::seconds(10)};
  return o;
}

// Drives a master through every kind of state change, so that its log
// holds every record type and its state has tasks in every state (pending,
// running, backoff, succeeded, failed, killed, moving on a draining node)
// and a dead node. Used by the snapshot and storage round-trip tests.
inline void run_busy_scenario(MasterCore& core) {
  using namespace std::chrono_literals;
  FakeAgent a = agent_for(register_node(core, "a"));
  FakeAgent b = agent_for(register_node(core, "b"));
  FakeAgent c = agent_for(register_node(core, "c"));
  submit(core, "big", 3, 4000, 512);                                       // 0
  submit(core, "svc", 1, 1000, 128, 2, v1::RESTART_POLICY_ON_FAILURE);     // 1, 2
  submit(core, "batch", 0, 1000, 128, 3);                                  // 3, 4, 5
  core.schedule(kT0);  // 0 -> a; 1, 2, 3, 4 -> b; 5 -> c
  submit(core, "crit", 3, 4000, 512);  // 6: nothing fits; evicts batch 5 on c
  core.schedule(kT0);
  a.running(0, 1, a.epoch);
  a.beat(core, kT0 + 100ms);
  c.running(6, 1, c.epoch);
  c.beat(core, kT0 + 100ms);
  b.running(1, 1, b.epoch);
  b.running(2, 1, b.epoch);
  b.running(3, 1, b.epoch);
  b.running(4, 1, b.epoch);
  b.beat(core, kT0 + 100ms);
  // svc 1 fails (on-failure: backoff), batch 3 succeeds, batch 4 fails.
  b.exited(1, 1, b.epoch, 1);
  b.running(2, 1, b.epoch);
  b.exited(3, 1, b.epoch, 0);
  b.exited(4, 1, b.epoch, 2);
  b.beat(core, kT0 + 200ms);
  // Kill svc 2; drain c (crit 6 is told to stop: moving).
  v1::KillResponse kr;
  v1::KillRequest kq;
  kq.set_task_id(2);
  EXPECT_TRUE(core.kill(kq, &kr).ok());
  v1::DrainRequest dq;
  dq.set_node("c");
  v1::DrainResponse dr;
  EXPECT_TRUE(core.drain(dq, &dr).ok());
  c.running(6, 1, c.epoch);
  c.beat(core, kT0 + 300ms);
  // a goes silent and dies; its task 0 goes back to pending. Backoff of
  // svc 1 is over by then too. b and c keep beating.
  b.beat(core, kT0 + 2900ms);
  c.running(6, 1, c.epoch);
  c.beat(core, kT0 + 2900ms);
  core.tick(kT0 + 3100ms);  // a dead; svc 1 out of backoff; scheduling pass
  // A new node takes what is still pending; svc 1 fails again there and
  // backs off; one more submission stays pending.
  FakeAgent d = agent_for(register_node(core, "d", 4000, 4096, kT0 + 3100ms));
  core.tick(kT0 + 3150ms);
  d.exited(1, core.attempt(1), d.epoch, 3);
  d.beat(core, kT0 + 3200ms);
  submit(core, "wait", 0, 4000, 512);
}

}  // namespace fleet::testing
