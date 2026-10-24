#pragma once
// Small helpers shared by the fleet_core tests: terse task submission and a
// macro that checks ClusterState invariants with the violations printed.

#include <gtest/gtest.h>

#include <string>

#include "cluster_state.h"

namespace fleet::testing {

// Submits a task and fails the test if admission rejects it.
inline TaskId submit_ok(ClusterState& st, const std::string& job, int priority,
                        Resources request, Labels labels = {}) {
  TaskSpec spec{job, priority, request, std::move(labels)};
  SubmitResult r = st.submit(spec);
  EXPECT_TRUE(r.ok) << r.reason;
  return r.id;
}

}  // namespace fleet::testing

// EXPECT that check_invariants() reports nothing; prints what broke if not.
#define EXPECT_INVARIANTS(st)                                   \
  do {                                                          \
    const std::string fleet_inv_err = (st).check_invariants();  \
    EXPECT_TRUE(fleet_inv_err.empty()) << fleet_inv_err;        \
  } while (0)
