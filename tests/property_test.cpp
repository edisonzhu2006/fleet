// Property test: thousands of random operations (submit, schedule, finish,
// kill, evict, direct place, node drain/death/recovery, new nodes, quota
// changes) from a fixed seed, with check_invariants() after every single
// one. Where the unit tests check specific scenarios, this checks that no
// interleaving of operations can make the bookkeeping drift, overcommit a
// node, leave a task on a dead node, or preempt equal or higher priority.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "cluster_state.h"
#include "scheduler.h"

namespace fleet {
namespace {

class Rand {
 public:
  explicit Rand(uint64_t seed) : x_(seed) {}
  uint64_t operator()(uint64_t mod) {
    x_ = x_ * 6364136223846793005ULL + 1442695040888963407ULL;
    return (x_ >> 33) % mod;
  }

 private:
  uint64_t x_;
};

// Picks a random task in the given state, or returns false if none.
bool random_task(const ClusterState& st, Rand& rnd, TaskState want, TaskId& out) {
  std::vector<TaskId> ids;
  for (const Task& t : st.tasks()) {
    if (t.state == want) ids.push_back(t.id);
  }
  if (ids.empty()) return false;
  out = ids[rnd(ids.size())];
  return true;
}

struct Counters {
  int submits = 0, rejected = 0, passes = 0, placements = 0, preemptions = 0;
  int finishes = 0, kills = 0, evictions = 0, direct_places = 0;
  int node_changes = 0;
};

Counters run_random_ops(uint64_t seed, int ops) {
  Rand rnd(seed);
  ClusterState st;
  Counters c;
  for (int i = 0; i < 8; ++i) {
    st.add_node({static_cast<int64_t>(4000 + 4000 * rnd(4)),
                 static_cast<int64_t>(8192 * (1 + rnd(4)))},
                rnd(3) == 0 ? Labels{{"disk", "ssd"}} : Labels{});
  }
  st.set_quota("job0", {20000, 40960});
  st.set_quota("job1", {8000, 16384});

  for (int op = 0; op < ops; ++op) {
    const uint64_t kind = rnd(100);
    TaskId t = 0;
    if (kind < 35) {
      TaskSpec spec;
      spec.job = "job" + std::to_string(rnd(4));
      spec.priority = static_cast<int>(rnd(4));
      spec.request = {static_cast<int64_t>(250 * (1 + rnd(16))),
                      static_cast<int64_t>(512 * (1 + rnd(16)))};
      if (rnd(10) == 0) spec.required_labels["disk"] = "ssd";
      ++c.submits;
      if (!st.submit(spec).ok) ++c.rejected;
    } else if (kind < 50) {
      SchedulerOptions o;
      o.strategy = static_cast<Strategy>(rnd(3));
      auto r = Scheduler(o).schedule_pending(st);
      ++c.passes;
      c.placements += static_cast<int>(r.placements.size());
      c.preemptions += static_cast<int>(r.preemptions.size());
      for (const auto& p : r.preemptions) {
        // The core promise of preemption, checked on every event.
        EXPECT_LT(st.task(p.victim).priority, st.task(p.preemptor).priority)
            << "op " << op;
      }
    } else if (kind < 65) {
      if (random_task(st, rnd, TaskState::kRunning, t)) {
        EXPECT_TRUE(st.finish(t, rnd(4) != 0));
        ++c.finishes;
      }
    } else if (kind < 70) {
      const TaskState s = rnd(2) ? TaskState::kRunning : TaskState::kPending;
      if (random_task(st, rnd, s, t)) {
        EXPECT_TRUE(st.kill(t));
        ++c.kills;
      }
    } else if (kind < 75) {
      if (random_task(st, rnd, TaskState::kRunning, t)) {
        EXPECT_TRUE(st.evict(t));
        ++c.evictions;
      }
    } else if (kind < 87) {
      // A direct placement that may well be illegal: place() must refuse
      // anything that would break an invariant.
      if (random_task(st, rnd, TaskState::kPending, t)) {
        if (st.place(t, static_cast<NodeId>(rnd(st.nodes().size())))) {
          ++c.direct_places;
        }
      }
    } else if (kind < 95) {
      const NodeState s = static_cast<NodeState>(rnd(3));
      st.set_node_state(static_cast<NodeId>(rnd(st.nodes().size())), s);
      ++c.node_changes;
    } else if (kind < 98) {
      if (st.nodes().size() < 24) {
        st.add_node({static_cast<int64_t>(4000 + 4000 * rnd(4)),
                     static_cast<int64_t>(8192 * (1 + rnd(4)))});
      }
    } else {
      // Only job0/job1 ever get quotas; job2/job3 stay unlimited so the
      // cluster still fills up and preemption gets exercised.
      st.set_quota("job" + std::to_string(rnd(2)),
                   {static_cast<int64_t>(4000 * (1 + rnd(8))),
                    static_cast<int64_t>(8192 * (1 + rnd(8)))});
    }
    const std::string err = st.check_invariants();
    if (!err.empty()) {
      ADD_FAILURE() << "invariant violated after op " << op << " (kind "
                    << kind << "):\n" << err;
      break;
    }
  }
  return c;
}

TEST(Property, InvariantsHoldUnderRandomOperations) {
  for (uint64_t seed : {1u, 2u, 3u}) {
    Counters c = run_random_ops(seed, 5000);
    // Make sure the sequence actually exercised every path.
    EXPECT_GT(c.passes, 500) << "seed " << seed;
    EXPECT_GT(c.placements, 500) << "seed " << seed;
    EXPECT_GT(c.preemptions, 20) << "seed " << seed;
    EXPECT_GT(c.rejected, 10) << "seed " << seed;
    EXPECT_GT(c.direct_places, 10) << "seed " << seed;
    EXPECT_GT(c.finishes, 200) << "seed " << seed;
    EXPECT_GT(c.kills, 50) << "seed " << seed;
    EXPECT_GT(c.evictions, 50) << "seed " << seed;
    EXPECT_GT(c.node_changes, 100) << "seed " << seed;
    std::printf("seed %llu: %d submits (%d rejected), %d passes, %d placements, "
                "%d preemptions, %d direct places, %d node state changes\n",
                static_cast<unsigned long long>(seed), c.submits, c.rejected,
                c.passes, c.placements, c.preemptions, c.direct_places,
                c.node_changes);
  }
}

}  // namespace
}  // namespace fleet
