#pragma once
// The data model of fleet: resources, nodes, tasks, quotas (milestone 1).
//
// WHY THIS EXISTS. Everything else (the scheduler, the master, the
// simulator) talks about the same few nouns. Keeping them in one small
// header of plain structs, with no behavior beyond arithmetic, means the
// rules about how they change live in exactly one place: ClusterState.
//
// UNITS. CPU is in milli-CPU (1000 = one core) and memory in MB, both as
// int64_t, the same units Borg and Kubernetes use. Integers instead of
// doubles so that "does it fit" and "is the node over capacity" are exact:
// with doubles, adding and removing the same request a thousand times can
// leave a node at 1e-13 over capacity and the invariant checker would have
// to guess at a tolerance.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <string_view>

namespace fleet {

struct Resources {
  int64_t milli_cpu = 0;
  int64_t memory_mb = 0;

  friend bool operator==(const Resources&, const Resources&) = default;

  Resources& operator+=(const Resources& o) {
    milli_cpu += o.milli_cpu;
    memory_mb += o.memory_mb;
    return *this;
  }
  Resources& operator-=(const Resources& o) {
    milli_cpu -= o.milli_cpu;
    memory_mb -= o.memory_mb;
    return *this;
  }
  friend Resources operator+(Resources a, const Resources& b) { return a += b; }
  friend Resources operator-(Resources a, const Resources& b) { return a -= b; }

  // True when every dimension of *this is <= the same dimension of other.
  // "Fits" is per dimension: 2 free cores and 1 GB free does not fit a task
  // needing 1 core and 2 GB, no matter how the totals compare.
  bool fits_in(const Resources& other) const {
    return milli_cpu <= other.milli_cpu && memory_mb <= other.memory_mb;
  }
};

std::string to_string(const Resources& r);

// Labels are key=value pairs ("zone=us-east", "disk=ssd"). A task's
// required labels must all be present with equal values on the node.
// std::map rather than unordered_map: label sets are tiny (a handful of
// entries), and ordered iteration makes printing and comparison stable.
using Labels = std::map<std::string, std::string>;

// Ids are dense indexes into ClusterState's vectors (0, 1, 2, ...), so
// lookup is an array access: O(1) with no hashing, and iteration in id order
// is free, which is what makes every tie-break deterministic.
using NodeId = uint32_t;
using TaskId = uint32_t;

enum class NodeState : uint8_t {
  kReady,     // accepts new tasks
  kDraining,  // operator is taking it out of service: no new tasks
  kDead,      // failure detector gave up on it (m3): no tasks at all
};

enum class TaskState : uint8_t {
  kPending,    // admitted, waiting for a node (new, preempted, or node lost)
  kRunning,    // placed on a node; its request is counted in node.allocated
  kSucceeded,  // terminal: exited 0
  kFailed,     // terminal: exited non-zero (restart policy is m3)
  kKilled,     // terminal: user killed it
};

// Job names: [A-Za-z0-9._-]{1,64}, and not "." or "..". They end up in
// log file names on agents, so anything that could be a path ("../x",
// "a/b") is rejected. The master checks at admission and the agent checks
// again before using one, because the agent should not trust the network.
inline bool valid_job_name(std::string_view job) {
  if (job.empty() || job.size() > 64 || job == "." || job == "..") return false;
  for (char c : job) {
    const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
    if (!ok) return false;
  }
  return true;
}

// Container image references ("busybox:1.36", "ghcr.io/x/y@sha256:...").
// Not a full parse of Docker's grammar, which the daemon does anyway; the
// point is what reaches the docker CLI's argv. The image sits right before
// the task's command in `docker run ... IMAGE CMD`, so an "image" of
// "--privileged" would be read by docker as a flag. So: 1..255 chars,
// first one alphanumeric (never '-'), the rest from the reference
// alphabet. Empty means "the runtime's default" and is checked by callers.
inline bool valid_image_ref(std::string_view image) {
  if (image.empty() || image.size() > 255) return false;
  auto alnum = [](char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9');
  };
  if (!alnum(image.front())) return false;
  for (char c : image) {
    if (!alnum(c) && c != '.' && c != '_' && c != '-' && c != '/' && c != ':' && c != '@') {
      return false;
    }
  }
  return true;
}

std::string_view to_string(NodeState s);
std::string_view to_string(TaskState s);

inline bool is_terminal(TaskState s) {
  return s == TaskState::kSucceeded || s == TaskState::kFailed ||
         s == TaskState::kKilled;
}

// Priority bands, matching the plan: higher number wins. Kept as a plain
// int in Task (0..3) so comparisons read naturally; these names are for
// call sites and printing.
inline constexpr int kPriorityBatch = 0;
inline constexpr int kPriorityNormal = 1;
inline constexpr int kPriorityProduction = 2;
inline constexpr int kPriorityCritical = 3;
inline constexpr int kMaxPriority = kPriorityCritical;

struct Node {
  NodeId id = 0;
  Resources capacity;
  Labels labels;
  NodeState state = NodeState::kReady;
  // Sum of the requests of the tasks running here. Maintained incrementally
  // by ClusterState on every place/evict/finish so the scheduler reads it in
  // O(1); check_invariants() recomputes it from scratch to catch drift.
  Resources allocated;

  Resources free() const { return capacity - allocated; }
};

// What a user submits. Separate from Task so that admission can validate a
// spec before any id is assigned or any state is touched.
struct TaskSpec {
  std::string job;
  int priority = kPriorityNormal;
  Resources request;
  Labels required_labels;
};

struct Task {
  TaskId id = 0;
  std::string job;
  int priority = kPriorityNormal;
  Resources request;
  Labels required_labels;
  TaskState state = TaskState::kPending;
  std::optional<NodeId> node;  // set exactly when state == kRunning
  // How many times this task was evicted by preemption. Not needed for
  // correctness; the simulator reports it, and Borg uses the same signal to
  // tell users their batch work is being churned.
  uint32_t preempted_count = 0;
};

// Per-job cap on the total request of the job's live (pending + running)
// tasks. Charged at admission, not at placement: a job over quota is told
// so immediately instead of waiting in the pending queue forever. A job
// nobody set a quota for has no limit (limit == nullopt) but its usage is
// still tracked, so a quota can be set later and the invariant checker can
// verify usage for every job.
struct JobQuota {
  std::optional<Resources> limit;
  Resources used;  // sum of requests of the job's non-terminal tasks
};

}  // namespace fleet
