#pragma once
// Conversions between core/ types and their Protobuf twins (m2, moved here
// in m4 when a second file, master_state.cpp, needed them).
//
// WHY A HEADER OF ITS OWN. core/ stays free of Protobuf (the simulator and
// the scheduler tests never see a generated type), so the mapping lives on
// the master side. Both the RPC layer (master_core.cpp) and the storage
// layer (master_state.cpp: log records and snapshots) translate the same
// enums, and one copy means they cannot disagree. Each from_proto returns
// nullopt for UNSPECIFIED or for a value this build does not know (a newer
// writer), so a caller decides whether that is an error.

#include <optional>
#include <string>

#include "fleet.pb.h"
#include "model.h"
#include "restart_policy.h"

namespace fleet {

inline Resources from_proto(const v1::Resources& r) {
  return Resources{r.milli_cpu(), r.memory_mb()};
}

inline void to_proto(const Resources& r, v1::Resources* out) {
  out->set_milli_cpu(r.milli_cpu);
  out->set_memory_mb(r.memory_mb);
}

inline Labels from_proto(const google::protobuf::Map<std::string, std::string>& m) {
  return Labels(m.begin(), m.end());
}

inline v1::NodeState to_proto(NodeState s) {
  switch (s) {
    case NodeState::kReady: return v1::NODE_STATE_READY;
    case NodeState::kDraining: return v1::NODE_STATE_DRAINING;
    case NodeState::kDead: return v1::NODE_STATE_DEAD;
  }
  return v1::NODE_STATE_UNSPECIFIED;
}

inline std::optional<NodeState> from_proto(v1::NodeState s) {
  switch (s) {
    case v1::NODE_STATE_READY: return NodeState::kReady;
    case v1::NODE_STATE_DRAINING: return NodeState::kDraining;
    case v1::NODE_STATE_DEAD: return NodeState::kDead;
    default: return std::nullopt;
  }
}

inline v1::TaskState to_proto(TaskState s) {
  switch (s) {
    case TaskState::kPending: return v1::TASK_STATE_PENDING;
    case TaskState::kRunning: return v1::TASK_STATE_RUNNING;
    case TaskState::kSucceeded: return v1::TASK_STATE_SUCCEEDED;
    case TaskState::kFailed: return v1::TASK_STATE_FAILED;
    case TaskState::kKilled: return v1::TASK_STATE_KILLED;
    case TaskState::kBackoff: return v1::TASK_STATE_BACKOFF;
  }
  return v1::TASK_STATE_UNSPECIFIED;
}

inline std::optional<TaskState> from_proto(v1::TaskState s) {
  switch (s) {
    case v1::TASK_STATE_PENDING: return TaskState::kPending;
    case v1::TASK_STATE_RUNNING: return TaskState::kRunning;
    case v1::TASK_STATE_SUCCEEDED: return TaskState::kSucceeded;
    case v1::TASK_STATE_FAILED: return TaskState::kFailed;
    case v1::TASK_STATE_KILLED: return TaskState::kKilled;
    case v1::TASK_STATE_BACKOFF: return TaskState::kBackoff;
    default: return std::nullopt;
  }
}

inline v1::RestartPolicy to_proto(RestartPolicy p) {
  switch (p) {
    case RestartPolicy::kNever: return v1::RESTART_POLICY_NEVER;
    case RestartPolicy::kOnFailure: return v1::RESTART_POLICY_ON_FAILURE;
    case RestartPolicy::kAlways: return v1::RESTART_POLICY_ALWAYS;
  }
  return v1::RESTART_POLICY_UNSPECIFIED;
}

// UNSPECIFIED is NEVER here, unlike the other enums: an m2 client that
// never sets a policy means "never" (NOTES.md m3), so it is a valid value.
inline std::optional<RestartPolicy> from_proto(v1::RestartPolicy p) {
  switch (p) {
    case v1::RESTART_POLICY_UNSPECIFIED:
    case v1::RESTART_POLICY_NEVER: return RestartPolicy::kNever;
    case v1::RESTART_POLICY_ON_FAILURE: return RestartPolicy::kOnFailure;
    case v1::RESTART_POLICY_ALWAYS: return RestartPolicy::kAlways;
    default: return std::nullopt;
  }
}

}  // namespace fleet
