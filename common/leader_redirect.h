#pragma once
// leader_redirect: how a replica that is not serving points a client at the
// one that is (m4b), shared by fleetlet and fleetctl.
//
// WHY THIS EXISTS. With 3 fleetmaster replicas only the leader serves
// agents and fleetctl. A client may call any replica; one that is not the
// leader answers UNAVAILABLE and puts the leader's address (when it knows
// it) in the trailing metadata under "fleet-leader". The client switches
// to that address and retries at once; with no hint (an election in
// progress, or the replica is down) it moves to the next address in its
// list and retries after a backoff.
//
// WHY UNAVAILABLE AND METADATA, not a field in each response or a
// dedicated status: UNAVAILABLE is gRPC's "not served, safe to retry"
// code, which every client already treats as retryable; and the codes the
// agent treats specially (FAILED_PRECONDITION = fenced, kill everything;
// NOT_FOUND = register again) must never mean "wrong replica". Metadata
// keeps the hint out of every message type.
//
// THREE OUTCOMES a client distinguishes:
//   OK or any other code: the leader answered; act on it.
//   UNAVAILABLE from a replica (not serving, or no leader known): it did
//     nothing; retry, at the hinted leader if there is a hint.
//   ABORTED: the leader made state changes for this call but lost its
//     leadership (or could not reach a majority) before they committed, so
//     they may or may not take effect. Retrying is safe only for calls
//     that are idempotent: heartbeats and lists are; a submission is
//     because it carries a request id the master deduplicates on
//     (SubmitRequest.request_id). The same holds for UNAVAILABLE after a
//     connection broke mid-call: the request may have been committed.

#include <string>
#include <vector>

#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>

namespace fleet {

inline constexpr char kLeaderMetadataKey[] = "fleet-leader";

// "a:1,b:2,c:3" -> {"a:1", "b:2", "c:3"}; empty items are skipped.
inline std::vector<std::string> split_addresses(const std::string& list) {
  std::vector<std::string> out;
  size_t start = 0;
  while (start <= list.size()) {
    const size_t comma = list.find(',', start);
    const size_t end = comma == std::string::npos ? list.size() : comma;
    if (end > start) out.push_back(list.substr(start, end - start));
    if (comma == std::string::npos) break;
    start = comma + 1;
  }
  return out;
}

// The leader address a replica put in the call's trailing metadata, or "".
// Valid only after the call has finished.
inline std::string leader_hint(const grpc::ClientContext& ctx) {
  const auto& md = ctx.GetServerTrailingMetadata();
  const auto it = md.find(kLeaderMetadataKey);
  if (it == md.end()) return {};
  return std::string(it->second.data(), it->second.size());
}

// The call was not answered by a serving leader: try again, here or
// elsewhere. DEADLINE_EXCEEDED is included because a replica that was
// cut off from the others can neither answer nor redirect in time.
inline bool should_retry_elsewhere(const grpc::Status& s) {
  return s.error_code() == grpc::StatusCode::UNAVAILABLE ||
         s.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
         s.error_code() == grpc::StatusCode::ABORTED;
}

}  // namespace fleet
