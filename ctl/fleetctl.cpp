// fleetctl: the command-line client for fleetmaster.
//
//   fleetctl [--master HOST:PORT[,HOST:PORT...]] [--timeout-ms N] <command> ...
//     submit --job J --cpu MILLICPU --mem MB [--priority 0..3] [--count N]
//            [--label k=v]... [--image IMAGE]
//            [--restart never|on-failure|always]
//            [--checkpoint-dir DIR [--resume-arg ARG]...] -- COMMAND [ARGS...]
//     list [--job J]
//     kill --task ID | --job J
//     drain NODE
//     status
//
// drain returns once the master has marked the node draining; the tasks
// then move over the next heartbeats (each is stopped where it runs and
// rescheduled once its exit is reported). `list` shows the progress: the
// node's TASKS column counts down to 0.
//
// Every call carries a deadline (--timeout-ms, default 3000), so a master
// that is down makes fleetctl fail in bounded time with the gRPC status,
// instead of hanging. Exit status: 0 ok, 1 the RPC failed, 2 bad usage.
//
// REPLICATED MASTERS (m4b). --master may list every replica. fleetctl
// follows a non-leader's redirect to the leader at once, and otherwise
// moves to the next replica after a backoff (50 ms doubling to 500 ms),
// until the deadline. Every submission carries a random request id
// (SubmitRequest.request_id), so retrying it is safe even when the first
// attempt was committed and only its reply was lost (a leader killed after
// committing, or one that logged it and then lost its majority, ABORTED):
// the master answers a repeated id with the first attempt's task ids
// instead of admitting the job again. `status` asks every listed replica
// for its role, term and log position.

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "fleet.grpc.pb.h"
#include "leader_redirect.h"

namespace {

using fleet::v1::Master;

[[noreturn]] void usage() {
  std::fprintf(stderr,
               "usage: fleetctl [--master HOST:PORT[,HOST:PORT...]] [--timeout-ms N] <command>\n"
               "  submit --job J --cpu MILLICPU --mem MB [--priority 0..3] [--count N]\n"
               "         [--label k=v]... [--image IMAGE]\n"
               "         [--restart never|on-failure|always]\n"
               "         [--checkpoint-dir DIR [--resume-arg ARG]...] -- COMMAND [ARGS...]\n"
               "         (stateful, m5: the resume command is COMMAND ARGS... followed by\n"
               "          every --resume-arg, e.g. --resume-arg --resume for streamkit)\n"
               "  list [--job J]\n"
               "  kill --task ID | --job J\n"
               "  drain NODE\n"
               "  status            (every listed replica's Raft role and log position)\n");
  std::exit(2);
}

int fail(const grpc::Status& s) {
  std::fprintf(stderr, "fleetctl: %s: %s\n",
               s.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ? "deadline exceeded"
               : s.error_code() == grpc::StatusCode::UNAVAILABLE     ? "master unavailable"
               : s.error_code() == grpc::StatusCode::ABORTED         ? "outcome unknown"
                                                                     : "error",
               s.error_message().c_str());
  return 1;
}

// Calls the leader among `masters`, following redirects (see the header).
class Client {
 public:
  Client(std::vector<std::string> masters, int timeout_ms)
      : masters_(std::move(masters)),
        deadline_(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms)) {}

  // f(stub, ctx) makes one call. Every call fleetctl makes is safe to
  // repeat even if an earlier attempt took effect (list and drain change
  // nothing twice, kill kills nothing more, submit has its request id), so
  // UNAVAILABLE, DEADLINE_EXCEEDED and ABORTED are all retried.
  template <typename F>
  grpc::Status call(F&& f) {
    auto backoff = std::chrono::milliseconds(50);
    int redirects = 0;
    grpc::Status s;
    while (true) {
      grpc::ClientContext ctx;
      ctx.set_deadline(deadline_);
      const std::string address = masters_[current_];
      s = f(stub(address), &ctx);
      if (!fleet::should_retry_elsewhere(s)) return s;
      if (std::chrono::system_clock::now() >= deadline_) return s;
      const std::string hint = fleet::leader_hint(ctx);
      // Redirected: straight to the leader, twice at most in a row. More
      // means replicas naming each other in the middle of an election;
      // then wait like any other failure, rather than spin.
      if (!hint.empty() && hint != address && ++redirects <= 2) {
        auto it = std::find(masters_.begin(), masters_.end(), hint);
        if (it == masters_.end()) it = masters_.insert(masters_.end(), hint);
        current_ = static_cast<size_t>(it - masters_.begin());
        continue;
      }
      redirects = 0;
      // Not redirected elsewhere (down, no leader known, or a leader that
      // cannot serve and names itself, e.g. one cut off from the
      // majority): try the next replica, which redirects back if need be.
      if (hint.empty() || hint == address) current_ = (current_ + 1) % masters_.size();
      std::this_thread::sleep_for(backoff);
      backoff = std::min(backoff * 2, std::chrono::milliseconds(500));
    }
  }

  const std::vector<std::string>& masters() const { return masters_; }
  std::chrono::system_clock::time_point deadline() const { return deadline_; }
  Master::Stub& stub(const std::string& address) {
    auto& s = stubs_[address];
    if (!s) s = Master::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
    return *s;
  }

 private:
  std::vector<std::string> masters_;
  size_t current_ = 0;
  std::chrono::system_clock::time_point deadline_;
  std::map<std::string, std::unique_ptr<Master::Stub>> stubs_;
};

const char* role_name(fleet::v1::ReplicaStatusResponse::Role r) {
  switch (r) {
    case fleet::v1::ReplicaStatusResponse::ROLE_FOLLOWER: return "follower";
    case fleet::v1::ReplicaStatusResponse::ROLE_CANDIDATE: return "candidate";
    case fleet::v1::ReplicaStatusResponse::ROLE_LEADER: return "leader";
    default: return "?";
  }
}

// One line per listed replica, asked directly (no redirects).
int print_status(Client& c) {
  std::printf("%-22s %3s %-9s %6s %6s %8s %8s %8s %8s %8s %s\n", "ADDRESS", "ID", "ROLE", "TERM",
              "LEADER", "COMMIT", "APPLIED", "LAST", "SNAP", "LSN", "NOTE");
  int answered = 0;
  for (const std::string& address : c.masters()) {
    grpc::ClientContext ctx;
    ctx.set_deadline(c.deadline());
    fleet::v1::ReplicaStatusResponse r;
    const grpc::Status s = c.stub(address).ReplicaStatus(&ctx, fleet::v1::ReplicaStatusRequest{}, &r);
    if (s.error_code() == grpc::StatusCode::UNIMPLEMENTED) {
      std::printf("%-22s standalone master (not replicated)\n", address.c_str());
      ++answered;
      continue;
    }
    if (!s.ok()) {
      std::printf("%-22s unreachable: %s\n", address.c_str(), s.error_message().c_str());
      continue;
    }
    ++answered;
    std::string note = r.serving() ? "serving" : "";
    if (r.isolated()) note += note.empty() ? "ISOLATED" : ", ISOLATED";
    std::printf("%-22s %3u %-9s %6llu %6u %8llu %8llu %8llu %8llu %8llu %s\n", address.c_str(),
                r.id(), role_name(r.role()), static_cast<unsigned long long>(r.term()),
                r.leader_id(), static_cast<unsigned long long>(r.commit_index()),
                static_cast<unsigned long long>(r.applied_index()),
                static_cast<unsigned long long>(r.last_index()),
                static_cast<unsigned long long>(r.snapshot_index()),
                static_cast<unsigned long long>(r.lsn()), note.c_str());
  }
  return answered > 0 ? 0 : 1;
}

const char* node_state(fleet::v1::NodeState s) {
  switch (s) {
    case fleet::v1::NODE_STATE_READY: return "ready";
    case fleet::v1::NODE_STATE_DRAINING: return "draining";
    case fleet::v1::NODE_STATE_DEAD: return "dead";
    default: return "?";
  }
}

const char* task_state(fleet::v1::TaskState s) {
  switch (s) {
    case fleet::v1::TASK_STATE_PENDING: return "pending";
    case fleet::v1::TASK_STATE_RUNNING: return "running";
    case fleet::v1::TASK_STATE_SUCCEEDED: return "succeeded";
    case fleet::v1::TASK_STATE_FAILED: return "failed";
    case fleet::v1::TASK_STATE_KILLED: return "killed";
    case fleet::v1::TASK_STATE_BACKOFF: return "backoff";
    default: return "?";
  }
}

const char* restart_policy(fleet::v1::RestartPolicy p) {
  switch (p) {
    case fleet::v1::RESTART_POLICY_ON_FAILURE: return "on-fail";
    case fleet::v1::RESTART_POLICY_ALWAYS: return "always";
    default: return "never";
  }
}

void print_list(const fleet::v1::ListResponse& r) {
  std::printf("%-10s %-9s %-17s %-17s %5s %8s\n", "NODE", "STATE", "CPU alloc/cap",
              "MEM alloc/cap", "TASKS", "LAST HB");
  for (const auto& n : r.nodes()) {
    const std::string cpu = std::to_string(n.allocated().milli_cpu()) + "/" +
                            std::to_string(n.capacity().milli_cpu()) + "m";
    const std::string mem = std::to_string(n.allocated().memory_mb()) + "/" +
                            std::to_string(n.capacity().memory_mb()) + "MB";
    // Suspect is shown in place of the state: it is the thing an operator
    // needs to see first, and a suspect node is ready or draining anyway.
    std::printf("%-10s %-9s %-17s %-17s %5u %6lldms\n", n.name().c_str(),
                n.suspect() ? "suspect" : node_state(n.state()), cpu.c_str(), mem.c_str(),
                n.running_tasks(), static_cast<long long>(n.ms_since_heartbeat()));
  }
  std::printf("\n%-4s %-10s %3s %-9s %-10s %3s %-10s %-7s %3s %s\n", "ID", "JOB", "PRI",
              "STATE", "NODE", "ATT", "EXIT", "RESTART", "RST", "COMMAND");
  for (const auto& t : r.tasks()) {
    std::string exit = "-";
    if (t.has_exit()) {
      // OOM first: an OOM-killed container also carries a message and
      // exit code 137, and "oom" is the part a user needs to see.
      exit = t.oom_killed()          ? "oom"
             : t.signal()            ? "sig " + std::to_string(t.signal())
             : !t.message().empty()  ? "startfail"
                                     : std::to_string(t.exit_code());
    }
    // The image, when one was given, prefixes the command: "[alpine:3.20] sh -c ...".
    std::string cmd = t.image().empty() ? "" : "[" + t.image() + "]";
    // A stateful task (m5) shows its checkpoint dir the same way.
    if (!t.checkpoint_dir().empty()) {
      cmd += (cmd.empty() ? "" : " ") + ("{ckpt " + t.checkpoint_dir() + "}");
    }
    for (const auto& a : t.argv()) cmd += (cmd.empty() ? "" : " ") + a;
    // A task in backoff shows how long until it is pending again.
    std::string state = task_state(t.state());
    if (t.state() == fleet::v1::TASK_STATE_BACKOFF) {
      state += " " + std::to_string((t.restart_in_ms() + 999) / 1000) + "s";
    }
    std::printf("%-4u %-10s %3d %-9s %-10s %3u %-10s %-7s %3u %s\n", t.id(), t.job().c_str(),
                t.priority(), state.c_str(), t.node().empty() ? "-" : t.node().c_str(),
                t.attempt(), exit.c_str(), restart_policy(t.restart_policy()), t.restarts(),
                cmd.c_str());
  }
}

}  // namespace

int main(int argc, char** argv) {
  std::string master = "127.0.0.1:7070";
  int timeout_ms = 3000;
  int i = 1;
  for (; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "--master" && i + 1 < argc) {
      master = argv[++i];
    } else if (a == "--timeout-ms" && i + 1 < argc) {
      timeout_ms = std::stoi(argv[++i]);
    } else {
      break;
    }
  }
  if (i >= argc) usage();
  const std::string cmd = argv[i++];

  std::vector<std::string> masters = fleet::split_addresses(master);
  if (masters.empty()) usage();
  Client client(std::move(masters), timeout_ms);
  if (cmd == "status") return print_status(client);

  if (cmd == "submit") {
    fleet::v1::SubmitRequest req;
    req.set_priority(1);
    // The resume command (m5) is the command plus these, so the common
    // case (one extra flag) needs no second copy of a long command line.
    // The proto carries the full resume argv, so other clients can send a
    // different command altogether.
    std::vector<std::string> resume_args;
    for (; i < argc; ++i) {
      const std::string a = argv[i];
      if (a == "--") {
        ++i;
        break;
      }
      if (i + 1 >= argc) usage();
      const std::string v = argv[++i];
      if (a == "--job") {
        req.set_job(v);
      } else if (a == "--cpu") {
        req.mutable_request()->set_milli_cpu(std::stoll(v));
      } else if (a == "--mem") {
        req.mutable_request()->set_memory_mb(std::stoll(v));
      } else if (a == "--priority") {
        req.set_priority(std::stoi(v));
      } else if (a == "--count") {
        req.set_count(static_cast<uint32_t>(std::stoul(v)));
      } else if (a == "--image") {
        req.set_image(v);
      } else if (a == "--restart") {
        if (v == "never") {
          req.set_restart_policy(fleet::v1::RESTART_POLICY_NEVER);
        } else if (v == "on-failure") {
          req.set_restart_policy(fleet::v1::RESTART_POLICY_ON_FAILURE);
        } else if (v == "always") {
          req.set_restart_policy(fleet::v1::RESTART_POLICY_ALWAYS);
        } else {
          usage();
        }
      } else if (a == "--checkpoint-dir") {
        req.set_checkpoint_dir(v);
      } else if (a == "--resume-arg") {
        resume_args.push_back(v);
      } else if (a == "--label") {
        const size_t eq = v.find('=');
        if (eq == std::string::npos) usage();
        (*req.mutable_required_labels())[v.substr(0, eq)] = v.substr(eq + 1);
      } else {
        usage();
      }
    }
    for (; i < argc; ++i) req.add_argv(argv[i]);
    if (req.argv().empty()) usage();
    if (!resume_args.empty()) {
      *req.mutable_resume_argv() = req.argv();
      for (const std::string& a : resume_args) req.add_resume_argv(a);
    }
    // 128 random bits: an id no other submission will ever use, the same
    // across every retry of this one.
    std::random_device rd;
    char rid[33];
    std::snprintf(rid, sizeof(rid), "%08x%08x%08x%08x", rd(), rd(), rd(), rd());
    req.set_request_id(rid);
    fleet::v1::SubmitResponse resp;
    // Idempotent thanks to the request id, so ABORTED is retried too.
    grpc::Status s = client.call(
        [&](Master::Stub& st, grpc::ClientContext* ctx) { return st.Submit(ctx, req, &resp); });
    if (!s.ok()) return fail(s);
    std::printf("submitted job %s:", req.job().c_str());
    for (uint32_t id : resp.task_ids()) std::printf(" %u", id);
    std::printf("\n");
    return 0;
  }
  if (cmd == "list") {
    fleet::v1::ListRequest req;
    if (i + 1 < argc && std::string(argv[i]) == "--job") req.set_job(argv[i + 1]);
    fleet::v1::ListResponse resp;
    grpc::Status s = client.call(
        [&](Master::Stub& st, grpc::ClientContext* ctx) { return st.List(ctx, req, &resp); });
    if (!s.ok()) return fail(s);
    print_list(resp);
    return 0;
  }
  if (cmd == "kill") {
    if (i + 1 >= argc) usage();
    fleet::v1::KillRequest req;
    const std::string a = argv[i];
    if (a == "--task") {
      req.set_task_id(static_cast<uint32_t>(std::stoul(argv[i + 1])));
    } else if (a == "--job") {
      req.set_job(argv[i + 1]);
    } else {
      usage();
    }
    fleet::v1::KillResponse resp;
    // Killing twice kills nothing more (a by-id kill of a killed task is
    // refused as already final), so retrying is safe.
    grpc::Status s = client.call(
        [&](Master::Stub& st, grpc::ClientContext* ctx) { return st.Kill(ctx, req, &resp); });
    if (!s.ok()) return fail(s);
    std::printf("killed %d task(s)\n", resp.killed_size());
    return 0;
  }
  if (cmd == "drain") {
    if (i >= argc) usage();
    fleet::v1::DrainRequest req;
    req.set_node(argv[i]);
    fleet::v1::DrainResponse resp;
    grpc::Status s = client.call(
        [&](Master::Stub& st, grpc::ClientContext* ctx) { return st.Drain(ctx, req, &resp); });
    if (!s.ok()) return fail(s);
    std::printf("draining %s: %d task(s) to stop and move\n", argv[i], resp.moving_size());
    return 0;
  }
  usage();
}
