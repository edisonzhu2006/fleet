// fleetctl: the command-line client for fleetmaster.
//
//   fleetctl [--master HOST:PORT] [--timeout-ms N] <command> ...
//     submit --job J --cpu MILLICPU --mem MB [--priority 0..3] [--count N]
//            [--label k=v]... [--image IMAGE] -- COMMAND [ARGS...]
//     list [--job J]
//     kill --task ID | --job J
//     drain NODE
//
// Every call carries a deadline (--timeout-ms, default 3000), so a master
// that is down makes fleetctl fail in bounded time with the gRPC status,
// instead of hanging. Exit status: 0 ok, 1 the RPC failed, 2 bad usage.

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "fleet.grpc.pb.h"

namespace {

using fleet::v1::Master;

[[noreturn]] void usage() {
  std::fprintf(stderr,
               "usage: fleetctl [--master HOST:PORT] [--timeout-ms N] <command>\n"
               "  submit --job J --cpu MILLICPU --mem MB [--priority 0..3] [--count N]\n"
               "         [--label k=v]... [--image IMAGE] -- COMMAND [ARGS...]\n"
               "  list [--job J]\n"
               "  kill --task ID | --job J\n"
               "  drain NODE\n");
  std::exit(2);
}

int fail(const grpc::Status& s) {
  std::fprintf(stderr, "fleetctl: %s: %s\n",
               s.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ? "deadline exceeded"
               : s.error_code() == grpc::StatusCode::UNAVAILABLE     ? "master unavailable"
                                                                     : "error",
               s.error_message().c_str());
  return 1;
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
    default: return "?";
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
    std::printf("%-10s %-9s %-17s %-17s %5u %6lldms\n", n.name().c_str(),
                node_state(n.state()), cpu.c_str(), mem.c_str(), n.running_tasks(),
                static_cast<long long>(n.ms_since_heartbeat()));
  }
  std::printf("\n%-4s %-10s %3s %-9s %-10s %3s %-10s %s\n", "ID", "JOB", "PRI", "STATE",
              "NODE", "ATT", "EXIT", "COMMAND");
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
    for (const auto& a : t.argv()) cmd += (cmd.empty() ? "" : " ") + a;
    std::printf("%-4u %-10s %3d %-9s %-10s %3u %-10s %s\n", t.id(), t.job().c_str(),
                t.priority(), task_state(t.state()), t.node().empty() ? "-" : t.node().c_str(),
                t.attempt(), exit.c_str(), cmd.c_str());
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

  auto stub = Master::NewStub(grpc::CreateChannel(master, grpc::InsecureChannelCredentials()));
  grpc::ClientContext ctx;
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeout_ms));

  if (cmd == "submit") {
    fleet::v1::SubmitRequest req;
    req.set_priority(1);
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
    fleet::v1::SubmitResponse resp;
    grpc::Status s = stub->Submit(&ctx, req, &resp);
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
    grpc::Status s = stub->List(&ctx, req, &resp);
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
    grpc::Status s = stub->Kill(&ctx, req, &resp);
    if (!s.ok()) return fail(s);
    std::printf("killed %d task(s)\n", resp.killed_size());
    return 0;
  }
  if (cmd == "drain") {
    if (i >= argc) usage();
    fleet::v1::DrainRequest req;
    req.set_node(argv[i]);
    fleet::v1::DrainResponse resp;
    grpc::Status s = stub->Drain(&ctx, req, &resp);
    if (!s.ok()) return fail(s);
    std::printf("drained %s: %d task(s) back to pending\n", argv[i], resp.evicted_size());
    return 0;
  }
  usage();
}
