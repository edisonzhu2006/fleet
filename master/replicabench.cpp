// replicabench: what replication costs per call (m4b), for
// bench/leader_failover.sh and NOTES.md. Times the same gRPC calls, made
// one after another from one client, against
//   A. the m4 single master with a state directory (one local log sync per
//      write), and
//   B. the leader of a 3-replica Raft group (a write is synced on the
//      leader, sent to the followers, synced there, acknowledged, and
//      confirmed by a read round; a read waits for a read round only),
// all in this process over loopback gRPC, with the same sync mode.
//
// Calls: Submit (one task, one record: a write) and List (a read). No
// agents, so nothing is ever placed and each Submit is exactly one record.
// Prints p50 / p99 / max per call. Every number it prints comes from
// timing those calls with the steady clock; it prints a line per
// experiment, a few seconds apart.

#include <algorithm>
#include <arpa/inet.h>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <functional>
#include <map>
#include <memory>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "fleet.grpc.pb.h"
#include "master_server.h"
#include "replica_server.h"

namespace fs = std::filesystem;
using namespace fleet;
using Ms = std::chrono::duration<double, std::milli>;

namespace {

double pct(std::vector<double> v, double p) {
  std::sort(v.begin(), v.end());
  if (v.empty()) return 0;
  return v[std::min(v.size() - 1, static_cast<size_t>(p * static_cast<double>(v.size())))];
}

int free_port() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in a{};
  a.sin_family = AF_INET;
  a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  ::bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a));
  socklen_t len = sizeof(a);
  ::getsockname(fd, reinterpret_cast<sockaddr*>(&a), &len);
  const int port = ntohs(a.sin_port);
  ::close(fd);
  return port;
}

struct Timings {
  std::vector<double> submit, list;
};

Timings run_calls(const std::string& address, int n) {
  auto stub = v1::Master::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));
  Timings t;
  auto timed = [&](const std::function<grpc::Status(grpc::ClientContext*)>& call,
                   std::vector<double>* out) {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    const auto t0 = std::chrono::steady_clock::now();
    const grpc::Status s = call(&ctx);
    out->push_back(Ms(std::chrono::steady_clock::now() - t0).count());
    if (!s.ok()) {
      std::fprintf(stderr, "replicabench: call failed: %s\n", s.error_message().c_str());
      std::exit(1);
    }
  };
  for (int i = 0; i < n + 10; ++i) {  // the first 10 warm the channel up, not counted
    v1::SubmitRequest req;
    req.set_job("bench" + std::to_string(i));
    req.mutable_request()->set_milli_cpu(100);
    req.mutable_request()->set_memory_mb(16);
    req.add_argv("true");
    v1::SubmitResponse resp;
    std::vector<double> sink;
    timed([&](grpc::ClientContext* c) { return stub->Submit(c, req, &resp); },
          i < 10 ? &sink : &t.submit);
    v1::ListResponse lr;
    timed([&](grpc::ClientContext* c) { return stub->List(c, v1::ListRequest{}, &lr); },
          i < 10 ? &sink : &t.list);
  }
  return t;
}

void print(const char* what, const std::vector<double>& v) {
  std::printf("  %-44s p50 %7.3f ms   p99 %7.3f ms   max %7.3f ms   (n=%zu)\n", what, pct(v, 0.5),
              pct(v, 0.99), pct(v, 1.0), v.size());
  std::fflush(stdout);
}

}  // namespace

int main(int argc, char** argv) {
  int n = 200;
  SyncMode sync = SyncMode::kFull;
  for (int i = 1; i < argc; ++i) {
    const std::string a = argv[i];
    if (a == "--n" && i + 1 < argc) {
      n = std::atoi(argv[++i]);
    } else if (a == "--sync" && i + 1 < argc) {
      const std::string m = argv[++i];
      sync = m == "none" ? SyncMode::kNone : m == "fsync" ? SyncMode::kFsync : SyncMode::kFull;
    } else {
      std::fprintf(stderr, "usage: replicabench [--n CALLS] [--sync full|fsync|none]\n");
      return 2;
    }
  }
  const fs::path dir = fs::path("/private/tmp/fleet/replicabench") / std::to_string(::getpid());
  fs::remove_all(dir);
  const char* mode = sync == SyncMode::kFull ? "full (F_FULLFSYNC)" : sync == SyncMode::kFsync ? "fsync" : "none";
  std::printf("replicabench: %d sequential calls of each kind, sync %s\n", n, mode);

  {
    MasterServerOptions o;
    o.listen_address = "127.0.0.1:0";
    o.state_dir = dir / "single";
    o.storage.sync = sync;
    MasterServer m(o);
    std::string err;
    if (!m.start(&err)) {
      std::fprintf(stderr, "replicabench: %s\n", err.c_str());
      return 1;
    }
    const Timings t = run_calls("127.0.0.1:" + std::to_string(m.port()), n);
    std::printf("A. m4 single master, one local log sync per write\n");
    print("Submit (1 record)", t.submit);
    print("List", t.list);
    m.shutdown();
  }
  {
    std::map<uint32_t, std::string> members;
    for (uint32_t i = 1; i <= 3; ++i) members[i] = "127.0.0.1:" + std::to_string(free_port());
    std::vector<std::unique_ptr<ReplicaServer>> replicas;
    for (const auto& [id, addr] : members) {
      ReplicaOptions o;
      o.id = id;
      o.members = members;
      o.state_dir = dir / ("r" + std::to_string(id));
      o.storage.sync = sync;
      replicas.push_back(std::make_unique<ReplicaServer>(o));
      std::string err;
      if (!replicas.back()->start(&err)) {
        std::fprintf(stderr, "replicabench: %s\n", err.c_str());
        return 1;
      }
    }
    uint32_t leader = 0;
    for (int i = 0; i < 500 && !leader; ++i) {
      for (auto& r : replicas) {
        if (r->status().serving) leader = r->status().id;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    if (!leader) {
      std::fprintf(stderr, "replicabench: no leader\n");
      return 1;
    }
    const Timings t = run_calls(members.at(leader), n);
    std::printf("B. leader of 3 Raft replicas: commit on a majority + read round\n");
    print("Submit (1 record)", t.submit);
    print("List", t.list);
    for (auto& r : replicas) {
      const raft::RaftStorageStats s = r->storage_stats();
      std::printf("  replica %u: %llu log syncs, mean %.3f ms each\n", r->status().id,
                  static_cast<unsigned long long>(s.log_syncs),
                  s.log_syncs ? s.log_sync_ms / static_cast<double>(s.log_syncs) : 0.0);
    }
    for (auto& r : replicas) r->shutdown();
  }
  fs::remove_all(dir);
  return 0;
}
