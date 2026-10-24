#pragma once
// MasterServer: fleetmaster's gRPC server and scheduler thread (m2).
//
// WHY THIS EXISTS. MasterCore is the logic; this class makes it a service.
// It owns three things: the gRPC server (whose thread pool runs the RPC
// handlers), one mutex that every handler takes around its MasterCore
// call, and one tick thread that runs MasterCore::tick() (failure
// detection, backoff timers, a scheduling pass) every `tick_interval`, or
// immediately when a handler reports that something changed
// (MasterCore::needs_schedule()).
//
// WHY ONE MUTEX AND NOT A SINGLE-THREADED EVENT LOOP. Both serialize
// access to ClusterState; NOTES.md (m2) has the comparison. In short:
// every handler is a few microseconds of work on in-memory state and a
// scheduling pass over 3 nodes is microseconds too, so contention is not
// a concern at this size, and a mutex keeps each handler a straight-line
// function that returns its reply, instead of a request object posted to
// a queue plus a promise to wait on. TSan checks the locking in the
// integration tests.
//
// THE ORDERING RULE (m4). With a state directory, every call runs as
//     lock -> MasterCore decides and applies in memory -> commit: append
//     the call's records to the log and sync -> unlock -> reply
// (Service::call and tick_loop). So when the lock is released, everything
// in memory is on disk, and no reply, start, stop, forget or epoch leaves
// the process before the state change behind it is durable. That is what
// makes a crash at any instant safe: a change that was not synced was
// never seen by anyone, so losing it is as if it never happened, and a
// change anyone has seen survives. Holding the lock across the sync (about
// 3 ms for F_FULLFSYNC here) serializes RPCs that change state behind it;
// NOTES.md m4 says why that is acceptable now and what m4b does instead.
//
// Usable in-process: tests start a MasterServer on 127.0.0.1:0 (an
// ephemeral port) and read port() back.

#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <grpcpp/server.h>

#include "master_core.h"
#include "wal.h"

namespace fleet {

struct MasterServerOptions {
  std::string listen_address = "127.0.0.1:7070";
  // The timer half of "on a timer and on events". Events cover every
  // change a handler makes; the timer is what notices silence (failure
  // detection) and expired backoffs, so it bounds how late either is
  // acted on: a node is declared dead at most one tick after its sixth
  // missed heartbeat. 100 ms is a fifth of a heartbeat interval.
  std::chrono::milliseconds tick_interval{100};
  MasterOptions core;
  // Where the write-ahead log and snapshot live (m4). Empty: no
  // persistence (state is lost with the process, as before m4; the
  // records MasterCore makes are dropped).
  std::filesystem::path state_dir;
  StorageOptions storage;
};

class MasterServer {
 public:
  explicit MasterServer(MasterServerOptions options);
  ~MasterServer();  // calls shutdown()

  MasterServer(const MasterServer&) = delete;
  MasterServer& operator=(const MasterServer&) = delete;

  // Recovers state from state_dir (if set), then binds and starts
  // serving. Returns false with *error set if the state is corrupt or the
  // address could not be bound.
  bool start(std::string* error);
  // What recovery found (zeros without a state directory).
  const RecoveryInfo& recovery_info() const { return recovery_; }
  // The bound port (useful when listen_address ends in ":0").
  int port() const { return port_; }
  // Stops accepting RPCs, finishes in-flight ones, joins the tick thread.
  // Idempotent.
  void shutdown();

  // Runs fn on the core under the lock, then commits whatever it changed.
  // For tests and for fleetmaster's final summary; RPC handlers use the
  // same lock. Does nothing before a successful start().
  void with_core(const std::function<void(MasterCore&)>& fn);
  // Log statistics (zeros without a state directory). Thread-safe.
  StorageStats storage_stats();

 private:
  class Service;
  void tick_loop();
  // Makes the core's new records durable (and snapshots when due). Called
  // with mu_ held, before the lock is released. Aborts the process if the
  // disk fails: see Storage::append for why there is no retry.
  void commit_locked();

  MasterServerOptions options_;
  std::mutex mu_;
  std::condition_variable cv_;  // wakes the tick thread
  std::unique_ptr<MasterCore> core_;  // guarded by mu_; built by start()
  std::unique_ptr<Storage> storage_;  // guarded by mu_; null without state_dir
  RecoveryInfo recovery_;
  bool stopping_ = false;       // guarded by mu_
  std::unique_ptr<Service> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread ticker_;
  int port_ = 0;
};

}  // namespace fleet
