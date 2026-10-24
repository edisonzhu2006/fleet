#pragma once
// MasterServer: fleetmaster's gRPC server and scheduler thread (m2).
//
// WHY THIS EXISTS. MasterCore is the logic; this class makes it a service.
// It owns three things: the gRPC server (whose thread pool runs the RPC
// handlers), one mutex that every handler takes around its MasterCore
// call, and one scheduler thread that runs a scheduling pass every
// `schedule_interval`, or immediately when a handler reports that
// something changed (MasterCore::needs_schedule()).
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
// Usable in-process: tests start a MasterServer on 127.0.0.1:0 (an
// ephemeral port) and read port() back.

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <grpcpp/server.h>

#include "master_core.h"

namespace fleet {

struct MasterServerOptions {
  std::string listen_address = "127.0.0.1:7070";
  // The timer half of "on a timer and on events". Events cover every
  // change m2 knows about; the timer is the backstop, and from m3 on it
  // also drives failure detection.
  std::chrono::milliseconds schedule_interval{200};
  MasterOptions core;
};

class MasterServer {
 public:
  explicit MasterServer(MasterServerOptions options);
  ~MasterServer();  // calls shutdown()

  MasterServer(const MasterServer&) = delete;
  MasterServer& operator=(const MasterServer&) = delete;

  // Binds and starts serving. Returns false with *error set if the address
  // could not be bound.
  bool start(std::string* error);
  // The bound port (useful when listen_address ends in ":0").
  int port() const { return port_; }
  // Stops accepting RPCs, finishes in-flight ones, joins the scheduler
  // thread. Idempotent.
  void shutdown();

  // Runs fn on the core under the lock. For tests and for fleetmaster's
  // final summary; RPC handlers use the same lock.
  void with_core(const std::function<void(MasterCore&)>& fn);

 private:
  class Service;
  void scheduler_loop();

  MasterServerOptions options_;
  std::mutex mu_;
  std::condition_variable cv_;  // wakes the scheduler thread
  MasterCore core_;             // guarded by mu_
  bool stopping_ = false;       // guarded by mu_
  std::unique_ptr<Service> service_;
  std::unique_ptr<grpc::Server> server_;
  std::thread scheduler_;
  int port_ = 0;
};

}  // namespace fleet
