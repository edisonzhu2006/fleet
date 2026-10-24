#pragma once
// Agent: the node-side half of fleet (fleetlet's logic, m2).
//
// WHY THIS EXISTS. A node needs one process that (1) tells the master it
// exists and how big it is, (2) every heartbeat interval reports exactly
// what it is running and what has exited, and (3) applies the master's
// reply: start what is missing, stop what should not run, forget exits the
// master has recorded. It holds no opinion about what should run; the
// master's reply is the whole truth, recomputed every time, so the agent
// is simple and a lost message costs one interval, not correctness.
//
// THREADING. Everything (gRPC calls, the Runtime, the instance table)
// happens on one loop thread, so none of it needs locks. The only state
// shared with other threads is the stop flag and a stats snapshot, both
// under mu_. Every RPC has a deadline (rpc_deadline) so a master that is
// down or slow delays the loop by at most that long, and tasks keep
// running meanwhile: losing the master does not stop work.
//
// INSTANCE IDENTITY. A running copy is keyed by (task id, attempt,
// incarnation), the TaskInstance in fleet.proto. A start for a key already
// in the table (running or exited but not yet forgotten) is ignored; that
// is what makes the master's re-sent starts harmless.

#include <chrono>
#include <compare>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "fleet.grpc.pb.h"
#include "model.h"
#include "runtime.h"

namespace fleet {

struct AgentOptions {
  std::string master_address = "127.0.0.1:7070";
  std::string name;
  Resources capacity;
  Labels labels;
  std::filesystem::path work_dir;  // per-task log files go here
  // Replaced by the master's value at registration (heartbeat_interval_ms).
  std::chrono::milliseconds heartbeat_interval{500};
  // Deadline on every RPC. Shorter than the heartbeat interval so a hung
  // call cannot make the agent skip beats silently.
  std::chrono::milliseconds rpc_deadline{400};
  // How often exits are collected between heartbeats.
  std::chrono::milliseconds poll_interval{20};
  bool log_events = false;
};

class Agent {
 public:
  Agent(AgentOptions options, std::unique_ptr<Runtime> runtime);
  ~Agent();  // request_stop() + join()

  Agent(const Agent&) = delete;
  Agent& operator=(const Agent&) = delete;

  // Blocks until request_stop(). On the way out kills every task it
  // started (Runtime::kill_all), so tasks never outlive a clean shutdown.
  void run();
  void start_background();  // run() on an owned thread (tests)
  void request_stop();
  void join();

  struct Stats {
    bool registered = false;
    uint32_t node_id = 0;
    size_t running = 0;  // instances the runtime reports alive
    uint64_t heartbeats_ok = 0;
    uint64_t heartbeats_failed = 0;
    uint64_t started = 0;
  };
  Stats stats() const;  // thread-safe

 private:
  struct Key {
    uint32_t task_id = 0;
    uint32_t attempt = 0;
    uint64_t incarnation = 0;
    friend auto operator<=>(const Key&, const Key&) = default;
  };
  struct Instance {
    RuntimeHandle handle = 0;
    v1::TaskStatus::Phase phase = v1::TaskStatus::PHASE_RUNNING;
    int exit_code = 0;
    int signal = 0;
    std::string message;
  };

  bool stop_requested() const;
  void do_register();
  void do_heartbeat();
  void collect_exits();
  void launch(const v1::TaskLaunch& l);
  void publish_stats();
  void log(const std::string& line) const;
  static Key key_of(const v1::TaskInstance& inst);

  AgentOptions options_;
  std::unique_ptr<Runtime> runtime_;
  std::unique_ptr<v1::Master::Stub> stub_;

  // Loop-thread state (no lock needed).
  bool registered_ = false;
  uint32_t node_id_ = 0;
  uint64_t incarnation_ = 0;
  std::map<Key, Instance> instances_;
  std::map<RuntimeHandle, Key> by_handle_;
  uint64_t hb_ok_ = 0, hb_failed_ = 0, started_ = 0;

  // Shared with other threads.
  mutable std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
  Stats stats_;
  std::thread thread_;
};

}  // namespace fleet
