#pragma once
// Agent: the node-side half of fleet (fleetlet's logic, m2; fencing m3).
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
// epoch), the TaskInstance in fleet.proto. A start for a key already
// in the table (running or exited but not yet forgotten) is ignored; that
// is what makes the master's re-sent starts harmless.
//
// FENCING (m3). When the master refuses a heartbeat with
// FAILED_PRECONDITION, this agent's epoch is stale or revoked: the master
// declared the node dead (it was partitioned, paused, or too slow) or saw
// a newer registration, and may already be running these tasks
// elsewhere. The agent then kills every task immediately (SIGKILL, not a
// graceful stop: each extra moment is a moment two copies run) and only
// then registers again. Before this response arrives the agent cannot
// know it was fenced; NOTES.md m3 discusses that window and why a lease
// is what closes it.
//
// SEVERAL MASTERS (m4b). master_address may list every fleetmaster
// replica, comma-separated. Only the Raft leader serves; any other
// replica answers UNAVAILABLE with the leader's address as a hint
// (common/leader_redirect.h). The agent follows a hint at once (up to two
// hops per beat); with no hint, or no answer, it moves to the next address
// and retries after a backoff that starts at 50 ms and doubles up to the
// heartbeat interval, so after a leader dies it finds the new one within
// a few hundred ms instead of a full interval or more. It needs no new
// registration: its epoch is in the replicated state, so the new leader
// accepts its next heartbeat as is.

#include <chrono>
#include <compare>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "fleet.grpc.pb.h"
#include "model.h"
#include "runtime.h"

namespace fleet {

struct AgentOptions {
  // One address, or every replica's, comma-separated (m4b).
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
  // Optional machine-readable record of every task instance's lifetime
  // (m4, for bench/master_failure.sh): one line per event,
  //     <unix microseconds> start|exit <task> <attempt> <epoch> <node name>
  // "start" is stamped just before the runtime is asked to start it and
  // "exit" once the agent has seen it gone, so each [start, exit] interval
  // contains the process's real lifetime. If two such intervals of one
  // task do not overlap, the processes did not run at the same time.
  std::filesystem::path event_log;
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

  // Fault injection for tests: while partitioned, the loop makes no RPCs
  // (as if every message were dropped) but keeps running its tasks, which
  // is exactly what a network partition looks like from the agent's side.
  void set_partitioned(bool partitioned);

  struct Stats {
    bool registered = false;
    uint32_t node_id = 0;
    size_t running = 0;  // instances the runtime reports alive
    uint64_t heartbeats_ok = 0;
    uint64_t heartbeats_failed = 0;
    uint64_t started = 0;
    uint64_t fenced = 0;  // times the master refused our epoch
  };
  Stats stats() const;  // thread-safe

  // Container name DockerRuntime gives an instance; see agent.cpp.
  static std::string container_name(uint32_t task_id, uint32_t attempt,
                                    uint64_t epoch);

 private:
  struct Key {
    uint32_t task_id = 0;
    uint32_t attempt = 0;
    uint64_t epoch = 0;
    friend auto operator<=>(const Key&, const Key&) = default;
  };
  struct Instance {
    RuntimeHandle handle = 0;
    v1::TaskStatus::Phase phase = v1::TaskStatus::PHASE_RUNNING;
    int exit_code = 0;
    int signal = 0;
    bool oom_killed = false;
    std::string message;
  };

  bool stop_requested() const;
  bool partitioned() const;
  void fence(const std::string& why);
  void do_register();
  void do_heartbeat();
  void collect_exits();
  void launch(const v1::TaskLaunch& l);
  void publish_stats();
  void log(const std::string& line) const;
  // Appends one line to options_.event_log (if set), stamped `when`.
  void record_event(const char* what, const Key& key,
                    std::chrono::system_clock::time_point when);
  // kill_all() plus an exit event for every instance it took down.
  void kill_everything();
  static Key key_of(const v1::TaskInstance& inst);
  // One RPC to the current master, following a leader redirect (m4b).
  // f(stub, ctx) makes the call; the context gets the RPC deadline.
  template <typename F>
  grpc::Status call_master(F&& f);
  v1::Master::Stub& stub_for(const std::string& address);

  AgentOptions options_;
  std::unique_ptr<Runtime> runtime_;
  // The master replicas, and the one calls go to now (loop thread only).
  std::vector<std::string> masters_;
  size_t current_ = 0;
  std::map<std::string, std::unique_ptr<v1::Master::Stub>> stubs_;
  // 0 while the masters answer; after an unanswered call, how long until
  // the next attempt (doubling, capped at the heartbeat interval).
  std::chrono::milliseconds retry_delay_{0};

  // Loop-thread state (no lock needed).
  std::FILE* events_ = nullptr;  // options_.event_log, opened by run()
  bool registered_ = false;
  uint32_t node_id_ = 0;
  uint64_t epoch_ = 0;
  std::map<Key, Instance> instances_;
  std::map<RuntimeHandle, Key> by_handle_;
  uint64_t hb_ok_ = 0, hb_failed_ = 0, started_ = 0, fenced_ = 0;

  // Shared with other threads.
  mutable std::mutex mu_;
  std::condition_variable cv_;
  bool stop_ = false;
  bool partitioned_ = false;
  Stats stats_;
  std::thread thread_;
};

}  // namespace fleet
