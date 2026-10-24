// MasterServer implementation: the gRPC service adapter (lock, call
// MasterCore, wake the tick thread if needed) and the tick thread.

#include "master_server.h"

#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>

#include "fleet.grpc.pb.h"

namespace fleet {

// The generated synchronous service: gRPC calls these from its own thread
// pool, any number at once, which is why each one takes mu_.
class MasterServer::Service final : public v1::Master::Service {
 public:
  explicit Service(MasterServer* m) : m_(m) {}

  grpc::Status RegisterNode(grpc::ServerContext*, const v1::RegisterNodeRequest* req,
                            v1::RegisterNodeResponse* resp) override {
    return call([&] { return m_->core_.register_node(*req, resp, Clock::now()); });
  }
  grpc::Status Heartbeat(grpc::ServerContext*, const v1::HeartbeatRequest* req,
                         v1::HeartbeatResponse* resp) override {
    return call([&] { return m_->core_.heartbeat(*req, resp, Clock::now()); });
  }
  grpc::Status Submit(grpc::ServerContext*, const v1::SubmitRequest* req,
                      v1::SubmitResponse* resp) override {
    return call([&] { return m_->core_.submit(*req, resp); });
  }
  grpc::Status List(grpc::ServerContext*, const v1::ListRequest* req,
                    v1::ListResponse* resp) override {
    return call([&] { return m_->core_.list(*req, resp, Clock::now()); });
  }
  grpc::Status Kill(grpc::ServerContext*, const v1::KillRequest* req,
                    v1::KillResponse* resp) override {
    return call([&] { return m_->core_.kill(*req, resp); });
  }
  grpc::Status Drain(grpc::ServerContext*, const v1::DrainRequest* req,
                     v1::DrainResponse* resp) override {
    return call([&] { return m_->core_.drain(*req, resp); });
  }

 private:
  // Every handler: lock, run, and if the call changed something the
  // scheduler cares about, wake the tick thread so a submission is placed
  // now rather than at the next tick. notify after unlocking so the woken thread does
  // not immediately block on the mutex we still hold.
  template <typename F>
  grpc::Status call(F&& f) {
    bool wake = false;
    grpc::Status s;
    {
      std::lock_guard<std::mutex> lock(m_->mu_);
      s = f();
      wake = m_->core_.needs_schedule();
    }
    if (wake) m_->cv_.notify_one();
    return s;
  }

  MasterServer* m_;
};

MasterServer::MasterServer(MasterServerOptions options)
    : options_(std::move(options)), core_(options_.core) {}

MasterServer::~MasterServer() { shutdown(); }

bool MasterServer::start(std::string* error) {
  service_ = std::make_unique<Service>(this);
  grpc::ServerBuilder builder;
  // Plaintext: m2 runs on localhost. TLS (grpc::SslServerCredentials) is a
  // drop-in change here and where the clients (fleetlet, fleetctl) create
  // their channels.
  builder.AddListeningPort(options_.listen_address,
                           grpc::InsecureServerCredentials(), &port_);
  builder.RegisterService(service_.get());
  server_ = builder.BuildAndStart();
  if (!server_ || port_ == 0) {
    if (error) *error = "could not listen on " + options_.listen_address;
    server_.reset();
    return false;
  }
  ticker_ = std::thread([this] { tick_loop(); });
  return true;
}

void MasterServer::shutdown() {
  if (server_) {
    // Deadline so a client stuck mid-call cannot hold shutdown forever.
    server_->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(2));
    server_->Wait();
  }
  {
    std::lock_guard<std::mutex> lock(mu_);
    stopping_ = true;
  }
  cv_.notify_all();
  if (ticker_.joinable()) ticker_.join();
  server_.reset();
}

void MasterServer::with_core(const std::function<void(MasterCore&)>& fn) {
  std::lock_guard<std::mutex> lock(mu_);
  fn(core_);
}

void MasterServer::tick_loop() {
  std::unique_lock<std::mutex> lock(mu_);
  while (!stopping_) {
    // Wakes on the timer or on an event; the predicate guards against
    // spurious wakeups and against a notify that arrived before we waited.
    cv_.wait_for(lock, options_.tick_interval,
                 [this] { return stopping_ || core_.needs_schedule(); });
    if (stopping_) break;
    // The tick runs under the lock. A scheduling pass is microseconds at
    // this scale (NOTES.md m1: mean 2.4 us per decision on 100 nodes) and
    // the detector is O(nodes), so holding the lock is cheaper to reason
    // about than copying state out and merging decisions back in. `now`
    // is read while holding the lock, so every heartbeat already recorded
    // is stamped no later than the time this tick judges silence against.
    core_.tick(Clock::now());
  }
}

}  // namespace fleet
