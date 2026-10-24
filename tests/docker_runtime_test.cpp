// Tests for DockerRuntime (m2b), in three layers:
//
// 1. Pure functions: --cpus formatting, the exact `docker run` argv, image
//    validation, container names.
// 2. DockerRuntime against a FAKE docker CLI: a /bin/sh script written into
//    the test's temp dir that records every argv it is called with and
//    keeps each "container" as a state file ("running 0 false"), answering
//    inspect in the same line format the real CLI produces for our
//    template. Tests flip a state file to simulate an exit or an OOM kill.
//    These run everywhere (CI has no Docker daemon) and check the
//    protocol: what is run, in what order, how each outcome is reported.
//    The fake is passed as DockerRuntimeOptions::docker (a path) rather
//    than by prepending its directory to PATH: setenv while other threads
//    may call getenv (gRPC threads from other tests in this binary) is a
//    data race.
// 3. The real Docker daemon, skipped unless `docker version` answers and
//    busybox:1.36 is already pulled (no network in tests): exit code and
//    logs, a real cgroup OOM kill, and a stop that is prompt because of
//    --init. bench/docker_limits.sh measures the limits themselves.
//
// Plus one end-to-end test over gRPC: master, an Agent with DockerRuntime
// on the fake CLI, and a submit with an image, to check that image and
// oom_killed travel through proto, master and agent.

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <sstream>
#include <thread>
#include <unistd.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include "agent.h"
#include "docker_runtime.h"
#include "fleet.grpc.pb.h"
#include "master_server.h"
#include "model.h"

namespace fleet {
namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;
using Argv = std::vector<std::string>;

// ------------------------------------------------------------ pure functions

TEST(DockerArgs, FormatCpusIsExact) {
  EXPECT_EQ(format_cpus(500), "0.5");
  EXPECT_EQ(format_cpus(1500), "1.5");
  EXPECT_EQ(format_cpus(2000), "2");
  EXPECT_EQ(format_cpus(250), "0.25");
  EXPECT_EQ(format_cpus(1), "0.001");
  EXPECT_EQ(format_cpus(10), "0.01");
  EXPECT_EQ(format_cpus(12345), "12.345");
}

TEST(DockerArgs, RunArgvMatchesTheSpec) {
  DockerRuntimeOptions o;
  o.docker = "docker";
  LaunchSpec s;
  s.argv = {"/bin/sh", "-c", "echo hi"};
  s.limits = Resources{500, 64};
  s.name = "fleet-7-2-0011aabb";
  EXPECT_EQ(docker_run_argv(o, s),
            (Argv{"docker", "run", "-d", "--name", "fleet-7-2-0011aabb", "--init",
                  "--cpus=0.5", "--memory=64m", "--memory-swap=64m", "busybox:1.36",
                  "/bin/sh", "-c", "echo hi"}));
  // An explicit image replaces the default; the command's own dashes are
  // passed through after it untouched.
  s.image = "alpine:3.20";
  s.argv = {"ls", "-la", "--color=never"};
  s.limits = Resources{2000, 1024};
  EXPECT_EQ(docker_run_argv(o, s),
            (Argv{"docker", "run", "-d", "--name", "fleet-7-2-0011aabb", "--init",
                  "--cpus=2", "--memory=1024m", "--memory-swap=1024m", "alpine:3.20",
                  "ls", "-la", "--color=never"}));
}

TEST(DockerArgs, ImageReferencesThatCouldBeFlagsAreRejected) {
  EXPECT_TRUE(valid_image_ref("busybox:1.36"));
  EXPECT_TRUE(valid_image_ref("ghcr.io/org/app:v1.2"));
  EXPECT_TRUE(valid_image_ref("alpine@sha256:abcdef0123"));
  EXPECT_TRUE(valid_image_ref("localhost:5000/x"));
  EXPECT_FALSE(valid_image_ref(""));
  EXPECT_FALSE(valid_image_ref("--privileged"));
  EXPECT_FALSE(valid_image_ref("-v"));
  EXPECT_FALSE(valid_image_ref("alpine latest"));
  EXPECT_FALSE(valid_image_ref("a;rm -rf /"));
  EXPECT_FALSE(valid_image_ref(std::string(256, 'a')));
}

TEST(DockerArgs, ContainerNamesIncludeTaskAttemptAndEpoch) {
  EXPECT_EQ(Agent::container_name(3, 1, 0x9f2c4a1b00000000ULL), "fleet-3-1-9f2c4a1b00000000");
  EXPECT_EQ(Agent::container_name(0, 12, 0x1ULL), "fleet-0-12-1");
  // Same task and attempt under two registrations: different names, so a
  // container left over from before a restart cannot block a new start.
  // m3 epochs are consecutive counter values, so they differ only in their
  // low digits; the whole epoch has to be in the name.
  const uint64_t e = 0x63f2a1b2c3d40ULL;  // a typical wall-clock-seeded epoch
  EXPECT_NE(Agent::container_name(5, 1, e), Agent::container_name(5, 1, e + 1));
}

// ------------------------------------------------------------ the fake CLI

// Records argv as one line per call, fields separated by \037 (ASCII unit
// separator, which no argument contains), built into one variable and
// written with one printf so concurrent calls do not interleave. Control
// files in the directory switch behaviors: daemon_down (every call fails
// like a stopped daemon), stop_ignored (docker stop leaves the container
// running, to exercise the retry).
constexpr char kFakeDocker[] = R"SH(#!/bin/sh
D=$(dirname "$0")
C="$D/c"
mkdir -p "$C"
line=$(printf '%s\037' "$@")
printf '%s\n' "$line" >> "$D/argv.log"
if [ -e "$D/daemon_down" ]; then
  echo "Cannot connect to the Docker daemon at unix:///var/run/docker.sock. Is the docker daemon running?" >&2
  exit 1
fi
cmd=$1; shift
case "$cmd" in
version) echo "29.0.0-fake"; exit 0 ;;
run)
  name=""
  while [ $# -gt 0 ]; do
    case "$1" in
      -d|--init) shift ;;
      --name) name=$2; shift 2 ;;
      --*) shift ;;
      *) break ;;
    esac
  done
  image=$1; shift
  case "$image" in
    fail/*)
      echo "docker: Error response from daemon: pull access denied for $image, repository does not exist" >&2
      echo "" >&2
      echo "Run 'docker run --help' for more information" >&2
      exit 125 ;;
    slow/*) sleep 1 ;;
  esac
  echo "$*" > "$C/$name.out"
  echo "running 0 false" > "$C/$name"
  echo "0123456789abcdef"
  exit 0 ;;
inspect)
  shift 2
  rc=0
  for n in "$@"; do
    if [ -f "$C/$n" ]; then echo "/$n $(cat "$C/$n")"; else echo "error: no such object: $n" >&2; rc=1; fi
  done
  exit $rc ;;
stop)
  shift 2
  for n in "$@"; do
    if [ ! -f "$C/$n" ]; then echo "Error response from daemon: No such container: $n" >&2; exit 1; fi
    if [ ! -e "$D/stop_ignored" ] && grep -q '^running' "$C/$n"; then echo "exited 143 false" > "$C/$n"; fi
    echo "$n"
  done
  exit 0 ;;
logs) cat "$C/$1.out" 2>/dev/null; exit 0 ;;
ps)
  # Lists every container it knows; the label filter is checked by the
  # test from argv.log, not applied here.
  for f in "$C"/*; do
    case "$f" in *.out) ;; *) [ -f "$f" ] && basename "$f" ;; esac
  done
  exit 0 ;;
rm)
  [ "$1" = "-f" ] && shift
  for n in "$@"; do rm -f "$C/$n" "$C/$n.out"; echo "$n"; done
  exit 0 ;;
*) echo "fake docker: unknown command $cmd" >&2; exit 1 ;;
esac
)SH";

class FakeDockerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::path("/private/tmp/fleet") /
           ("dk-" + std::to_string(::getpid()) + "-" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(dir_);
    fs::create_directories(dir_ / "c");
    docker_ = dir_ / "docker";
    std::ofstream(docker_) << kFakeDocker;
    fs::permissions(docker_, fs::perms::owner_all | fs::perms::group_read |
                                 fs::perms::group_exec | fs::perms::others_read |
                                 fs::perms::others_exec);
  }
  void TearDown() override { fs::remove_all(dir_); }

  DockerRuntimeOptions options() const {
    DockerRuntimeOptions o;
    o.docker = docker_.string();
    o.poll_interval = 20ms;  // fast tests; production default is 250 ms
    o.stop_grace = 1500ms;
    return o;
  }

  LaunchSpec spec(const std::string& name, Argv argv = {"/bin/sh", "-c", "echo hi"},
                  const std::string& image = "") const {
    LaunchSpec s;
    s.argv = std::move(argv);
    s.limits = Resources{500, 64};
    s.log_path = dir_ / (name + ".log");
    s.image = image;
    s.name = name;
    return s;
  }

  // Every recorded call whose first argument is `sub` (run, inspect, ...).
  std::vector<Argv> calls(const std::string& sub) const {
    std::vector<Argv> out;
    std::ifstream in(dir_ / "argv.log");
    std::string line;
    while (std::getline(in, line)) {
      Argv a;
      std::string field;
      for (char c : line) {
        if (c == '\037') {
          a.push_back(field);
          field.clear();
        } else {
          field += c;
        }
      }
      if (!a.empty() && a[0] == sub) out.push_back(a);
    }
    return out;
  }

  // Simulates the container changing state (written then renamed, so the
  // fake never reads a half-written file).
  void set_state(const std::string& name, const std::string& state) const {
    const fs::path tmp = dir_ / "c" / (name + ".tmp");
    std::ofstream(tmp) << state << "\n";
    fs::rename(tmp, dir_ / "c" / name);
  }
  bool exists(const std::string& name) const { return fs::exists(dir_ / "c" / name); }
  void touch(const std::string& file) const { std::ofstream(dir_ / file) << ""; }

  static bool wait_for(const std::function<bool()>& pred, std::chrono::milliseconds limit = 10s) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < end) {
      if (pred()) return true;
      std::this_thread::sleep_for(5ms);
    }
    return pred();
  }

  // Calls reap() (as the agent loop does) until n exits arrive.
  static std::vector<ExitInfo> reap_n(Runtime& rt, size_t n,
                                      std::chrono::milliseconds limit = 10s) {
    std::vector<ExitInfo> all;
    wait_for([&] {
      for (ExitInfo& e : rt.reap()) all.push_back(std::move(e));
      return all.size() >= n;
    }, limit);
    return all;
  }

  // Calls reap() until pred() holds; for waiting on background commands.
  static bool reap_until(Runtime& rt, const std::function<bool()>& pred) {
    return wait_for([&] {
      EXPECT_TRUE(rt.reap().empty());
      return pred();
    });
  }

  static std::string slurp(const fs::path& p) {
    std::ifstream in(p);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
  }

  fs::path dir_;
  fs::path docker_;
};

TEST_F(FakeDockerTest, ProbeReportsAvailability) {
  DockerProbe ok = probe_docker(docker_.string(), 5s);
  EXPECT_TRUE(ok.available) << ok.detail;
  EXPECT_EQ(ok.detail, "29.0.0-fake");

  touch("daemon_down");
  DockerProbe down = probe_docker(docker_.string(), 5s);
  EXPECT_FALSE(down.available);
  EXPECT_NE(down.detail.find("Cannot connect"), std::string::npos) << down.detail;

  DockerProbe missing = probe_docker((dir_ / "no-docker-here").string(), 5s);
  EXPECT_FALSE(missing.available);
}

TEST_F(FakeDockerTest, RunsExactArgvAndReportsExitWithLogsThenRemoves) {
  DockerRuntime rt(options());
  StartResult r = rt.start(spec("fleet-1-1-aaaa", {"/bin/sh", "-c", "echo hi; exit 3"}));
  ASSERT_TRUE(r.ok) << r.error;
  EXPECT_EQ(rt.live_count(), 1u);
  ASSERT_TRUE(reap_until(rt, [&] { return exists("fleet-1-1-aaaa"); }));
  const auto runs = calls("run");
  ASSERT_EQ(runs.size(), 1u);
  EXPECT_EQ(runs[0], (Argv{"run", "-d", "--name", "fleet-1-1-aaaa", "--init", "--cpus=0.5",
                           "--memory=64m", "--memory-swap=64m", "busybox:1.36", "/bin/sh",
                           "-c", "echo hi; exit 3"}));

  set_state("fleet-1-1-aaaa", "exited 3 false");
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].handle, r.handle);
  EXPECT_EQ(exits[0].exit_code, 3);
  EXPECT_FALSE(exits[0].oom_killed);
  EXPECT_FALSE(exits[0].start_failed);
  EXPECT_EQ(rt.live_count(), 0u);
  // The log was copied (docker logs) before the exit was reported.
  EXPECT_NE(slurp(dir_ / "fleet-1-1-aaaa.log").find("echo hi; exit 3"), std::string::npos);
  EXPECT_EQ(calls("logs").size(), 1u);
  // Then the container is removed, without -f: it has already exited.
  ASSERT_TRUE(reap_until(rt, [&] { return !exists("fleet-1-1-aaaa"); }));
  const auto rms = calls("rm");
  ASSERT_EQ(rms.size(), 1u);
  EXPECT_EQ(rms[0], (Argv{"rm", "fleet-1-1-aaaa"}));
  EXPECT_TRUE(rt.reap().empty());  // reported exactly once
}

TEST_F(FakeDockerTest, ReportsOomKill) {
  DockerRuntime rt(options());
  StartResult r = rt.start(spec("fleet-2-1-aaaa"));
  ASSERT_TRUE(r.ok) << r.error;
  ASSERT_TRUE(reap_until(rt, [&] { return exists("fleet-2-1-aaaa"); }));
  set_state("fleet-2-1-aaaa", "exited 137 true");
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_TRUE(exits[0].oom_killed);
  EXPECT_EQ(exits[0].exit_code, 137);
  EXPECT_EQ(exits[0].signal, 0);  // Docker's integer is passed through
  EXPECT_NE(exits[0].message.find("64 MB memory limit"), std::string::npos)
      << exits[0].message;
}

TEST_F(FakeDockerTest, Exit137WithoutOomIsNotAnOom) {
  // 137 is any SIGKILL (docker stop's escalation, kill -9): only Docker's
  // OOMKilled flag makes it an OOM.
  DockerRuntime rt(options());
  ASSERT_TRUE(rt.start(spec("fleet-3-1-aaaa")).ok);
  ASSERT_TRUE(reap_until(rt, [&] { return exists("fleet-3-1-aaaa"); }));
  set_state("fleet-3-1-aaaa", "exited 137 false");
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_FALSE(exits[0].oom_killed);
  EXPECT_EQ(exits[0].exit_code, 137);
}

TEST_F(FakeDockerTest, RunFailureIsAStartFailureWithDockersMessage) {
  DockerRuntime rt(options());
  StartResult r = rt.start(spec("fleet-4-1-aaaa", {"true"}, "fail/image"));
  ASSERT_TRUE(r.ok) << r.error;  // accepted: docker has not answered yet
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_TRUE(exits[0].start_failed);
  EXPECT_EQ(exits[0].exit_code, 125);
  // The first line of docker's stderr, not the generic --help hint.
  EXPECT_NE(exits[0].message.find("pull access denied"), std::string::npos)
      << exits[0].message;
  EXPECT_EQ(exits[0].message.find("--help"), std::string::npos) << exits[0].message;
  // A failed run may leave a created container: removed by name, forced.
  ASSERT_TRUE(reap_until(rt, [&] { return calls("rm").size() == 1; }));
  EXPECT_EQ(calls("rm")[0], (Argv{"rm", "-f", "fleet-4-1-aaaa"}));
}

TEST_F(FakeDockerTest, InvalidImageAndMissingCliFailSynchronously) {
  DockerRuntime rt(options());
  StartResult bad = rt.start(spec("fleet-5-1-aaaa", {"true"}, "--privileged"));
  EXPECT_FALSE(bad.ok);
  EXPECT_NE(bad.error.find("invalid image"), std::string::npos) << bad.error;
  EXPECT_TRUE(calls("run").empty());  // never reached docker

  DockerRuntimeOptions o = options();
  o.docker = (dir_ / "no-docker-here").string();
  DockerRuntime none(o);
  StartResult r = none.start(spec("fleet-5-2-aaaa"));
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.error.find("not found"), std::string::npos) << r.error;
}

TEST_F(FakeDockerTest, StopIsDockerStopWithGraceInSecondsOnce) {
  DockerRuntime rt(options());
  StartResult r = rt.start(spec("fleet-6-1-aaaa", {"sleep", "600"}));
  ASSERT_TRUE(r.ok);
  ASSERT_TRUE(reap_until(rt, [&] { return exists("fleet-6-1-aaaa"); }));
  // Let one inspect see it running, so the runtime is in its steady state.
  ASSERT_TRUE(reap_until(rt, [&] { return !calls("inspect").empty(); }));
  rt.stop(r.handle);
  rt.stop(r.handle);  // idempotent
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].exit_code, 143);  // SIGTERM, as docker reports it
  const auto stops = calls("stop");
  ASSERT_EQ(stops.size(), 1u);
  // 1500 ms of grace rounds up to 2 s: never less grace than asked for.
  EXPECT_EQ(stops[0], (Argv{"stop", "-t", "2", "fleet-6-1-aaaa"}));
  ASSERT_TRUE(reap_until(rt, [&] { return !exists("fleet-6-1-aaaa"); }));
}

TEST_F(FakeDockerTest, StopDuringStartIsSentWhenRunReturns) {
  DockerRuntime rt(options());
  // slow/ makes the fake's run take 1 s, so the stop arrives mid-start.
  StartResult r = rt.start(spec("fleet-7-1-aaaa", {"sleep", "600"}, "slow/image"));
  ASSERT_TRUE(r.ok);
  rt.stop(r.handle);
  EXPECT_TRUE(calls("stop").empty());  // nothing to stop yet
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].exit_code, 143);
  EXPECT_EQ(calls("stop").size(), 1u);
}

TEST_F(FakeDockerTest, FailedStopIsRetriedAtThePollCadence) {
  touch("stop_ignored");
  DockerRuntime rt(options());
  StartResult r = rt.start(spec("fleet-8-1-aaaa", {"sleep", "600"}));
  ASSERT_TRUE(r.ok);
  ASSERT_TRUE(reap_until(rt, [&] { return exists("fleet-8-1-aaaa"); }));
  ASSERT_TRUE(reap_until(rt, [&] { return !calls("inspect").empty(); }));
  rt.stop(r.handle);
  // The container keeps running, so each later inspect sends stop again.
  ASSERT_TRUE(reap_until(rt, [&] { return calls("stop").size() >= 3; }));
  fs::remove(dir_ / "stop_ignored");
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].exit_code, 143);
}

TEST_F(FakeDockerTest, OneInspectCoversEveryRunningContainer) {
  DockerRuntime rt(options());
  for (const char* n : {"fleet-9-1-aaaa", "fleet-10-1-aaaa", "fleet-11-1-aaaa"}) {
    ASSERT_TRUE(rt.start(spec(n)).ok);
  }
  ASSERT_TRUE(reap_until(rt, [&] {
    for (const Argv& a : calls("inspect")) {
      if (a.size() == 6) return true;  // inspect --format FMT + 3 names
    }
    return false;
  }));
  for (const Argv& a : calls("inspect")) {
    ASSERT_GE(a.size(), 4u);
    EXPECT_EQ(a[1], "--format");
    EXPECT_EQ(a[2], "{{.Name}} {{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}}");
  }
}

TEST_F(FakeDockerTest, ContainerRemovedBehindOurBackIsReportedAsVanished) {
  DockerRuntime rt(options());
  ASSERT_TRUE(rt.start(spec("fleet-12-1-aaaa")).ok);
  ASSERT_TRUE(reap_until(rt, [&] { return exists("fleet-12-1-aaaa"); }));
  fs::remove(dir_ / "c" / "fleet-12-1-aaaa");
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].exit_code, -1);
  EXPECT_NE(exits[0].message.find("vanished"), std::string::npos) << exits[0].message;
}

TEST_F(FakeDockerTest, DaemonOutageIsNotAnExit) {
  // An inspect that fails for a reason other than "no such object" says
  // nothing about the container: it must stay running, not be reported.
  DockerRuntime rt(options());
  ASSERT_TRUE(rt.start(spec("fleet-13-1-aaaa")).ok);
  ASSERT_TRUE(reap_until(rt, [&] { return exists("fleet-13-1-aaaa"); }));
  touch("daemon_down");
  const size_t before = calls("inspect").size();
  ASSERT_TRUE(reap_until(rt, [&] { return calls("inspect").size() >= before + 3; }));
  EXPECT_EQ(rt.live_count(), 1u);
  fs::remove(dir_ / "daemon_down");
  set_state("fleet-13-1-aaaa", "exited 0 false");
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].exit_code, 0);
}

TEST_F(FakeDockerTest, KillAllForceRemovesEverythingInOneCall) {
  DockerRuntime rt(options());
  ASSERT_TRUE(rt.start(spec("fleet-14-1-aaaa")).ok);
  ASSERT_TRUE(rt.start(spec("fleet-15-1-aaaa")).ok);
  rt.kill_all();  // waits for the in-flight runs, then rm -f both
  EXPECT_EQ(rt.live_count(), 0u);
  EXPECT_FALSE(exists("fleet-14-1-aaaa"));
  EXPECT_FALSE(exists("fleet-15-1-aaaa"));
  const auto rms = calls("rm");
  ASSERT_EQ(rms.size(), 1u);
  EXPECT_EQ(rms[0], (Argv{"rm", "-f", "fleet-14-1-aaaa", "fleet-15-1-aaaa"}));
}

TEST_F(FakeDockerTest, OwnerLabelsContainersAndKillLeftoversRemovesThem) {
  DockerRuntimeOptions o = options();
  o.owner = "n7";
  // `before` plays a fleetlet that crashed with a container running;
  // `after` is the next fleetlet of the same node.
  DockerRuntime before(o);
  ASSERT_TRUE(before.start(spec("fleet-1-1-aa")).ok);
  ASSERT_TRUE(reap_until(before, [&] { return exists("fleet-1-1-aa"); }));
  const auto runs = calls("run");
  ASSERT_EQ(runs.size(), 1u);
  EXPECT_NE(std::find(runs[0].begin(), runs[0].end(), "--label=fleet.owner=n7"), runs[0].end());

  DockerRuntime after(o);
  EXPECT_EQ(after.kill_leftovers(), 1u);
  EXPECT_FALSE(exists("fleet-1-1-aa"));
  const auto ps = calls("ps");
  ASSERT_EQ(ps.size(), 1u);
  EXPECT_EQ(ps[0], (Argv{"ps", "-aq", "--filter", "label=fleet.owner=n7"}));
  // No owner: no label, and nothing is ever removed.
  DockerRuntime anonymous(options());
  EXPECT_EQ(anonymous.kill_leftovers(), 0u);
  EXPECT_EQ(calls("ps").size(), 1u);
}

// ------------------------------------------------------- end to end, fake CLI

TEST_F(FakeDockerTest, ImageAndOomTravelThroughMasterAndAgent) {
  MasterServerOptions mo;
  mo.listen_address = "127.0.0.1:0";
  mo.tick_interval = 50ms;
  mo.core.heartbeat_interval_ms = 100;
  // 3 s of silence before a node is dead, not the default 600 ms at this
  // cadence: a slow sanitizer build must not have the node declared dead
  // in the middle of a test about something else.
  mo.core.suspect_after_missed = 10;
  mo.core.dead_after_missed = 30;
  MasterServer master(mo);
  std::string err;
  ASSERT_TRUE(master.start(&err)) << err;
  const std::string address = "127.0.0.1:" + std::to_string(master.port());
  auto stub = v1::Master::NewStub(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()));

  AgentOptions ao;
  ao.master_address = address;
  ao.name = "d1";
  ao.capacity = {4000, 4096};
  ao.work_dir = dir_ / "d1";
  auto agent = std::make_unique<Agent>(ao, std::make_unique<DockerRuntime>(options()));
  agent->start_background();

  auto ctx = [] {
    auto c = std::make_unique<grpc::ClientContext>();
    c->set_deadline(std::chrono::system_clock::now() + 2s);
    return c;
  };
  auto task = [&](uint32_t id) {
    v1::ListResponse all;
    EXPECT_TRUE(stub->List(ctx().get(), v1::ListRequest{}, &all).ok());
    for (const auto& t : all.tasks()) {
      if (t.id() == id) return t;
    }
    return v1::TaskInfo{};
  };

  // A bad image is refused at submit, before any node sees it.
  v1::SubmitRequest bad;
  bad.set_job("bad");
  bad.mutable_request()->set_milli_cpu(100);
  bad.mutable_request()->set_memory_mb(32);
  bad.add_argv("true");
  bad.set_image("--privileged");
  v1::SubmitResponse bad_resp;
  EXPECT_EQ(stub->Submit(ctx().get(), bad, &bad_resp).error_code(),
            grpc::StatusCode::INVALID_ARGUMENT);

  v1::SubmitRequest req;
  req.set_job("hog");
  req.mutable_request()->set_milli_cpu(500);
  req.mutable_request()->set_memory_mb(32);
  req.add_argv("/bin/sh");
  req.add_argv("-c");
  req.add_argv("head -c 200m /dev/zero | tail");
  req.set_image("alpine:3.20");
  v1::SubmitResponse resp;
  ASSERT_TRUE(stub->Submit(ctx().get(), req, &resp).ok());
  const uint32_t id = resp.task_ids(0);

  // The container appears under the agent's name for this instance.
  const auto runs = [&] { return calls("run"); };
  ASSERT_TRUE(wait_for([&] { return !runs().empty(); }));
  const Argv run = runs()[0];
  ASSERT_GE(run.size(), 10u);
  const std::string name = run[3];
  EXPECT_EQ(name.rfind("fleet-" + std::to_string(id) + "-1-", 0), 0u) << name;
  EXPECT_EQ(run[8], "alpine:3.20");
  EXPECT_EQ(run[6], "--memory=32m");
  ASSERT_TRUE(wait_for([&] { return task(id).state() == v1::TASK_STATE_RUNNING; }));
  EXPECT_EQ(task(id).image(), "alpine:3.20");

  ASSERT_TRUE(wait_for([&] { return exists(name); }));
  set_state(name, "exited 137 true");
  ASSERT_TRUE(wait_for([&] { return task(id).state() == v1::TASK_STATE_FAILED; }));
  const v1::TaskInfo t = task(id);
  EXPECT_TRUE(t.has_exit());
  EXPECT_TRUE(t.oom_killed());
  EXPECT_EQ(t.exit_code(), 137);
  EXPECT_NE(t.message().find("OOM-killed"), std::string::npos) << t.message();

  agent->request_stop();
  agent.reset();
  master.with_core([](MasterCore& c) {
    const std::string e = c.check_invariants();
    EXPECT_TRUE(e.empty()) << e;
  });
  master.shutdown();
}

// ------------------------------------------------------------ real Docker

constexpr char kImage[] = "busybox:1.36";

class RealDockerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const DockerProbe p = probe_docker("docker", 5s);
    if (!p.available) GTEST_SKIP() << "Docker not available: " << p.detail;
    std::string err;
    auto img = Subprocess::spawn({"docker", "image", "inspect", kImage}, 10s, &err);
    if (!img) GTEST_SKIP() << err;
    img->wait();
    if (!img->result().ok()) {
      GTEST_SKIP() << kImage << " not pulled; run `docker pull " << kImage << "`";
    }
    dir_ = fs::path("/private/tmp/fleet") /
           ("rd-" + std::to_string(::getpid()) + "-" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::create_directories(dir_);
  }
  void TearDown() override {
    if (!dir_.empty()) fs::remove_all(dir_);
  }

  LaunchSpec spec(const std::string& tag, Argv argv, Resources limits = {500, 64}) const {
    LaunchSpec s;
    s.argv = std::move(argv);
    s.limits = limits;
    s.name = "fleet-test-" + std::to_string(::getpid()) + "-" + tag;
    s.log_path = dir_ / (tag + ".log");
    return s;
  }

  static DockerRuntimeOptions options() {
    DockerRuntimeOptions o;
    o.poll_interval = 100ms;
    return o;
  }

  static std::vector<ExitInfo> reap_n(Runtime& rt, size_t n, std::chrono::seconds limit = 30s) {
    std::vector<ExitInfo> all;
    const auto end = std::chrono::steady_clock::now() + limit;
    while (all.size() < n && std::chrono::steady_clock::now() < end) {
      for (ExitInfo& e : rt.reap()) all.push_back(std::move(e));
      std::this_thread::sleep_for(10ms);
    }
    return all;
  }

  // Does a container with exactly this name exist (in any state)?
  static bool container_exists(const std::string& name) {
    std::string err;
    auto p = Subprocess::spawn({"docker", "ps", "-aq", "--filter", "name=^/" + name + "$"},
                               10s, &err);
    if (!p) return false;
    p->wait();
    return !p->result().out.empty();
  }

  fs::path dir_;
};

TEST_F(RealDockerTest, ExitCodeLogsAndRemoval) {
  DockerRuntime rt(options());
  LaunchSpec s = spec("exit", {"/bin/sh", "-c", "echo hello from a container; exit 3"});
  ASSERT_TRUE(rt.start(s).ok);
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_FALSE(exits[0].start_failed) << exits[0].message;
  EXPECT_EQ(exits[0].exit_code, 3);
  EXPECT_FALSE(exits[0].oom_killed);
  std::ifstream in(s.log_path);
  std::stringstream ss;
  ss << in.rdbuf();
  EXPECT_NE(ss.str().find("hello from a container"), std::string::npos) << ss.str();
  // The background rm finishes on a later reap.
  const auto end = std::chrono::steady_clock::now() + 10s;
  while (container_exists(s.name) && std::chrono::steady_clock::now() < end) rt.reap();
  EXPECT_FALSE(container_exists(s.name));
}

TEST_F(RealDockerTest, MemoryOverLimitIsOomKilled) {
  DockerRuntime rt(options());
  // `tail` keeps the current line in memory and /dev/zero has no newline,
  // so this grows without bound until the cgroup's 32 MB limit kills it.
  ASSERT_TRUE(rt.start(spec("oom", {"/bin/sh", "-c", "head -c 256m /dev/zero | tail"},
                            {500, 32}))
                  .ok);
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_TRUE(exits[0].oom_killed) << "exit " << exits[0].exit_code << " " << exits[0].message;
  EXPECT_EQ(exits[0].exit_code, 137);
}

TEST_F(RealDockerTest, StopIsPromptBecauseOfInit) {
  DockerRuntimeOptions o = options();
  o.stop_grace = 10s;
  DockerRuntime rt(o);
  StartResult r = rt.start(spec("stop", {"sleep", "600"}));
  ASSERT_TRUE(r.ok);
  // Wait until it is really running (one inspect has seen it).
  const auto up_by = std::chrono::steady_clock::now() + 10s;
  while (std::chrono::steady_clock::now() < up_by) {
    ASSERT_TRUE(rt.reap().empty());
    if (container_exists(spec("stop", {}).name)) break;
  }
  const auto t0 = std::chrono::steady_clock::now();
  rt.stop(r.handle);
  auto exits = reap_n(rt, 1);
  const auto took = std::chrono::steady_clock::now() - t0;
  ASSERT_EQ(exits.size(), 1u);
  // 143 = SIGTERM: tini forwarded it and sleep died, well inside the 10 s
  // grace. Without --init, sleep as PID 1 ignores SIGTERM and this would
  // be 137 after 10 s.
  EXPECT_EQ(exits[0].exit_code, 143);
  EXPECT_LT(took, 5s);
}

}  // namespace
}  // namespace fleet
