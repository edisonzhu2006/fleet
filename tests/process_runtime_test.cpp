// Tests for ProcessRuntime against real processes: exit codes and signals
// are reported once, output lands in the log, a missing command fails at
// start() rather than as exit 127, stop() kills the whole process group
// (including a grandchild), SIGTERM is escalated to SIGKILL after the
// grace period, and kill_all() leaves nothing behind.

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>
#include <unistd.h>

#include "process_runtime.h"

namespace fleet {
namespace {

namespace fs = std::filesystem;
using namespace std::chrono_literals;

class ProcessRuntimeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = fs::path("/private/tmp/fleet") /
           ("rt-" + std::to_string(::getpid()) + "-" +
            ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(dir_);
    fs::create_directories(dir_);
  }
  void TearDown() override { fs::remove_all(dir_); }

  LaunchSpec spec(std::vector<std::string> argv, const std::string& log = "t.log") {
    LaunchSpec s;
    s.argv = std::move(argv);
    s.limits = Resources{100, 64};
    s.log_path = dir_ / log;
    return s;
  }

  // Polls reap() until n exits arrive or the deadline passes.
  static std::vector<ExitInfo> reap_n(Runtime& rt, size_t n,
                                      std::chrono::milliseconds limit = 5000ms) {
    std::vector<ExitInfo> all;
    const auto end = std::chrono::steady_clock::now() + limit;
    while (all.size() < n && std::chrono::steady_clock::now() < end) {
      for (const ExitInfo& e : rt.reap()) all.push_back(e);
      std::this_thread::sleep_for(5ms);
    }
    return all;
  }

  static std::string slurp(const fs::path& p) {
    std::ifstream in(p);
    std::stringstream ss;
    ss << in.rdbuf();
    return ss.str();
  }

  fs::path dir_;
};

TEST_F(ProcessRuntimeTest, ReportsExitCodesOnce) {
  ProcessRuntime rt;
  StartResult ok = rt.start(spec({"true"}, "a.log"));
  StartResult bad = rt.start(spec({"/bin/sh", "-c", "exit 42"}, "b.log"));
  ASSERT_TRUE(ok.ok) << ok.error;
  ASSERT_TRUE(bad.ok) << bad.error;
  auto exits = reap_n(rt, 2);
  ASSERT_EQ(exits.size(), 2u);
  for (const ExitInfo& e : exits) {
    EXPECT_EQ(e.signal, 0);
    EXPECT_EQ(e.exit_code, e.handle == ok.handle ? 0 : 42);
  }
  EXPECT_EQ(rt.live_count(), 0u);
  EXPECT_TRUE(rt.reap().empty());  // reported exactly once
}

TEST_F(ProcessRuntimeTest, CapturesOutputAndStdinIsDevNull) {
  ProcessRuntime rt;
  ASSERT_TRUE(rt.start(spec({"/bin/sh", "-c", "echo out; echo err >&2; cat"})).ok);
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].exit_code, 0);  // cat read EOF from /dev/null at once
  const std::string log = slurp(dir_ / "t.log");
  EXPECT_NE(log.find("out\n"), std::string::npos);
  EXPECT_NE(log.find("err\n"), std::string::npos);
}

TEST_F(ProcessRuntimeTest, MissingCommandFailsAtStart) {
  ProcessRuntime rt;
  StartResult r = rt.start(spec({"definitely-not-a-command-fleet"}));
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.error.find("command not found"), std::string::npos) << r.error;
  // Exists on PATH lookup terms but cannot be exec'd: the error comes back
  // through the CLOEXEC pipe.
  const fs::path not_exec = dir_ / "script";
  std::ofstream(not_exec) << "#!/bin/sh\n";
  r = rt.start(spec({not_exec.string()}));
  EXPECT_FALSE(r.ok);
  EXPECT_NE(r.error.find("Permission denied"), std::string::npos) << r.error;
  EXPECT_EQ(rt.live_count(), 0u);
}

TEST_F(ProcessRuntimeTest, StopKillsWholeProcessGroup) {
  ProcessRuntime rt;
  // The shell starts a grandchild sleep in the background and records its
  // pid, then waits. Stopping the task must take the grandchild too.
  const fs::path pidfile = dir_ / "grandchild.pid";
  StartResult r = rt.start(
      spec({"/bin/sh", "-c", "sleep 30 & echo $! > " + pidfile.string() + "; wait"}));
  ASSERT_TRUE(r.ok) << r.error;
  pid_t grandchild = -1;
  for (int i = 0; i < 200 && grandchild <= 0; ++i) {
    std::this_thread::sleep_for(10ms);
    std::ifstream in(pidfile);
    in >> grandchild;
  }
  ASSERT_GT(grandchild, 0);
  rt.stop(r.handle);
  auto exits = reap_n(rt, 1);
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].signal, SIGTERM);
  // The grandchild is not our child, so poll kill(pid, 0) until it is gone.
  bool gone = false;
  for (int i = 0; i < 200 && !gone; ++i) {
    gone = ::kill(grandchild, 0) != 0;
    if (!gone) std::this_thread::sleep_for(10ms);
  }
  EXPECT_TRUE(gone) << "grandchild " << grandchild << " survived stop";
}

TEST_F(ProcessRuntimeTest, EscalatesToSigkillAfterGrace) {
  ProcessRuntime rt(ProcessRuntimeOptions{std::chrono::milliseconds(200), true});
  // Ignores SIGTERM; only SIGKILL ends it.
  StartResult r = rt.start(spec({"/bin/sh", "-c", "trap '' TERM; while :; do sleep 0.05; done"}));
  ASSERT_TRUE(r.ok) << r.error;
  std::this_thread::sleep_for(100ms);  // let the trap be installed
  const auto t0 = std::chrono::steady_clock::now();
  rt.stop(r.handle);
  auto exits = reap_n(rt, 1);
  const auto took = std::chrono::steady_clock::now() - t0;
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].signal, SIGKILL);
  EXPECT_GE(took, 200ms);
}

TEST_F(ProcessRuntimeTest, KillAllLeavesNothing) {
  ProcessRuntime rt;
  std::vector<pid_t> pids;
  for (int i = 0; i < 3; ++i) {
    StartResult r = rt.start(spec({"sleep", "30"}, "s" + std::to_string(i) + ".log"));
    ASSERT_TRUE(r.ok) << r.error;
    pids.push_back(rt.pid_of(r.handle));
  }
  EXPECT_EQ(rt.live_count(), 3u);
  rt.kill_all();
  EXPECT_EQ(rt.live_count(), 0u);
  for (pid_t p : pids) EXPECT_NE(::kill(p, 0), 0) << p;  // reaped, so gone
}

TEST_F(ProcessRuntimeTest, TaskSeesDefaultSignalState) {
  // The agent blocks SIGTERM in all its threads; the task must not inherit
  // that, or stop() could never end it gracefully.
  sigset_t block, old;
  sigemptyset(&block);
  sigaddset(&block, SIGTERM);
  pthread_sigmask(SIG_BLOCK, &block, &old);
  ProcessRuntime rt;
  StartResult r = rt.start(spec({"sleep", "30"}));
  pthread_sigmask(SIG_SETMASK, &old, nullptr);
  ASSERT_TRUE(r.ok) << r.error;
  rt.stop(r.handle);
  auto exits = reap_n(rt, 1, 1500ms);  // well under the 2 s grace
  ASSERT_EQ(exits.size(), 1u);
  EXPECT_EQ(exits[0].signal, SIGTERM);
}

}  // namespace
}  // namespace fleet
