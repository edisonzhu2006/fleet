// Tests for Subprocess, the non-blocking helper-command runner behind
// DockerRuntime: output and exit codes are captured, poll() never blocks
// on a running command, a command past its deadline is killed and marked
// timed_out, output can be redirected into a file, and a command that is
// not on PATH fails at spawn.

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>
#include <unistd.h>

#include "subprocess.h"

namespace fleet {
namespace {

using namespace std::chrono_literals;

std::unique_ptr<Subprocess> spawn_ok(const std::vector<std::string>& argv,
                                     std::chrono::milliseconds timeout = 5000ms,
                                     int output_fd = -1) {
  std::string err;
  auto p = Subprocess::spawn(argv, timeout, &err, output_fd);
  EXPECT_NE(p, nullptr) << err;
  return p;
}

TEST(Subprocess, CapturesStdoutStderrAndExitCode) {
  auto p = spawn_ok({"/bin/sh", "-c", "echo out; echo err >&2; exit 7"});
  ASSERT_NE(p, nullptr);
  p->wait();
  EXPECT_EQ(p->result().exit_code, 7);
  EXPECT_EQ(p->result().signal, 0);
  EXPECT_FALSE(p->result().timed_out);
  EXPECT_FALSE(p->result().ok());
  EXPECT_EQ(p->result().out, "out\n");
  EXPECT_EQ(p->result().err, "err\n");
}

TEST(Subprocess, LargeOutputDoesNotDeadlock) {
  // 1 MB is far more than a pipe buffer (64 KB): the child blocks writing
  // unless poll() keeps draining while it waits for the exit.
  auto p = spawn_ok({"/bin/sh", "-c", "head -c 1000000 /dev/zero | tr '\\0' x"});
  ASSERT_NE(p, nullptr);
  p->wait();
  EXPECT_TRUE(p->result().ok());
  EXPECT_EQ(p->result().out.size(), 1000000u);
}

TEST(Subprocess, PollDoesNotBlock) {
  auto p = spawn_ok({"/bin/sleep", "2"});
  ASSERT_NE(p, nullptr);
  const auto t0 = std::chrono::steady_clock::now();
  EXPECT_FALSE(p->poll());
  EXPECT_LT(std::chrono::steady_clock::now() - t0, 500ms);
  p->kill();
  p->wait();
  EXPECT_EQ(p->result().signal, SIGKILL);
}

TEST(Subprocess, TimeoutKillsTheCommand) {
  auto p = spawn_ok({"/bin/sleep", "30"}, 100ms);
  ASSERT_NE(p, nullptr);
  const auto t0 = std::chrono::steady_clock::now();
  p->wait();
  EXPECT_LT(std::chrono::steady_clock::now() - t0, 5s);
  EXPECT_TRUE(p->result().timed_out);
  EXPECT_EQ(p->result().signal, SIGKILL);
  EXPECT_FALSE(p->result().ok());
}

TEST(Subprocess, RedirectsOutputToAFile) {
  const std::filesystem::path dir =
      "/private/tmp/fleet/sp-" + std::to_string(::getpid());
  std::filesystem::create_directories(dir);
  const auto file = dir / "out.log";
  const int fd = ::open(file.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
  ASSERT_GE(fd, 0);
  auto p = spawn_ok({"/bin/sh", "-c", "echo to-file; echo also >&2"}, 5000ms, fd);
  ::close(fd);
  ASSERT_NE(p, nullptr);
  p->wait();
  EXPECT_TRUE(p->result().ok());
  EXPECT_TRUE(p->result().out.empty());  // went to the file instead
  std::ifstream in(file);
  std::stringstream ss;
  ss << in.rdbuf();
  EXPECT_NE(ss.str().find("to-file"), std::string::npos);
  EXPECT_NE(ss.str().find("also"), std::string::npos);
  std::filesystem::remove_all(dir);
}

TEST(Subprocess, MissingCommandFailsAtSpawn) {
  std::string err;
  EXPECT_EQ(Subprocess::spawn({"no-such-command-fleet"}, 1000ms, &err), nullptr);
  EXPECT_NE(err.find("command not found"), std::string::npos) << err;
}

TEST(Subprocess, DestructorKillsARunningCommand) {
  pid_t pid = 0;
  {
    auto p = spawn_ok({"/bin/sh", "-c", "echo $$; exec sleep 30"});
    ASSERT_NE(p, nullptr);
    // Wait for the pid to be printed, then drop the Subprocess.
    const auto end = std::chrono::steady_clock::now() + 5s;
    while (std::chrono::steady_clock::now() < end) {
      p->poll();
      if (!p->result().out.empty() && p->result().out.back() == '\n') break;
      std::this_thread::sleep_for(5ms);
    }
    pid = std::stoi(p->result().out);
  }
  // Killed and reaped: the pid no longer exists (not even as a zombie).
  EXPECT_NE(::kill(pid, 0), 0);
}

}  // namespace
}  // namespace fleet
