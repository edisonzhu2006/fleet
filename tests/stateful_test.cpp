// Tests for the agent side of stateful tasks (m5, agent/stateful.h): the
// resume decision (fresh vs resume, from what the checkpoint directory
// holds) and the attempt fence (a file beside the checkpoint directory that
// records the newest (task, attempt) started on it, so an older attempt is
// refused). Real files under /private/tmp/fleet, no network.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <numeric>
#include <random>
#include <thread>
#include <unistd.h>
#include <vector>

#include "stateful.h"

namespace fleet {
namespace {

namespace fs = std::filesystem;

class StatefulDir : public ::testing::Test {
 protected:
  void SetUp() override {
    root_ = fs::path("/private/tmp/fleet") /
            ("stateful-" + std::to_string(::getpid()) + "-" +
             ::testing::UnitTest::GetInstance()->current_test_info()->name());
    fs::remove_all(root_);
    fs::create_directories(root_);
  }
  void TearDown() override { fs::remove_all(root_); }

  static void touch(const fs::path& p, const std::string& content = "") {
    std::ofstream(p) << content;
  }
  CheckpointDirState inspect(const fs::path& d) {
    std::string err;
    CheckpointDirState s = inspect_checkpoint_dir(d, &err);
    EXPECT_EQ(err, "");
    return s;
  }

  fs::path root_;
};

// ------------------------------------------------------- resume decision

TEST(ResumeDecision, FreshOnlyWhenNothingIsThere) {
  // {exists, has_manifest, has_other} -> mode, every combination that can
  // occur (a missing directory has no entries).
  EXPECT_EQ(decide_start_mode({false, false, false}), StartMode::kFresh);
  EXPECT_EQ(decide_start_mode({true, false, false}), StartMode::kFresh);
  // A committed checkpoint: continue from it.
  EXPECT_EQ(decide_start_mode({true, true, false}), StartMode::kResume);
  EXPECT_EQ(decide_start_mode({true, true, true}), StartMode::kResume);
  // Leftovers of an attempt that died before its first commit: the resume
  // command handles "no MANIFEST" by starting over; argv would be refused
  // by streamkit because the directory holds a previous run.
  EXPECT_EQ(decide_start_mode({true, false, true}), StartMode::kResume);
}

TEST_F(StatefulDir, InspectSeesMissingEmptyCommittedAndLeftovers) {
  const fs::path d = root_ / "ckpt";
  CheckpointDirState s = inspect(d);
  EXPECT_FALSE(s.exists);
  EXPECT_EQ(decide_start_mode(s), StartMode::kFresh);

  fs::create_directories(d);
  s = inspect(d);
  EXPECT_TRUE(s.exists);
  EXPECT_FALSE(s.has_manifest);
  EXPECT_FALSE(s.has_other);
  EXPECT_EQ(decide_start_mode(s), StartMode::kFresh);

  // streamkit's layout before its first commit: an epoch directory only.
  fs::create_directories(d / "epoch-1");
  s = inspect(d);
  EXPECT_FALSE(s.has_manifest);
  EXPECT_TRUE(s.has_other);
  EXPECT_EQ(decide_start_mode(s), StartMode::kResume);

  touch(d / "MANIFEST", "epoch 1\n");
  s = inspect(d);
  EXPECT_TRUE(s.has_manifest);
  EXPECT_EQ(decide_start_mode(s), StartMode::kResume);
}

TEST_F(StatefulDir, InspectRefusesAFileWhereTheDirectoryShouldBe) {
  touch(root_ / "notadir");
  std::string err;
  inspect_checkpoint_dir(root_ / "notadir", &err);
  EXPECT_NE(err.find("not a directory"), std::string::npos) << err;
}

// ------------------------------------------------------- the fence, pure

TEST(Fence, TokensOrderByTaskThenAttempt) {
  EXPECT_LT((FenceToken{1, 1}), (FenceToken{1, 2}));
  // A later task reusing the directory outranks every attempt of an older one.
  EXPECT_LT((FenceToken{1, 99}), (FenceToken{2, 1}));
  EXPECT_EQ((FenceToken{3, 4}), (FenceToken{3, 4}));
}

TEST(Fence, AllowsNewerAndEqualRefusesOlder) {
  EXPECT_TRUE(fence_allows(std::nullopt, {7, 1}));     // first ever
  EXPECT_TRUE(fence_allows(FenceToken{7, 1}, {7, 2}));  // a later attempt
  EXPECT_TRUE(fence_allows(FenceToken{7, 2}, {7, 2}));  // its own record
  EXPECT_FALSE(fence_allows(FenceToken{7, 3}, {7, 2}));  // superseded
  EXPECT_FALSE(fence_allows(FenceToken{8, 1}, {7, 9}));  // newer task owns it
}

TEST(Fence, FormatRoundTripsAndParseRejectsAnythingElse) {
  const FenceToken t{42, 7};
  EXPECT_EQ(format_fence(t), "task 42 attempt 7\n");
  EXPECT_EQ(parse_fence(format_fence(t)), t);
  EXPECT_EQ(parse_fence("task 4294967295 attempt 4294967295\n"),
            (FenceToken{4294967295u, 4294967295u}));
  for (const char* bad : {"", "task 1 attempt 2", "task 1 attempt 2\n\n", "task 1 attempt 2\nx",
                          "task +1 attempt 2\n", "task 01 attempt 2\n", "task  1 attempt 2\n",
                          "task 4294967296 attempt 1\n", "task -1 attempt 2\n",
                          "attempt 2 task 1\n", "task 1 attempt 2 \n", "garbage"}) {
    EXPECT_EQ(parse_fence(bad), std::nullopt) << "'" << bad << "'";
  }
}

TEST(Fence, LivesBesideTheCheckpointDirNotInIt) {
  EXPECT_EQ(fence_path("/shared/ckpt/job-a"), fs::path("/shared/ckpt/job-a.fleet-fence"));
  // A dot in the name is part of the name, not an extension to replace.
  EXPECT_EQ(fence_path("/shared/job.v2"), fs::path("/shared/job.v2.fleet-fence"));
}

// ------------------------------------------------------- the fence, on disk

TEST_F(StatefulDir, AcquireRecordsNewerAttemptsAndRefusesOlderOnes) {
  const fs::path f = fence_path(root_ / "ckpt");
  FenceResult r = fence_acquire(f, {5, 1});
  EXPECT_TRUE(r.granted);
  EXPECT_EQ(r.recorded, std::nullopt);
  EXPECT_EQ(r.error, "");

  // Attempt 2 (the task moved after its node died) supersedes attempt 1.
  r = fence_acquire(f, {5, 2});
  EXPECT_TRUE(r.granted);
  EXPECT_EQ(r.recorded, (FenceToken{5, 1}));

  // A stale start of attempt 1 now arrives (a node that missed the news):
  // refused, with the newer token named, and the record is unchanged.
  r = fence_acquire(f, {5, 1});
  EXPECT_FALSE(r.granted);
  EXPECT_EQ(r.error, "");
  EXPECT_EQ(r.recorded, (FenceToken{5, 2}));
  std::string err;
  EXPECT_EQ(fence_read(f, &err), (FenceToken{5, 2}));
  EXPECT_EQ(err, "");

  // The same attempt starting again (its first start was never reported)
  // is its own record: granted, nothing rewritten.
  r = fence_acquire(f, {5, 2});
  EXPECT_TRUE(r.granted);

  // A later submission reusing the directory: a larger task id wins even
  // at attempt 1.
  r = fence_acquire(f, {6, 1});
  EXPECT_TRUE(r.granted);
  EXPECT_EQ(fence_read(f, &err), (FenceToken{6, 1}));
}

TEST_F(StatefulDir, AFenceFileThatDoesNotParseRefusesEveryStart) {
  const fs::path f = fence_path(root_ / "ckpt");
  touch(f, "task 5 att");  // torn by a crash mid-write
  const FenceResult r = fence_acquire(f, {5, 9});
  EXPECT_FALSE(r.granted);
  EXPECT_NE(r.error.find("not a fence record"), std::string::npos) << r.error;
  std::string err;
  EXPECT_EQ(fence_read(f, &err), std::nullopt);
  EXPECT_NE(err, "");
}

TEST_F(StatefulDir, ReadOfAMissingOrEmptyFenceIsNoTokenAndNoError) {
  const fs::path f = fence_path(root_ / "ckpt");
  std::string err;
  EXPECT_EQ(fence_read(f, &err), std::nullopt);
  EXPECT_EQ(err, "");
  touch(f);  // created by an acquire that failed before writing
  EXPECT_EQ(fence_read(f, &err), std::nullopt);
  EXPECT_EQ(err, "");
  EXPECT_TRUE(fence_acquire(f, {1, 1}).granted);
}

// Many agents (threads here; flock locks belong to each open() of the file,
// so threads contend exactly like processes) race to start attempts 1..N of
// one task in a random order. Whatever the interleaving: every grant saw a
// recorded token no newer than itself (and since each grant writes its own
// token, that token is the newest grant before it), the newest attempt is
// always granted, and the file ends holding it.
TEST_F(StatefulDir, ConcurrentAcquiresNeverGrantAnOlderAttemptAfterANewerOne) {
  const fs::path f = fence_path(root_ / "ckpt");
  constexpr uint32_t kAttempts = 64;
  std::vector<uint32_t> order(kAttempts);
  std::iota(order.begin(), order.end(), 1u);
  std::shuffle(order.begin(), order.end(), std::mt19937(12345));

  std::mutex mu;
  std::vector<uint32_t> granted_in_order;  // every attempt that was granted
  std::atomic<size_t> next{0};
  std::vector<std::thread> threads;
  std::atomic<int> errors{0};
  for (int t = 0; t < 8; ++t) {
    threads.emplace_back([&] {
      for (size_t i; (i = next.fetch_add(1)) < kAttempts;) {
        const FenceResult r = fence_acquire(f, {9, order[i]});
        if (!r.error.empty()) ++errors;
        if (!r.granted) continue;
        // The grant's own `recorded` must be older than or equal to it:
        // that is the property the lock makes atomic.
        if (r.recorded && r.recorded->attempt > order[i]) ++errors;
        std::lock_guard<std::mutex> lock(mu);
        granted_in_order.push_back(order[i]);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(errors.load(), 0);
  std::string err;
  EXPECT_EQ(fence_read(f, &err), (FenceToken{9, kAttempts}));
  EXPECT_FALSE(granted_in_order.empty());
  EXPECT_NE(std::find(granted_in_order.begin(), granted_in_order.end(), kAttempts),
            granted_in_order.end());
}

}  // namespace
}  // namespace fleet
