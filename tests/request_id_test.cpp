// Tests for idempotent submissions (m4b): a SubmitRequest's request_id
// makes a retry return the first attempt's task ids instead of admitting
// the job again, in the live master, after replaying its log (what every
// Raft follower does), and after a snapshot (what a restarted or lagging
// replica loads). bench/leader_failover.sh found the case this closes: a
// leader committed a submission, was SIGKILLed before replying, and the
// client's retry at the new leader admitted the job a second time.

#include <gtest/gtest.h>

#include "master_core.h"
#include "master_test_helpers.h"
#include "wal_test_helpers.h"

namespace fleet {
namespace {

using namespace fleet::testing;

v1::SubmitRequest request(const std::string& job, const std::string& id, uint32_t count = 2) {
  v1::SubmitRequest req;
  req.set_job(job);
  req.set_priority(1);
  req.mutable_request()->set_milli_cpu(100);
  req.mutable_request()->set_memory_mb(16);
  req.add_argv("true");
  req.set_count(count);
  req.set_request_id(id);
  return req;
}

std::vector<uint32_t> ids(const v1::SubmitResponse& r) {
  return {r.task_ids().begin(), r.task_ids().end()};
}

TEST(RequestId, ARetryIsAnsweredWithTheFirstTaskIdsAndAdmitsNothing) {
  MasterCore core(durable_test_options(), 1);
  v1::SubmitResponse first, again;
  ASSERT_TRUE(core.submit(request("web", "r-1"), &first).ok());
  EXPECT_EQ(ids(first), (std::vector<uint32_t>{0, 1}));
  EXPECT_EQ(core.take_records().size(), 1u);
  ASSERT_TRUE(core.submit(request("web", "r-1"), &again).ok());
  EXPECT_EQ(ids(again), ids(first));
  EXPECT_TRUE(core.take_records().empty());  // nothing new to replicate
  EXPECT_EQ(core.state().tasks().size(), 2u);
  // Without an id, or with another one, it is a new submission.
  v1::SubmitResponse other;
  ASSERT_TRUE(core.submit(request("web", ""), &other).ok());
  ASSERT_TRUE(core.submit(request("web", "r-2"), &other).ok());
  EXPECT_EQ(core.state().tasks().size(), 6u);
  EXPECT_CORE_OK(core);
}

TEST(RequestId, AnIdReusedForAnotherJobIsRefused) {
  MasterCore core(durable_test_options(), 1);
  v1::SubmitResponse r;
  ASSERT_TRUE(core.submit(request("web", "r-1"), &r).ok());
  EXPECT_EQ(core.submit(request("db", "r-1"), &r).error_code(), grpc::StatusCode::ALREADY_EXISTS);
}

TEST(RequestId, FollowersAndSnapshotsRememberAdmittedIds) {
  MasterCore core(durable_test_options(), 1);
  v1::SubmitResponse r;
  ASSERT_TRUE(core.submit(request("web", "r-1", 3), &r).ok());
  ASSERT_TRUE(core.submit(request("db", "r-2", 1), &r).ok());
  // A follower applying the leader's records...
  MasterState follower;
  for (const v1::WalRecord& rec : core.take_records()) ASSERT_EQ(follower.apply(rec), "");
  const auto sub = follower.find_submission("r-1");
  ASSERT_TRUE(sub);
  EXPECT_EQ(sub->first_task_id, 0u);
  EXPECT_EQ(sub->count, 3u);
  // ...and a replica loading its snapshot both know them.
  v1::Snapshot snap;
  follower.to_snapshot(&snap);
  std::string err;
  const auto loaded = MasterState::from_snapshot(snap, &err);
  ASSERT_TRUE(loaded) << err;
  EXPECT_EQ(state_diff(*loaded, follower), "");
  ASSERT_TRUE(loaded->find_submission("r-2"));
  EXPECT_EQ(loaded->find_submission("r-2")->first_task_id, 3u);
  // A leader built from that state answers a retry without admitting.
  MasterCore leader(durable_test_options(), *loaded, kT0, 1);
  v1::SubmitResponse retry;
  ASSERT_TRUE(leader.submit(request("web", "r-1", 3), &retry).ok());
  EXPECT_EQ(ids(retry), (std::vector<uint32_t>{0, 1, 2}));
  EXPECT_TRUE(leader.take_records().empty());
  // A log that admits the same id twice does not apply.
  v1::WalRecord dup;
  dup.set_lsn(follower.lsn() + 1);
  v1::TasksSubmitted* ts = dup.mutable_tasks_submitted();
  ts->set_first_task_id(4);
  ts->set_count(1);
  ts->set_job("web");
  ts->set_priority(1);
  ts->mutable_request()->set_milli_cpu(100);
  ts->mutable_request()->set_memory_mb(16);
  ts->add_argv("true");
  ts->set_request_id("r-1");
  EXPECT_NE(follower.apply(dup).find("already admitted"), std::string::npos);
}

TEST(RequestId, ACorruptSubmissionRecordInASnapshotIsRefused) {
  MasterCore core(durable_test_options(), 1);
  v1::SubmitResponse r;
  ASSERT_TRUE(core.submit(request("web", "r-1"), &r).ok());
  v1::Snapshot snap;
  core.durable_state().to_snapshot(&snap);
  snap.mutable_submissions(0)->set_count(99);  // more tasks than exist
  std::string err;
  EXPECT_FALSE(MasterState::from_snapshot(snap, &err));
  EXPECT_NE(err.find("submission"), std::string::npos);
}

}  // namespace
}  // namespace fleet
