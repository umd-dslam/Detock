#include "module/scheduler_components/ddr_lock_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

class DDRLockManagerTest : public ::testing::Test {
 protected:
  // This must be define first so that it is destroyed last, thus avoid blocking
  zmq::context_t context;
  zmq::socket_t socket;
  DDRLockManager lock_manager;

  void SetUp() {
    context.set(zmq::ctxopt::blocky, false);
    socket = zmq::socket_t(context, ZMQ_PULL);
    socket.bind(MakeInProcChannelAddress(0));
  }

  bool HasSignalFromResolver() {
    zmq::message_t msg;
    return socket.recv(msg, zmq::recv_flags::dontwait) > 0;
  }
};

TEST_F(DDRLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn = MakeTxnHolder(configs[0], 100, {"readA", "readB"}, {"writeC"});
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(txn);
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"readA", "readB"}, {});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"readB", "readC"}, {});
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn1).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn2).empty());
}

TEST_F(DDRLockManagerTest, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"writeA", "writeB"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"readA"}, {"writeA"});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(txn1).size(), 1U);
  // Make sure the lock is already held by txn2
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::WAITING);
}

TEST_F(DDRLockManagerTest, ReleaseLocksAndReturnMultipleNewLockHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"B"}, {"A"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {}, {"A"});
  auto txn4 = MakeTxnHolder(configs[0], 400, {"C"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn4), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_EQ(result.size(), 2U);
  ASSERT_TRUE(find(result.begin(), result.end(), 200) != result.end());
  ASSERT_TRUE(find(result.begin(), result.end(), 400) != result.end());

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn4).empty());

  result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn3).empty());
}

TEST_F(DDRLockManagerTest, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {}, {"A", "C"});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(300));
}

TEST_F(DDRLockManagerTest, PrioritizeWriteLock) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"A"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyTxn1) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {}, {"B"});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(100));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyTxn2) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {}, {"B"});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_TRUE(lock_manager.AcceptTransaction(txn2));

  auto result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(100));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnlyTxnOutOfOrder) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {"B"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {}, {"B"});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"A"}, {});

  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_EQ(lock_manager.AcquireLocks(txn1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(100));
}

TEST_F(DDRLockManagerTest, MultiEdgeBetweenTwoTxns) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"A", "B"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A", "B"}, {});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {"A"}, {});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {"B"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(txn2);
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, KeyReplicaLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"writeA", "writeB"});

  auto txn2 = MakeTxnHolder(configs[0], 200, {"readA"}, {"writeA"}, {{"readA", {1, 0}}, {"writeA", {1, 0}}});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::ACQUIRED);
}

TEST_F(DDRLockManagerTest, RemasterTxn) {
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto txn = MakeTxnHolder(configs[0], 100, {}, {"A"});
  txn.transaction()->mutable_remaster()->set_new_master(1);
  auto txn_lockonly1 = MakeTxnHolder(configs[0], 100, {}, {"A"});
  txn_lockonly1.transaction()->mutable_remaster()->set_new_master(1);
  auto txn_lockonly2 = MakeTxnHolder(configs[0], 100, {}, {"A"});
  txn_lockonly1.transaction()->mutable_remaster()->set_new_master(1);
  txn_lockonly1.transaction()->mutable_remaster()->set_is_new_master_lock_only(true);

  ASSERT_FALSE(lock_manager.AcceptTransaction(txn));
  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly2), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(txn);

  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn));
  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly2), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(txn);

  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_TRUE(lock_manager.AcceptTransaction(txn));
  lock_manager.ReleaseLocks(txn);
}

TEST_F(DDRLockManagerTest, EnsureStateIsClean) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"B"}, {"A"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {}, {"C"});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn1).empty());

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn2).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(txn3).empty());
}

TEST_F(DDRLockManagerTest, LongChain) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"A"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"A"}, {});
  auto txn3 = MakeTxnHolder(configs[0], 300, {"A"}, {});
  auto txn4 = MakeTxnHolder(configs[0], 400, {}, {"A"});
  auto txn5 = MakeTxnHolder(configs[0], 500, {"A"}, {});

  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn1), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn4), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn5), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, UnorderedElementsAre(200, 300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn2).empty());
  result = lock_manager.ReleaseLocks(txn3);
  ASSERT_THAT(result, ElementsAre(400));

  result = lock_manager.ReleaseLocks(txn4);
  ASSERT_THAT(result, ElementsAre(500));

  ASSERT_TRUE(lock_manager.ReleaseLocks(txn5).empty());
}

TEST_F(DDRLockManagerTest, SimpleDeadlock) {
  lock_manager.StartDeadlockResolver(context, 0, 1ms, true /* init_only */);

  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B"});
  auto txn1_lockonly1 = MakeTxnHolder(configs[0], 100, {}, {"B"});
  auto txn1_lockonly2 = MakeTxnHolder(configs[0], 100, {"A"}, {});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"B"}, {"A"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {"B"}, {});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {}, {"A"});

  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_lockonly2), AcquireLocksResult::WAITING);

  lock_manager.ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver());

  auto ready_txns = lock_manager.GetReadyTxns();
  ASSERT_THAT(ready_txns, ElementsAre(100));

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));
}

TEST_F(DDRLockManagerTest, IncompleteDeadlock) {
  lock_manager.StartDeadlockResolver(context, 0, 1ms, true /* init_only */);

  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn1_lockonly1 = MakeTxnHolder(configs[0], 100, {}, {"B"});
  auto txn1_lockonly2 = MakeTxnHolder(configs[0], 100, {"A"}, {});
  auto txn1_lockonly3 = MakeTxnHolder(configs[0], 100, {}, {"C"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"B"}, {"A"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {"B"}, {});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {}, {"A"});

  // A lock-only txn is missing for txn1 and the main txn is missing for txn2
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_lockonly2), AcquireLocksResult::WAITING);

  lock_manager.ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver());

  // All txns are complete after this point
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_lockonly3), AcquireLocksResult::WAITING);

  lock_manager.ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver());

  auto ready_txns = lock_manager.GetReadyTxns();
  ASSERT_THAT(ready_txns, ElementsAre(100));

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));
}

TEST_F(DDRLockManagerTest, CompleteButUnstableDeadlock) {
  lock_manager.StartDeadlockResolver(context, 0, 1ms, true /* init_only */);

  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B", "C"});
  auto txn1_lockonly1 = MakeTxnHolder(configs[0], 100, {}, {"B", "C"});
  auto txn1_lockonly2 = MakeTxnHolder(configs[0], 100, {"A"}, {});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"B"}, {"A"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {"B"}, {});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {}, {"A"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {"C", "D"}, {});
  auto txn3_lockonly1 = MakeTxnHolder(configs[0], 300, {"C"}, {});
  auto txn3_lockonly2 = MakeTxnHolder(configs[0], 300, {"D"}, {});
  
  // Txn1 and Txn2 forms a deadlock. Their component depends on Txn3, which is still
  // incomplete
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn3));
  ASSERT_EQ(lock_manager.AcquireLocks(txn3_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_lockonly2), AcquireLocksResult::WAITING);

  // No deadlock is resolved because the component is not stable due to its dependency on
  // the incomplete txn
  lock_manager.ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver());

  // All txns are complete after this point
  ASSERT_EQ(lock_manager.AcquireLocks(txn3_lockonly2), AcquireLocksResult::ACQUIRED);

  // The deadlock should be resolved after this but its head txn is not yet immediately ready
  lock_manager.ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver());
  ASSERT_TRUE(lock_manager.GetReadyTxns().empty());

  // Release txn3 so the next one from the deadlock is ready
  auto result = lock_manager.ReleaseLocks(txn3);
  ASSERT_THAT(result, ElementsAre(100));

  result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));
}

TEST_F(DDRLockManagerTest, ConcurrentResolver) {
  // This resolver runs in a different thread so this test might or might not run into
  // a faulty execution. The interval is set to a low number in hope that it is more
  // probable to expose a bug, if any.
  lock_manager.StartDeadlockResolver(context, 0, 1ms);

  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto txn1 = MakeTxnHolder(configs[0], 100, {"A"}, {"B"});
  auto txn1_lockonly1 = MakeTxnHolder(configs[0], 100, {}, {"B"});
  auto txn1_lockonly2 = MakeTxnHolder(configs[0], 100, {"A"}, {});
  auto txn2 = MakeTxnHolder(configs[0], 200, {"B"}, {"A"});
  auto txn2_lockonly1 = MakeTxnHolder(configs[0], 200, {"B"}, {});
  auto txn2_lockonly2 = MakeTxnHolder(configs[0], 200, {}, {"A"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {"B"}, {});
  auto txn4 = MakeTxnHolder(configs[0], 400, {"A"}, {});
  auto txn5 = MakeTxnHolder(configs[0], 500, {"B"}, {});
  auto txn6 = MakeTxnHolder(configs[0], 600, {"A"}, {});
  
  // Txn1 and Txn2 forms a deadlock. Other txns have to wait until this deadlock is resolved
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_lockonly2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn4), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn5), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcceptTxnAndAcquireLocks(txn6), AcquireLocksResult::WAITING);

  zmq::message_t msg;
  (void) socket.recv(msg);
  auto ready_txns = lock_manager.GetReadyTxns();
  ASSERT_THAT(ready_txns, ElementsAre(100));

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, UnorderedElementsAre(200, 300, 500));

  result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, UnorderedElementsAre(400, 600));
}

TEST_F(DDRLockManagerTest, MultipleDeadlocks) {
  lock_manager.StartDeadlockResolver(context, 0, 1ms, true /* init_only */);

  auto configs = MakeTestConfigurations("locking", 1, 1);
  // The key names are the edges that will be formed from these txns
  auto txn1 = MakeTxnHolder(configs[0], 100, {}, {"1->2", "2->1", "4->1"});
  auto txn1_1to2 = MakeTxnHolder(configs[0], 100, {}, {"1->2"});
  auto txn1_2to1 = MakeTxnHolder(configs[0], 100, {}, {"2->1"});
  auto txn1_4to1 = MakeTxnHolder(configs[0], 100, {}, {"4->1"});
  auto txn2 = MakeTxnHolder(configs[0], 200, {}, {"2->1", "2->3", "1->2"});
  auto txn2_2to1 = MakeTxnHolder(configs[0], 200, {}, {"2->1"});
  auto txn2_2to3 = MakeTxnHolder(configs[0], 200, {}, {"2->3"});
  auto txn2_1to2 = MakeTxnHolder(configs[0], 200, {}, {"1->2"});
  auto txn3 = MakeTxnHolder(configs[0], 300, {}, {"3->4", "4->3", "2->3"});
  auto txn3_3to4 = MakeTxnHolder(configs[0], 300, {}, {"3->4"});
  auto txn3_4to3 = MakeTxnHolder(configs[0], 300, {}, {"4->3"});
  auto txn3_2to3 = MakeTxnHolder(configs[0], 300, {}, {"2->3"});
  auto txn4 = MakeTxnHolder(configs[0], 400, {}, {"4->3", "4->1", "3->4"});
  auto txn4_4to3 = MakeTxnHolder(configs[0], 400, {}, {"4->3"});
  auto txn4_4to1 = MakeTxnHolder(configs[0], 400, {}, {"4->1"});
  auto txn4_3to4 = MakeTxnHolder(configs[0], 400, {}, {"3->4"});
  auto txn5 = MakeTxnHolder(configs[0], 500, {}, {"5->6", "7->5"});
  auto txn5_5to6 = MakeTxnHolder(configs[0], 500, {}, {"5->6"});
  auto txn5_7to5 = MakeTxnHolder(configs[0], 500, {}, {"7->5"});
  auto txn6 = MakeTxnHolder(configs[0], 600, {}, {"6->7", "5->6"});
  auto txn6_6to7 = MakeTxnHolder(configs[0], 600, {}, {"6->7"});
  auto txn6_5to6 = MakeTxnHolder(configs[0], 600, {}, {"5->6"});
  auto txn7 = MakeTxnHolder(configs[0], 700, {}, {"7->5", "6->7"});
  auto txn7_7to5 = MakeTxnHolder(configs[0], 700, {}, {"7->5"});
  auto txn7_6to7 = MakeTxnHolder(configs[0], 700, {}, {"6->7"});
  auto txn8 = MakeTxnHolder(configs[0], 800, {}, {"8->9", "9->8"});
  auto txn8_8to9 = MakeTxnHolder(configs[0], 800, {}, {"8->9"});
  auto txn8_9to8 = MakeTxnHolder(configs[0], 800, {}, {"9->8"});
  auto txn9 = MakeTxnHolder(configs[0], 900, {}, {"9->8", "8->9"});
  auto txn9_9to8 = MakeTxnHolder(configs[0], 900, {}, {"9->8"});
  auto txn9_8to9 = MakeTxnHolder(configs[0], 900, {}, {"8->9"});
  
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn1));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn2));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn3));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn4));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn5));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn6));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn7));

  ASSERT_EQ(lock_manager.AcquireLocks(txn1_1to2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_1to2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_2to1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_2to1), AcquireLocksResult::WAITING);

  lock_manager.ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver());

  ASSERT_EQ(lock_manager.AcquireLocks(txn3_3to4), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn4_3to4), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn4_4to3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn3_4to3), AcquireLocksResult::WAITING);

  lock_manager.ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver());

  ASSERT_EQ(lock_manager.AcquireLocks(txn4_4to1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn1_4to1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn2_2to3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn3_2to3), AcquireLocksResult::WAITING);

  ASSERT_EQ(lock_manager.AcquireLocks(txn5_5to6), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn6_5to6), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn6_6to7), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn7_6to7), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn7_7to5), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn5_7to5), AcquireLocksResult::WAITING);

  // The two deadlock components become stable and can be resolved
  lock_manager.ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver());

  ASSERT_EQ(lock_manager.AcquireLocks(txn8_8to9), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn9_8to9), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn9_9to8), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(txn8_9to8), AcquireLocksResult::WAITING);

  lock_manager.ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver());

  ASSERT_FALSE(lock_manager.AcceptTransaction(txn8));
  ASSERT_FALSE(lock_manager.AcceptTransaction(txn9));

  // The new deadlock component becomes stable and can be resolved. The new ready
  // txns are appended to the ready txn list
  lock_manager.ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver());

  auto ready_txns = lock_manager.GetReadyTxns();
  ASSERT_THAT(ready_txns, UnorderedElementsAre(100, 500, 800));

  auto result = lock_manager.ReleaseLocks(txn1);
  ASSERT_THAT(result, ElementsAre(200));
  result = lock_manager.ReleaseLocks(txn2);
  ASSERT_THAT(result, ElementsAre(300));
  result = lock_manager.ReleaseLocks(txn3);
  ASSERT_THAT(result, ElementsAre(400));
  result = lock_manager.ReleaseLocks(txn4);
  ASSERT_TRUE(result.empty());

  result = lock_manager.ReleaseLocks(txn5);
  ASSERT_THAT(result, ElementsAre(600));
  result = lock_manager.ReleaseLocks(txn6);
  ASSERT_THAT(result, ElementsAre(700));
  result = lock_manager.ReleaseLocks(txn7);
  ASSERT_TRUE(result.empty());

  result = lock_manager.ReleaseLocks(txn8);
  ASSERT_THAT(result, ElementsAre(900));
  result = lock_manager.ReleaseLocks(txn9);
  ASSERT_TRUE(result.empty());
}