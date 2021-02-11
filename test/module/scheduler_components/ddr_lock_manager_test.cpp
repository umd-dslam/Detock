#include "module/scheduler_components/ddr_lock_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <numeric>

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
    auto env = RecvEnvelope(socket, true /* dont_wait */);
    return env != nullptr;
  }
};

TEST_F(DDRLockManagerTest, GetAllLocksOnFirstTry) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder = MakeTestTxnHolder(
      configs[0], 100, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}, {"writeC", KeyType::WRITE, 0}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder.id());
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, ReadLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readB", KeyType::READ, 0}, {"readC", KeyType::READ, 0}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1.id()).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.id()).empty());
}

TEST_F(DDRLockManagerTest, WriteLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"writeA", KeyType::WRITE, 0}, {"writeB", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", KeyType::READ, 0}, {"writeA", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1.id()).size(), 1U);
  // Make sure the lock is already held by holder2
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
}

TEST_F(DDRLockManagerTest, ReleaseLocksAndReturnMultipleNewLockHolders) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::WRITE, 0}});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {{"C", KeyType::READ, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.id());
  ASSERT_EQ(result.size(), 2U);
  ASSERT_THAT(result, UnorderedElementsAre(200, 400));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder4.id()).empty());

  result = lock_manager.ReleaseLocks(holder2.id());
  ASSERT_THAT(result, ElementsAre(300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3.id()).empty());
}

TEST_F(DDRLockManagerTest, PartiallyAcquiredLocks) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.id());
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(holder2.id());
  ASSERT_THAT(result, ElementsAre(300));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnly1) {
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(holder2.id());
  ASSERT_THAT(result, ElementsAre(100));
}

TEST_F(DDRLockManagerTest, AcquireLocksWithLockOnly2) {
  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.id());
  ASSERT_THAT(result, ElementsAre(200));
}

TEST_F(DDRLockManagerTest, MultiEdgeBetweenTwoTxns) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::WRITE, 1}, {"B", KeyType::WRITE, 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 1}, {"B", KeyType::READ, 2}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(2)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.id());
  ASSERT_THAT(result, ElementsAre(200));

  result = lock_manager.ReleaseLocks(holder2.id());
  ASSERT_TRUE(result.empty());
}

TEST_F(DDRLockManagerTest, KeyReplicaLocks) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"writeA", KeyType::WRITE, 2}, {"writeB", KeyType::WRITE, 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", KeyType::READ, 1}, {"writeA", KeyType::WRITE, 1}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
}

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST_F(DDRLockManagerTest, RemasterTxn) {
  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::WRITE, 2}}, 1 /* new_master */);

  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(holder.id());

  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(2)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(holder.id());
}
#endif

TEST_F(DDRLockManagerTest, EnsureStateIsClean) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"C", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1.id()).empty());

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.id()).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3.id()).empty());
}

TEST_F(DDRLockManagerTest, LongChain) {
  auto configs = MakeTestConfigurations("locking", 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::READ, 0}});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {{"A", KeyType::WRITE, 0}});
  auto holder5 = MakeTestTxnHolder(configs[0], 500, {{"A", KeyType::READ, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder5.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.id());
  ASSERT_THAT(result, UnorderedElementsAre(200, 300));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.id()).empty());
  result = lock_manager.ReleaseLocks(holder3.id());
  ASSERT_THAT(result, ElementsAre(400));

  result = lock_manager.ReleaseLocks(holder4.id());
  ASSERT_THAT(result, ElementsAre(500));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder5.id()).empty());
}

TEST_F(DDRLockManagerTest, SimpleDeadlock) {
  lock_manager.StartDeadlockResolver(context, 0, 1ms, true /* init_only */);

  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 1000, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 1}});
  auto holder2 = MakeTestTxnHolder(configs[0], 2000, {{"B", KeyType::READ, 1}, {"A", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);

  lock_manager.ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver());

  auto ready_txns = lock_manager.GetReadyTxns();
  ASSERT_THAT(ready_txns, ElementsAre(1000));

  auto result = lock_manager.ReleaseLocks(holder1.id());
  ASSERT_THAT(result, ElementsAre(2000));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.id()).empty());
}

TEST_F(DDRLockManagerTest, IncompleteDeadlock) {
  lock_manager.StartDeadlockResolver(context, 0, 1ms, true /* init_only */);

  auto configs = MakeTestConfigurations("locking", 3, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 1000,
                                   {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 2000, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 1}});

  // A lock-only txn is missing for txn1
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);

  // There is already a deadlock but txn 1000 is still incomplete
  lock_manager.ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver());

  // All txns are complete after this point
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::WAITING);

  // Now deadlock can be resolved
  lock_manager.ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver());

  auto ready_txns = lock_manager.GetReadyTxns();
  ASSERT_THAT(ready_txns, ElementsAre(1000));

  auto result = lock_manager.ReleaseLocks(holder1.id());
  ASSERT_THAT(result, ElementsAre(2000));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.id()).empty());
}

TEST_F(DDRLockManagerTest, CompleteButUnstableDeadlock) {
  lock_manager.StartDeadlockResolver(context, 0, 1ms, true /* init_only */);

  auto configs = MakeTestConfigurations("locking", 2, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 1000,
                                   {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 2000, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 1}});
  auto holder3 = MakeTestTxnHolder(configs[0], 3000, {{"C", KeyType::READ, 0}, {"D", KeyType::WRITE, 1}});

  // Txn1 and Txn2 forms a deadlock. Their component depends on Txn3, which is still
  // incomplete
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);

  // No deadlock is resolved because the component is not stable due to its dependency on
  // the incomplete txn
  lock_manager.ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver());

  // All txns are complete after this point
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);

  // The deadlock should be resolved after this but its head txn is not yet immediately ready
  lock_manager.ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver());
  ASSERT_TRUE(lock_manager.GetReadyTxns().empty());

  // Release txn3 so the next one from the deadlock is ready
  auto result = lock_manager.ReleaseLocks(holder3.id());
  ASSERT_THAT(result, ElementsAre(1000));

  result = lock_manager.ReleaseLocks(holder1.id());
  ASSERT_THAT(result, ElementsAre(2000));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.id()).empty());
}

TEST_F(DDRLockManagerTest, ConcurrentResolver) {
  // This resolver runs in a different thread so this test might or might not run into
  // a faulty execution. The interval is set to a low number in hope that it is more
  // probable to expose a bug, if any.
  lock_manager.StartDeadlockResolver(context, 0, 0ms);
  // Loop multiple times to increase the chance of bugs showing
  for (int i = 0; i < 50; i++) {
    // Change txn ids at every loop so that they are all uniques
    std::array<TxnId, 7> ids;
    std::iota(ids.begin(), ids.end(), (i + 1) * 1000);
    auto configs = MakeTestConfigurations("locking", 2, 1);
    auto holder1 = MakeTestTxnHolder(configs[0], ids[1], {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}});
    auto holder2 = MakeTestTxnHolder(configs[0], ids[2], {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 1}});
    auto holder3 = MakeTestTxnHolder(configs[0], ids[3], {{"B", KeyType::READ, 0}});
    auto holder4 = MakeTestTxnHolder(configs[0], ids[4], {{"A", KeyType::READ, 1}});
    auto holder5 = MakeTestTxnHolder(configs[0], ids[5], {{"B", KeyType::READ, 0}});
    auto holder6 = MakeTestTxnHolder(configs[0], ids[6], {{"A", KeyType::READ, 1}});

    // Txn1 and Txn2 forms a deadlock. Other txns have to wait until this deadlock is resolved
    ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
    ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(1)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_manager.AcquireLocks(holder5.lock_only_txn(0)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_manager.AcquireLocks(holder6.lock_only_txn(1)), AcquireLocksResult::WAITING);

    auto result = lock_manager.ReleaseLocks(holder3.id());
    if (!result.empty()) {
      LOG(INFO) << "Deadlock resolved before releasing txn3";
      // This case happens when the resolver resolves the deadlock before txn3 releasing locks
      ASSERT_THAT(result, ElementsAre(ids[1]));
    } else {
      LOG(INFO) << "Deadlock resolved after releasing txn3";
      // This case happens when the resolver resolves the deadlock after the txn3 releasing locks
      RecvEnvelope(socket);
      auto ready_txns = lock_manager.GetReadyTxns();
      ASSERT_THAT(ready_txns, ElementsAre(ids[1]));
    }

    result = lock_manager.ReleaseLocks(holder1.id());
    ASSERT_THAT(result, UnorderedElementsAre(ids[2], ids[5]));
    ASSERT_TRUE(lock_manager.ReleaseLocks(holder5.id()).empty());

    result = lock_manager.ReleaseLocks(holder2.id());
    ASSERT_THAT(result, UnorderedElementsAre(ids[4], ids[6]));
    ASSERT_TRUE(lock_manager.ReleaseLocks(holder4.id()).empty());
    ASSERT_TRUE(lock_manager.ReleaseLocks(holder6.id()).empty());
  }
}

TEST_F(DDRLockManagerTest, MultipleDeadlocks) {
  lock_manager.StartDeadlockResolver(context, 0, 1ms, true /* init_only */);

  auto configs = MakeTestConfigurations("locking", 3, 1);
  // The key names are the edges that will be formed from these txns
  auto holder1 = MakeTestTxnHolder(
      configs[0], 1000, {{"1->2", KeyType::WRITE, 0}, {"2->1", KeyType::WRITE, 1}, {"4->1", KeyType::WRITE, 2}});
  auto& txn1_1to2 = holder1.lock_only_txn(0);
  auto& txn1_2to1 = holder1.lock_only_txn(1);
  auto& txn1_4to1 = holder1.lock_only_txn(2);
  auto holder2 = MakeTestTxnHolder(
      configs[0], 2000, {{"2->1", KeyType::WRITE, 1}, {"2->3", KeyType::WRITE, 2}, {"1->2", KeyType::WRITE, 0}});
  auto& txn2_1to2 = holder2.lock_only_txn(0);
  auto& txn2_2to1 = holder2.lock_only_txn(1);
  auto& txn2_2to3 = holder2.lock_only_txn(2);
  auto holder3 = MakeTestTxnHolder(
      configs[0], 3000, {{"3->4", KeyType::WRITE, 0}, {"4->3", KeyType::WRITE, 1}, {"2->3", KeyType::WRITE, 2}});
  auto& txn3_3to4 = holder3.lock_only_txn(0);
  auto& txn3_4to3 = holder3.lock_only_txn(1);
  auto& txn3_2to3 = holder3.lock_only_txn(2);
  auto holder4 = MakeTestTxnHolder(
      configs[0], 4000, {{"4->3", KeyType::WRITE, 1}, {"4->1", KeyType::WRITE, 2}, {"3->4", KeyType::WRITE, 0}});
  auto& txn4_3to4 = holder4.lock_only_txn(0);
  auto& txn4_4to3 = holder4.lock_only_txn(1);
  auto& txn4_4to1 = holder4.lock_only_txn(2);
  auto holder5 = MakeTestTxnHolder(configs[0], 5000, {{"5->6", KeyType::WRITE, 0}, {"7->5", KeyType::WRITE, 1}});
  auto& txn5_5to6 = holder5.lock_only_txn(0);
  auto& txn5_7to5 = holder5.lock_only_txn(1);
  auto holder6 = MakeTestTxnHolder(configs[0], 6000, {{"6->7", KeyType::WRITE, 2}, {"5->6", KeyType::WRITE, 0}});
  auto& txn6_5to6 = holder6.lock_only_txn(0);
  auto& txn6_6to7 = holder6.lock_only_txn(2);
  auto holder7 = MakeTestTxnHolder(configs[0], 7000, {{"7->5", KeyType::WRITE, 1}, {"6->7", KeyType::WRITE, 2}});
  auto& txn7_7to5 = holder7.lock_only_txn(1);
  auto& txn7_6to7 = holder7.lock_only_txn(2);
  auto holder8 = MakeTestTxnHolder(configs[0], 8000, {{"8->9", KeyType::WRITE, 0}, {"9->8", KeyType::WRITE, 1}});
  auto& txn8_8to9 = holder8.lock_only_txn(0);
  auto& txn8_9to8 = holder8.lock_only_txn(1);
  auto holder9 = MakeTestTxnHolder(configs[0], 9000, {{"9->8", KeyType::WRITE, 1}, {"8->9", KeyType::WRITE, 0}});
  auto& txn9_8to9 = holder9.lock_only_txn(0);
  auto& txn9_9to8 = holder9.lock_only_txn(1);

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

  // The new deadlock component becomes stable and can be resolved. The new ready
  // txns are appended to the ready txn list
  lock_manager.ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver());

  auto ready_txns = lock_manager.GetReadyTxns();
  ASSERT_THAT(ready_txns, UnorderedElementsAre(1000, 5000, 8000));

  auto result = lock_manager.ReleaseLocks(holder1.id());
  ASSERT_THAT(result, ElementsAre(2000));
  result = lock_manager.ReleaseLocks(holder2.id());
  ASSERT_THAT(result, ElementsAre(3000));
  result = lock_manager.ReleaseLocks(holder3.id());
  ASSERT_THAT(result, ElementsAre(4000));
  result = lock_manager.ReleaseLocks(holder4.id());
  ASSERT_TRUE(result.empty());

  result = lock_manager.ReleaseLocks(holder5.id());
  ASSERT_THAT(result, ElementsAre(6000));
  result = lock_manager.ReleaseLocks(holder6.id());
  ASSERT_THAT(result, ElementsAre(7000));
  result = lock_manager.ReleaseLocks(holder7.id());
  ASSERT_TRUE(result.empty());

  result = lock_manager.ReleaseLocks(holder8.id());
  ASSERT_THAT(result, ElementsAre(9000));
  result = lock_manager.ReleaseLocks(holder9.id());
  ASSERT_TRUE(result.empty());
}