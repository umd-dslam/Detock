#include "module/scheduler_components/ddr_lock_manager.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <deque>
#include <numeric>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;
using testing::ElementsAre;
using testing::UnorderedElementsAre;

TEST(DDRLockManagerTest, GetAllLocksOnFirstTry) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1, 1);
  auto holder = MakeTestTxnHolder(
      configs[0], 100, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}, {"writeC", KeyType::WRITE, 0}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  auto result = lock_manager.ReleaseLocks(holder.txn_id());
  ASSERT_TRUE(result.empty());
}

TEST(DDRLockManagerTest, ReadLocks) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"readA", KeyType::READ, 0}, {"readB", KeyType::READ, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readB", KeyType::READ, 0}, {"readC", KeyType::READ, 0}});
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1.txn_id()).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.txn_id()).empty());
}

TEST(DDRLockManagerTest, WriteLocks) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"writeA", KeyType::WRITE, 0}, {"writeB", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", KeyType::READ, 0}, {"writeA", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  // The blocked txn becomes ready
  ASSERT_EQ(lock_manager.ReleaseLocks(holder1.txn_id()).size(), 1U);
  // Make sure the lock is already held by holder2
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
}

TEST(DDRLockManagerTest, ReleaseLocksAndReturnMultipleNewLockHolders) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::WRITE, 0}});
  auto holder4 = MakeTestTxnHolder(configs[0], 400, {{"C", KeyType::READ, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder4.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_EQ(result.size(), 2U);
  ASSERT_THAT(result, UnorderedElementsAre(make_pair(200, false), make_pair(400, false)));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder4.txn_id()).empty());

  result = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(300, false)));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3.txn_id()).empty());
}

TEST(DDRLockManagerTest, PartiallyAcquiredLocks) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"A", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(200, false)));

  result = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(300, false)));
}

TEST(DDRLockManagerTest, AcquireLocksWithLockOnly1) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 2, 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);

  auto result = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(100, false)));
}

TEST(DDRLockManagerTest, AcquireLocksWithLockOnly2) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 2, 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(200, false)));
}

TEST(DDRLockManagerTest, MultiEdgeBetweenTwoTxns) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 3, 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::WRITE, 1}, {"B", KeyType::WRITE, 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"A", KeyType::READ, 1}, {"B", KeyType::READ, 2}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(2)), AcquireLocksResult::WAITING);

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(200, false)));

  result = lock_manager.ReleaseLocks(holder2.txn_id());
  ASSERT_TRUE(result.empty());
}

TEST(DDRLockManagerTest, KeyRegionLocks) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 3, 1, 1);
  auto holder1 = MakeTestTxnHolder(configs[0], 100, {{"writeA", KeyType::WRITE, 2}, {"writeB", KeyType::WRITE, 2}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"readA", KeyType::READ, 1}, {"writeA", KeyType::WRITE, 1}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
}

#ifdef REMASTER_PROTOCOL_COUNTERLESS
TEST(DDRLockManagerTest, RemasterTxn) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 3, 1, 1);
  auto holder = MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::WRITE, 2}}, {}, 1 /* new_master */);

  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(2)), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(holder.txn_id());

  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(2)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_manager.AcquireLocks(holder.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
  lock_manager.ReleaseLocks(holder.txn_id());
}
#endif

TEST(DDRLockManagerTest, EnsureStateIsClean) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1, 1);
  auto holder1 =
      MakeTestTxnHolder(configs[0], 100, {{"A", KeyType::READ, 0}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 200, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 0}});
  auto holder3 = MakeTestTxnHolder(configs[0], 300, {{"C", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_manager.AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder1.txn_id()).empty());

  ASSERT_EQ(lock_manager.AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_manager.AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.txn_id()).empty());
  ASSERT_TRUE(lock_manager.ReleaseLocks(holder3.txn_id()).empty());
}

TEST(DDRLockManagerTest, LongChain) {
  DDRLockManager lock_manager;
  auto configs = MakeTestConfigurations("locking", 1, 1, 1);
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

  auto result = lock_manager.ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, UnorderedElementsAre(make_pair(200, false), make_pair(300, false)));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder2.txn_id()).empty());
  result = lock_manager.ReleaseLocks(holder3.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(400, false)));

  result = lock_manager.ReleaseLocks(holder4.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(500, false)));

  ASSERT_TRUE(lock_manager.ReleaseLocks(holder5.txn_id()).empty());
}

class DDRLockManagerWithResolverTest : public ::testing::Test {
  std::vector<std::shared_ptr<Broker>> brokers_;
  std::vector<zmq::socket_t> signal_sockets_;

 protected:
  std::deque<DDRLockManager> lock_managers;

  slog::ConfigVec Initialize(int num_regions, int num_partitions, int ddr_interval = 0) {
    internal::Configuration add_on;
    add_on.set_ddr_interval(ddr_interval);
    auto configs = MakeTestConfigurations("locking", num_regions, 1, num_partitions, add_on);

    for (auto config : configs) {
      auto broker = brokers_.emplace_back(Broker::New(config, kTestModuleTimeout));
      auto& signal_socket = signal_sockets_.emplace_back(*broker->context(), ZMQ_PULL);
      signal_socket.bind(MakeInProcChannelAddress(kSchedulerChannel));
      auto& lm = lock_managers.emplace_back();
      // When ddr_interval = 0, we want to manually control when the deadlock resolver
      // runs, so we set poll_timeout to empty so that we can synchronously wait for
      // remote message.
      optional<std::chrono::milliseconds> poll_timeout;
      if (ddr_interval > 0) {
        poll_timeout = kTestModuleTimeout;
      }
      lm.InitializeDeadlockResolver(broker, nullptr, kSchedulerChannel, poll_timeout);
    }

    return configs;
  }

  void StartBrokers() {
    for (auto& broker : brokers_) {
      broker->StartInNewThreads();
    }
  }

  bool HasSignalFromResolver(int i, bool dont_wait = true) {
    auto env = RecvEnvelope(signal_sockets_[i], dont_wait);
    return env != nullptr;
  }
};

TEST_F(DDRLockManagerWithResolverTest, SimpleDeadlock) {
  auto configs = Initialize(2, 1);

  auto holder1 = MakeTestTxnHolder(configs[0], 1000, {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  auto holder2 = MakeTestTxnHolder(configs[0], 2000, {{"B", KeyType::WRITE, 1}, {"A", KeyType::WRITE, 0}});

  ASSERT_EQ(lock_managers[0].AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);

  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_TRUE(HasSignalFromResolver(0));

  auto ready_txns = lock_managers[0].GetReadyTxns();
  ASSERT_THAT(ready_txns, ElementsAre(1000));

  auto result = lock_managers[0].ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(2000, true)));

  ASSERT_TRUE(lock_managers[0].ReleaseLocks(holder2.txn_id()).empty());
}

TEST_F(DDRLockManagerWithResolverTest, SimplePartitionedDeadlock) {
  auto configs = Initialize(2, 2);

  StartBrokers();

  // Partition 0
  auto holder1_0 = MakeTestTxnHolder(configs[0], 1000, {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  auto holder2_0 = MakeTestTxnHolder(configs[0], 2000, {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  // Lock queues on this partition:
  // A: 1000 2000
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder1_0.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder2_0.lock_only_txn(0)), AcquireLocksResult::WAITING);

  // Partition 0 does not see any deadlock yet
  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_FALSE(HasSignalFromResolver(0));

  // Partition 1
  auto holder1_1 = MakeTestTxnHolder(configs[1], 1000, {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  auto holder2_1 = MakeTestTxnHolder(configs[1], 2000, {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  // Lock queues on this partition:
  // B: 2000 1000
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder2_1.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder1_1.lock_only_txn(1)), AcquireLocksResult::WAITING);

  // Partition 1 sees the deadlock since the partition 0 has already sent its view in above call
  // to ResolveDeadlock
  lock_managers[1].ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver(1));
  {
    auto ready_txns = lock_managers[1].GetReadyTxns();
    ASSERT_THAT(ready_txns, ElementsAre(1000));

    auto result = lock_managers[1].ReleaseLocks(1000);
    ASSERT_THAT(result, ElementsAre(make_pair(2000, true)));

    ASSERT_TRUE(lock_managers[1].ReleaseLocks(2000).empty());
  }

  // Partition 0 now sees the deadlock
  lock_managers[0].ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver(0));
  {
    auto ready_txns = lock_managers[0].GetReadyTxns();
    ASSERT_THAT(ready_txns, ElementsAre(1000));

    auto result = lock_managers[0].ReleaseLocks(1000);
    ASSERT_THAT(result, ElementsAre(make_pair(2000, true)));

    ASSERT_TRUE(lock_managers[0].ReleaseLocks(2000).empty());
  }
}

TEST_F(DDRLockManagerWithResolverTest, IdempotentDeadlockSignal) {
  auto configs = Initialize(2, 2);

  StartBrokers();

  // Partition 0
  auto holder1_0 = MakeTestTxnHolder(configs[0], 1000, {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  auto holder2_0 = MakeTestTxnHolder(configs[0], 2000, {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  // Lock queues on this partition:
  // A: 1000 2000
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder1_0.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder2_0.lock_only_txn(0)), AcquireLocksResult::WAITING);

  // Partition 1
  auto holder1_1 = MakeTestTxnHolder(configs[1], 1000, {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  auto holder2_1 = MakeTestTxnHolder(configs[1], 2000, {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  // Lock queues on this partition:
  // B: 2000 1000
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder2_1.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder1_1.lock_only_txn(1)), AcquireLocksResult::WAITING);

  // Let partition 1 send its part to partition 0
  lock_managers[1].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_FALSE(HasSignalFromResolver(0));

  // Partition 0 resolves the deadlock and send out signal
  lock_managers[0].ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver(0));
  ASSERT_THAT(lock_managers[0].GetReadyTxns(), ElementsAre(1000));

  // Partition 0 is called to resolve deadlock again but there shouldn't be other deadlock
  lock_managers[0].ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver(0));
  ASSERT_TRUE(lock_managers[0].GetReadyTxns().empty());
}

TEST_F(DDRLockManagerWithResolverTest, UnstableDeadlock) {
  auto configs = Initialize(2, 1);

  auto holder1 = MakeTestTxnHolder(configs[0], 1000,
                                   {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}, {"C", KeyType::WRITE, 0}});
  auto holder2 = MakeTestTxnHolder(configs[0], 2000, {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 1}});
  auto holder3 = MakeTestTxnHolder(configs[0], 3000, {{"C", KeyType::READ, 0}, {"D", KeyType::WRITE, 1}});
  auto holder4 = MakeTestTxnHolder(configs[0], 4000, {{"A", KeyType::WRITE, 1}});

  // Txn1 and Txn2 forms a deadlock. They both depend on Txn3, which is still incomplete
  // A: 2000 1000 4000
  // B: 1000 2000
  // C: 3000 1000
  // D:
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder4.lock_only_txn(1)), AcquireLocksResult::WAITING);
  // No deadlock is resolved because the component is not stable due to its dependency on the incomplete txn
  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_FALSE(HasSignalFromResolver(0));

  // All txns are complete after this point
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder3.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);

  // The deadlock should be resolved after this but its head txn is not yet immediately ready
  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_FALSE(HasSignalFromResolver(0));
  ASSERT_TRUE(lock_managers[0].GetReadyTxns().empty());

  // Release txn3 so the next one from the deadlock is ready
  ASSERT_THAT(lock_managers[0].ReleaseLocks(3000), ElementsAre(make_pair(1000, true)));
  ASSERT_THAT(lock_managers[0].ReleaseLocks(1000), ElementsAre(make_pair(2000, true), make_pair(4000, false)));
  ASSERT_TRUE(lock_managers[0].ReleaseLocks(2000).empty());
  ASSERT_TRUE(lock_managers[0].ReleaseLocks(4000).empty());
}

TEST_F(DDRLockManagerWithResolverTest, UnstablePartitionedDeadlock) {
  // Partition 0: A, C, E
  // Partition 1: B
  auto configs = Initialize(2, 2);

  StartBrokers();

  // Partition 0
  auto holder1_0 = MakeTestTxnHolder(configs[0], 1000,
                                     {{"A", KeyType::WRITE, 1}, {"C", KeyType::WRITE, 0}, {"E", KeyType::WRITE, 0}});
  auto holder2_0 = MakeTestTxnHolder(configs[0], 2000,
                                     {{"A", KeyType::WRITE, 1}, {"B", KeyType::WRITE, 1}, {"E", KeyType::WRITE, 0}});
  auto holder3_0 = MakeTestTxnHolder(configs[0], 3000, {{"C", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});

  // Txn1 and Txn2 forms a deadlock. Their component depends on Txn3, which cannot be seen as a whole
  // from the current partition
  // Lock queues on partition 0:
  // A: 2000 1000
  // C: 3000 1000
  // E: 1000 2000
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder3_0.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder1_0.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder2_0.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder2_0.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder1_0.lock_only_txn(1)), AcquireLocksResult::WAITING);

  // Partition 1
  auto holder2_1 = MakeTestTxnHolder(configs[1], 2000,
                                     {{"A", KeyType::WRITE, 1}, {"B", KeyType::WRITE, 1}, {"E", KeyType::WRITE, 0}});
  auto holder3_1 = MakeTestTxnHolder(configs[1], 3000, {{"C", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}});
  // Lock queues on partition 1:
  // B: 3000 2000
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder3_1.lock_only_txn(1)), AcquireLocksResult::ACQUIRED);
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder2_1.lock_only_txn(1)), AcquireLocksResult::WAITING);

  // No deadlock is resolved on partition because the component is not stable due to its dependency on
  // the incomplete txn
  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_FALSE(HasSignalFromResolver(0));

  // Partition 1 sees the deadlock but the deadlock is irrelevant to this partition
  lock_managers[1].ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver(1));

  // Partition 0 can see the deadlock now
  lock_managers[0].ResolveDeadlock();
  ASSERT_FALSE(HasSignalFromResolver(0));

  // On partition 0, release txn3 so the next one from the deadlock is ready
  auto result = lock_managers[0].ReleaseLocks(3000);
  ASSERT_THAT(result, ElementsAre(make_pair(1000, true)));
  result = lock_managers[0].ReleaseLocks(1000);
  ASSERT_THAT(result, ElementsAre(make_pair(2000, true)));

  ASSERT_TRUE(lock_managers[0].ReleaseLocks(2000).empty());

  // On partition 1
  result = lock_managers[1].ReleaseLocks(3000);
  ASSERT_THAT(result, ElementsAre(make_pair(2000, true)));

  ASSERT_TRUE(lock_managers[0].ReleaseLocks(2000).empty());
}

TEST_F(DDRLockManagerWithResolverTest, PartitionedDeadlockWithSinglePartitionVertex) {
  auto configs = Initialize(2, 2);

  StartBrokers();

  // Partition 0
  auto holder1_0 = MakeTestTxnHolder(configs[0], 1000,
                                     {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}, {"D", KeyType::WRITE, 1}});
  // Lock queues on this partition:
  // A: 1000
  ASSERT_EQ(lock_managers[0].AcquireLocks(holder1_0.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);

  // Partition 0 does not see any deadlock yet
  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_FALSE(HasSignalFromResolver(0));

  // Partition 1
  auto holder1_1 = MakeTestTxnHolder(configs[1], 1000,
                                     {{"A", KeyType::WRITE, 0}, {"B", KeyType::WRITE, 1}, {"D", KeyType::WRITE, 1}});
  auto holder2 = MakeTestTxnHolder(configs[1], 2000, {{"B", KeyType::WRITE, 0}, {"D", KeyType::WRITE, 1}});
  auto holder3 = MakeTestTxnHolder(configs[1], 3000, {{"B", KeyType::WRITE, 0}, {"D", KeyType::WRITE, 1}});
  // Lock queues on this partition:
  // B: 2000 3000
  // D: 3000 1000 2000
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder3.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder1_1.lock_only_txn(1)), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[1].AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);

  // Partition 1 has a deadlock
  lock_managers[1].ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver(1));

  ASSERT_THAT(lock_managers[1].GetReadyTxns(), ElementsAre(1000));
  ASSERT_THAT(lock_managers[1].ReleaseLocks(1000), ElementsAre(make_pair(2000, true)));
  ASSERT_THAT(lock_managers[1].ReleaseLocks(2000), ElementsAre(make_pair(3000, true)));
  ASSERT_TRUE(lock_managers[1].ReleaseLocks(3000).empty());

  // Partition 0 now sees the deadlock
  lock_managers[0].ResolveDeadlock();
  ASSERT_TRUE(HasSignalFromResolver(0));
  ASSERT_THAT(lock_managers[0].GetReadyTxns(), ElementsAre(1000));
  ASSERT_TRUE(lock_managers[0].ReleaseLocks(1000).empty());
}

TEST_F(DDRLockManagerWithResolverTest, ConcurrentResolver) {
  auto configs = Initialize(2, 1, 1);

  StartBrokers();

  // This resolver runs in a different thread so this test might or might not run into
  // a faulty execution. The interval is set to a low number in hope that it is more
  // probable to expose a bug, if any.
  lock_managers[0].StartDeadlockResolver();

  // Loop multiple times to increase the chance of bugs showing
  for (int i = 0; i < 20; i++) {
    // Change txn ids at every loop so that they are all uniques
    std::array<TxnId, 7> ids;
    std::iota(ids.begin(), ids.end(), (i + 1) * 1000);
    auto configs = MakeTestConfigurations("locking", 2, 1, 1);
    auto holder1 = MakeTestTxnHolder(configs[0], ids[1], {{"A", KeyType::READ, 1}, {"B", KeyType::WRITE, 0}});
    auto holder2 = MakeTestTxnHolder(configs[0], ids[2], {{"B", KeyType::READ, 0}, {"A", KeyType::WRITE, 1}});
    auto holder3 = MakeTestTxnHolder(configs[0], ids[3], {{"B", KeyType::READ, 0}});
    auto holder4 = MakeTestTxnHolder(configs[0], ids[4], {{"A", KeyType::READ, 1}});
    auto holder5 = MakeTestTxnHolder(configs[0], ids[5], {{"B", KeyType::READ, 0}});
    auto holder6 = MakeTestTxnHolder(configs[0], ids[6], {{"A", KeyType::READ, 1}});

    // Txn1 and Txn2 forms a deadlock. Other txns have to wait until this deadlock is resolved
    ASSERT_EQ(lock_managers[0].AcquireLocks(holder3.lock_only_txn(0)), AcquireLocksResult::ACQUIRED);
    ASSERT_EQ(lock_managers[0].AcquireLocks(holder1.lock_only_txn(0)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_managers[0].AcquireLocks(holder2.lock_only_txn(0)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_managers[0].AcquireLocks(holder2.lock_only_txn(1)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_managers[0].AcquireLocks(holder1.lock_only_txn(1)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_managers[0].AcquireLocks(holder4.lock_only_txn(1)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_managers[0].AcquireLocks(holder5.lock_only_txn(0)), AcquireLocksResult::WAITING);
    ASSERT_EQ(lock_managers[0].AcquireLocks(holder6.lock_only_txn(1)), AcquireLocksResult::WAITING);

    this_thread::sleep_for((i % 2) * 1ms);
    auto result = lock_managers[0].ReleaseLocks(holder3.txn_id());
    if (!result.empty()) {
      LOG(INFO) << "Deadlock resolved before releasing txn3";
      // This case happens when the resolver resolves the deadlock before txn3 releasing locks
      ASSERT_THAT(result, ElementsAre(make_pair(ids[1], true)));
    } else {
      LOG(INFO) << "Deadlock resolved after releasing txn3";
      // This case happens when the resolver resolves the deadlock after the txn3 releasing locks
      ASSERT_TRUE(HasSignalFromResolver(0, false /* dont_wait */));
      auto ready_txns = lock_managers[0].GetReadyTxns();
      ASSERT_THAT(ready_txns, ElementsAre(ids[1]));
    }

    result = lock_managers[0].ReleaseLocks(holder1.txn_id());
    ASSERT_THAT(result, UnorderedElementsAre(make_pair(ids[2], true), make_pair(ids[5], false)));
    ASSERT_TRUE(lock_managers[0].ReleaseLocks(holder5.txn_id()).empty());

    result = lock_managers[0].ReleaseLocks(holder2.txn_id());
    ASSERT_THAT(result, UnorderedElementsAre(make_pair(ids[4], false), make_pair(ids[6], false)));
    ASSERT_TRUE(lock_managers[0].ReleaseLocks(holder4.txn_id()).empty());
    ASSERT_TRUE(lock_managers[0].ReleaseLocks(holder6.txn_id()).empty());
  }
}

TEST_F(DDRLockManagerWithResolverTest, MultipleDeadlocks) {
  auto configs = Initialize(3, 1);
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

  ASSERT_EQ(lock_managers[0].AcquireLocks(txn1_1to2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn2_1to2), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn2_2to1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn1_2to1), AcquireLocksResult::WAITING);

  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_FALSE(HasSignalFromResolver(0));

  ASSERT_EQ(lock_managers[0].AcquireLocks(txn3_3to4), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn4_3to4), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn4_4to3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn3_4to3), AcquireLocksResult::WAITING);

  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_FALSE(HasSignalFromResolver(0));

  ASSERT_EQ(lock_managers[0].AcquireLocks(txn4_4to1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn1_4to1), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn2_2to3), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn3_2to3), AcquireLocksResult::WAITING);

  ASSERT_EQ(lock_managers[0].AcquireLocks(txn5_5to6), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn6_5to6), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn6_6to7), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn7_6to7), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn7_7to5), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn5_7to5), AcquireLocksResult::WAITING);

  // The two deadlock components become stable and can be resolved
  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_TRUE(HasSignalFromResolver(0));

  ASSERT_EQ(lock_managers[0].AcquireLocks(txn8_8to9), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn9_8to9), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn9_9to8), AcquireLocksResult::WAITING);
  ASSERT_EQ(lock_managers[0].AcquireLocks(txn8_9to8), AcquireLocksResult::WAITING);

  // The new deadlock component becomes stable and can be resolved. The new ready
  // txns are appended to the ready txn list
  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_TRUE(HasSignalFromResolver(0));

  auto ready_txns = lock_managers[0].GetReadyTxns();
  ASSERT_THAT(ready_txns, UnorderedElementsAre(1000, 5000, 8000));

  auto result = lock_managers[0].ReleaseLocks(holder1.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(2000, true)));
  result = lock_managers[0].ReleaseLocks(holder2.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(3000, true)));
  result = lock_managers[0].ReleaseLocks(holder3.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(4000, true)));
  result = lock_managers[0].ReleaseLocks(holder4.txn_id());
  ASSERT_TRUE(result.empty());

  result = lock_managers[0].ReleaseLocks(holder5.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(6000, true)));
  result = lock_managers[0].ReleaseLocks(holder6.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(7000, true)));
  result = lock_managers[0].ReleaseLocks(holder7.txn_id());
  ASSERT_TRUE(result.empty());

  result = lock_managers[0].ReleaseLocks(holder8.txn_id());
  ASSERT_THAT(result, ElementsAre(make_pair(9000, true)));
  result = lock_managers[0].ReleaseLocks(holder9.txn_id());
  ASSERT_TRUE(result.empty());

  // The graph should be empty at this point
  lock_managers[0].ResolveDeadlock(true /* dont_recv_remote_msg */);
  ASSERT_FALSE(HasSignalFromResolver(0));
}