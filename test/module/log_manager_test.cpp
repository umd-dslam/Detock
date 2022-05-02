#include "module/log_manager.h"

#include <gtest/gtest.h>

#include <vector>

#include "common/proto_utils.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

using internal::Envelope;

TEST(LocalLogTest, InOrder) {
  LocalLog log;
  log.AddBatchId(111 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  ASSERT_FALSE(log.HasNextBatch());

  log.AddSlot(0 /* slot_id */, 111 /* queue_id */, 0 /* leader */);
  ASSERT_EQ(make_pair(0U, make_pair(100UL, 0U)), log.NextBatch());

  log.AddBatchId(222 /* queue_id */, 0 /* position */, 200 /* batch_id */);
  ASSERT_FALSE(log.HasNextBatch());

  log.AddSlot(1 /* slot_id */, 222 /* queue_id */, 1 /* leader */);
  ASSERT_EQ(make_pair(1U, make_pair(200UL, 1U)), log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST(LocalLogTest, BatchesComeFirst) {
  LocalLog log;

  log.AddBatchId(222 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  log.AddBatchId(111 /* queue_id */, 0 /* position */, 200 /* batch_id */);
  log.AddBatchId(333 /* queue_id */, 0 /* position */, 300 /* batch_id */);
  log.AddBatchId(333 /* queue_id */, 1 /* position */, 400 /* batch_id */);

  log.AddSlot(0 /* slot_id */, 111 /* queue_id */, 0 /* leader */);
  ASSERT_EQ(make_pair(0U, make_pair(200UL, 0U)), log.NextBatch());

  log.AddSlot(1 /* slot_id */, 333 /* queue_id */, 1 /* leader */);
  ASSERT_EQ(make_pair(1U, make_pair(300UL, 1U)), log.NextBatch());

  log.AddSlot(2 /* slot_id */, 222 /* queue_id */, 2 /* leader */);
  ASSERT_EQ(make_pair(2U, make_pair(100UL, 2U)), log.NextBatch());

  log.AddSlot(3 /* slot_id */, 333 /* queue_id */, 3 /* leader */);
  ASSERT_EQ(make_pair(3U, make_pair(400UL, 3U)), log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST(LocalLogTest, SlotsComeFirst) {
  LocalLog log;

  log.AddSlot(2 /* slot_id */, 222 /* queue_id */, 0 /* leader */);
  log.AddSlot(1 /* slot_id */, 333 /* queue_id */, 0 /* leader */);
  log.AddSlot(3 /* slot_id */, 333 /* queue_id */, 0 /* leader */);
  log.AddSlot(0 /* slot_id */, 111 /* queue_id */, 0 /* leader */);

  log.AddBatchId(111 /* queue_id */, 0 /* position */, 200 /* batch_id */);
  ASSERT_EQ(make_pair(0U, make_pair(200UL, 0U)), log.NextBatch());

  log.AddBatchId(333 /* queue_id */, 0 /* position */, 300 /* batch_id */);
  ASSERT_EQ(make_pair(1U, make_pair(300UL, 0U)), log.NextBatch());

  log.AddBatchId(222 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  ASSERT_EQ(make_pair(2U, make_pair(100UL, 0U)), log.NextBatch());

  log.AddBatchId(333 /* queue_id */, 1 /* position */, 400 /* batch_id */);
  ASSERT_EQ(make_pair(3U, make_pair(400UL, 0U)), log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST(LocalLogTest, MultipleNextBatches) {
  LocalLog log;

  log.AddBatchId(111 /* queue_id */, 0 /* position */, 300 /* batch_id */);
  log.AddBatchId(222 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  log.AddBatchId(333 /* queue_id */, 0 /* position */, 400 /* batch_id */);
  log.AddBatchId(333 /* queue_id */, 1 /* position */, 200 /* batch_id */);

  log.AddSlot(3 /* slot_id */, 333 /* queue_id */, 1 /* leader */);
  log.AddSlot(1 /* slot_id */, 333 /* queue_id */, 1 /* leader */);
  log.AddSlot(2 /* slot_id */, 111 /* queue_id */, 1 /* leader */);
  log.AddSlot(0 /* slot_id */, 222 /* queue_id */, 1 /* leader */);

  ASSERT_EQ(make_pair(0U, make_pair(100UL, 1U)), log.NextBatch());
  ASSERT_EQ(make_pair(1U, make_pair(400UL, 1U)), log.NextBatch());
  ASSERT_EQ(make_pair(2U, make_pair(300UL, 1U)), log.NextBatch());
  ASSERT_EQ(make_pair(3U, make_pair(200UL, 1U)), log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST(LocalLogTest, SameOriginOutOfOrder) {
  LocalLog log;

  log.AddBatchId(111 /* queue_id */, 1 /* position */, 200 /* batch_id */);
  log.AddBatchId(111 /* queue_id */, 2 /* position */, 300 /* batch_id */);

  log.AddSlot(0 /* slot_id */, 111 /* queue_id */, 0 /* leader */);
  ASSERT_FALSE(log.HasNextBatch());

  log.AddSlot(1 /* slot_id */, 111 /* queue_id */, 0 /* leader */);
  ASSERT_FALSE(log.HasNextBatch());

  log.AddBatchId(111 /* queue_id */, 0 /* position */, 100 /* batch_id */);

  log.AddSlot(2 /* slot_id */, 111 /* queue_id */, 0 /* leader */);
  ASSERT_TRUE(log.HasNextBatch());

  ASSERT_EQ(make_pair(0U, make_pair(100UL, 0U)), log.NextBatch());
  ASSERT_EQ(make_pair(1U, make_pair(200UL, 0U)), log.NextBatch());
  ASSERT_EQ(make_pair(2U, make_pair(300UL, 0U)), log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

const int NUM_REGIONS = 2;
const int NUM_PARTITIONS = 2;
constexpr int NUM_MACHINES = NUM_REGIONS * NUM_PARTITIONS;

class LogManagerTest : public ::testing::Test {
 public:
  void SetUp() {
    auto configs = MakeTestConfigurations("log_manager", NUM_REGIONS, 1, NUM_PARTITIONS);
    int counter = 0;
    for (int reg = 0; reg < NUM_REGIONS; reg++)
      for (int p = 0; p < NUM_PARTITIONS; p++) {
        MachineId id = MakeMachineId(reg, 0, p);
        auto res = slogs_.emplace(id, configs[counter++]);
        CHECK(res.second);
        auto& slog = res.first->second;
        slog.AddLogManagers();
        slog.AddOutputSocket(kSchedulerChannel);
        senders_.emplace(id, slog.NewSender());
        slog.StartInNewThreads();
      }
  }

  void SendToLogManager(MachineId from, MachineId to, const Envelope& req, int home) {
    auto it = senders_.find(from);
    CHECK(it != senders_.end());
    it->second->Send(req, to, kLogManagerChannel + home);
  }

  Transaction* ReceiveTxn(MachineId id) {
    auto it = slogs_.find(id);
    CHECK(it != slogs_.end());
    auto req_env = it->second.ReceiveFromOutputSocket(kSchedulerChannel);
    if (req_env == nullptr) {
      return nullptr;
    }
    if (req_env->request().type_case() != internal::Request::kForwardTxn) {
      return nullptr;
    }
    return req_env->mutable_request()->mutable_forward_txn()->release_txn();
  }

  unordered_map<MachineId, unique_ptr<Sender>> senders_;
  unordered_map<MachineId, TestSlog> slogs_;
};

internal::Batch* MakeBatch(BatchId batch_id, const vector<Transaction*>& txns, TransactionType batch_type) {
  internal::Batch* batch = new internal::Batch();
  batch->set_id(batch_id);
  batch->set_transaction_type(batch_type);
  for (auto txn : txns) {
    batch->mutable_transactions()->AddAllocated(txn);
  }
  return batch;
}

TEST_F(LogManagerTest, BatchDataBeforeBatchOrder) {
  auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
  auto expected_txn_2 = MakeTransaction({{"X"}, {"Y", KeyType::WRITE}});
  auto batch = MakeBatch(100, {expected_txn_1, expected_txn_2}, SINGLE_HOME);

  // Distribute batch to local replica
  {
    Envelope req;
    auto forward_batch_data = req.mutable_request()->mutable_forward_batch_data();
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->set_generator(0);
    forward_batch_data->set_generator_position(0);

    SendToLogManager(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), req, 0);
    SendToLogManager(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 1), req, 0);
  }
  // Distribute batch to remote region
  {
    Envelope req;
    auto forward_batch_data = req.mutable_request()->mutable_forward_batch_data();
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->set_generator(0);
    forward_batch_data->set_generator_position(0);

    SendToLogManager(MakeMachineId(0, 0, 0), MakeMachineId(1, 0, 0), req, 0);
  }

  // Then send local ordering
  {
    Envelope req;
    auto local_batch_order = req.mutable_request()->mutable_forward_batch_order()->mutable_local_batch_order();
    local_batch_order->set_generator(0);
    local_batch_order->set_slot(0);
    local_batch_order->set_leader(0);
    SendToLogManager(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), req, 0);
    SendToLogManager(MakeMachineId(0, 0, 1), MakeMachineId(0, 0, 1), req, 0);
  }

  // The batch order is replicated across all machines
  for (int reg = 0; reg < NUM_REGIONS; reg++)
    for (int p = 0; p < NUM_PARTITIONS; p++) {
      MachineId id = MakeMachineId(reg, 0, p);
      auto txn1 = ReceiveTxn(id);
      auto txn2 = ReceiveTxn(id);
      ASSERT_EQ(*txn1, *expected_txn_1);
      ASSERT_EQ(*txn2, *expected_txn_2);
      delete txn1;
      delete txn2;
    }
}

TEST_F(LogManagerTest, BatchOrderBeforeBatchData) {
  auto expected_txn_1 = MakeTransaction({{"A"}, {"B", KeyType::WRITE}});
  auto expected_txn_2 = MakeTransaction({{"X"}, {"Y", KeyType::WRITE}});
  auto batch = MakeBatch(100, {expected_txn_1, expected_txn_2}, SINGLE_HOME);

  // Send local ordering
  {
    Envelope req;
    auto local_batch_order = req.mutable_request()->mutable_forward_batch_order()->mutable_local_batch_order();
    local_batch_order->set_generator(0);
    local_batch_order->set_slot(0);
    local_batch_order->set_leader(0);
    SendToLogManager(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), req, 0);
    SendToLogManager(MakeMachineId(0, 0, 1), MakeMachineId(0, 0, 1), req, 0);
  }

  // Replicate batch data to all machines
  // Distribute batch to local replica
  {
    Envelope req;
    auto forward_batch_data = req.mutable_request()->mutable_forward_batch_data();
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->set_generator(0);
    forward_batch_data->set_generator_position(0);

    SendToLogManager(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 0), req, 0);
    SendToLogManager(MakeMachineId(0, 0, 0), MakeMachineId(0, 0, 1), req, 0);
  }
  // Distribute batch to remote replica
  {
    Envelope req;
    auto forward_batch_data = req.mutable_request()->mutable_forward_batch_data();
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->set_generator(0);
    forward_batch_data->set_generator_position(0);

    SendToLogManager(MakeMachineId(0, 0, 0), MakeMachineId(1, 0, 0), req, 0);
  }

  // Both batch data and batch order are sent at the same time
  for (int reg = 0; reg < NUM_REGIONS; reg++)
    for (int p = 0; p < NUM_PARTITIONS; p++) {
      MachineId id = MakeMachineId(reg, 0, p);
      auto txn1 = ReceiveTxn(id);
      auto txn2 = ReceiveTxn(id);
      ASSERT_EQ(*txn1, *expected_txn_1);
      ASSERT_EQ(*txn2, *expected_txn_2);
      delete txn1;
      delete txn2;
    }

  delete batch;
}