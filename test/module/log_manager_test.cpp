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
  ASSERT_EQ(make_pair(0U, make_pair(100UL, 0)), log.NextBatch());

  log.AddBatchId(222 /* queue_id */, 0 /* position */, 200 /* batch_id */);
  ASSERT_FALSE(log.HasNextBatch());

  log.AddSlot(1 /* slot_id */, 222 /* queue_id */, 1 /* leader */);
  ASSERT_EQ(make_pair(1U, make_pair(200UL, 1)), log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST(LocalLogTest, BatchesComeFirst) {
  LocalLog log;

  log.AddBatchId(222 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  log.AddBatchId(111 /* queue_id */, 0 /* position */, 200 /* batch_id */);
  log.AddBatchId(333 /* queue_id */, 0 /* position */, 300 /* batch_id */);
  log.AddBatchId(333 /* queue_id */, 1 /* position */, 400 /* batch_id */);

  log.AddSlot(0 /* slot_id */, 111 /* queue_id */, 0 /* leader */);
  ASSERT_EQ(make_pair(0U, make_pair(200UL, 0)), log.NextBatch());

  log.AddSlot(1 /* slot_id */, 333 /* queue_id */, 1 /* leader */);
  ASSERT_EQ(make_pair(1U, make_pair(300UL, 1)), log.NextBatch());

  log.AddSlot(2 /* slot_id */, 222 /* queue_id */, 2 /* leader */);
  ASSERT_EQ(make_pair(2U, make_pair(100UL, 2)), log.NextBatch());

  log.AddSlot(3 /* slot_id */, 333 /* queue_id */, 3 /* leader */);
  ASSERT_EQ(make_pair(3U, make_pair(400UL, 3)), log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

TEST(LocalLogTest, SlotsComeFirst) {
  LocalLog log;

  log.AddSlot(2 /* slot_id */, 222 /* queue_id */, 0 /* leader */);
  log.AddSlot(1 /* slot_id */, 333 /* queue_id */, 0 /* leader */);
  log.AddSlot(3 /* slot_id */, 333 /* queue_id */, 0 /* leader */);
  log.AddSlot(0 /* slot_id */, 111 /* queue_id */, 0 /* leader */);

  log.AddBatchId(111 /* queue_id */, 0 /* position */, 200 /* batch_id */);
  ASSERT_EQ(make_pair(0U, make_pair(200UL, 0)), log.NextBatch());

  log.AddBatchId(333 /* queue_id */, 0 /* position */, 300 /* batch_id */);
  ASSERT_EQ(make_pair(1U, make_pair(300UL, 0)), log.NextBatch());

  log.AddBatchId(222 /* queue_id */, 0 /* position */, 100 /* batch_id */);
  ASSERT_EQ(make_pair(2U, make_pair(100UL, 0)), log.NextBatch());

  log.AddBatchId(333 /* queue_id */, 1 /* position */, 400 /* batch_id */);
  ASSERT_EQ(make_pair(3U, make_pair(400UL, 0)), log.NextBatch());

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

  ASSERT_EQ(make_pair(0U, make_pair(100UL, 1)), log.NextBatch());
  ASSERT_EQ(make_pair(1U, make_pair(400UL, 1)), log.NextBatch());
  ASSERT_EQ(make_pair(2U, make_pair(300UL, 1)), log.NextBatch());
  ASSERT_EQ(make_pair(3U, make_pair(200UL, 1)), log.NextBatch());

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

  ASSERT_EQ(make_pair(0U, make_pair(100UL, 0)), log.NextBatch());
  ASSERT_EQ(make_pair(1U, make_pair(200UL, 0)), log.NextBatch());
  ASSERT_EQ(make_pair(2U, make_pair(300UL, 0)), log.NextBatch());

  ASSERT_FALSE(log.HasNextBatch());
}

const int NUM_REPLICAS = 2;
const int NUM_PARTITIONS = 2;
constexpr int NUM_MACHINES = NUM_REPLICAS * NUM_PARTITIONS;

class LogManagerTest : public ::testing::Test {
 public:
  void SetUp() {
    auto configs = MakeTestConfigurations("log_manager", NUM_REPLICAS, NUM_PARTITIONS);
    for (int i = 0; i < 4; i++) {
      slogs_[i] = make_unique<TestSlog>(configs[i]);
      slogs_[i]->AddLogManagers();
      slogs_[i]->AddOutputSocket(kSchedulerChannel);
      senders_[i] = slogs_[i]->NewSender();
      slogs_[i]->StartInNewThreads();
    }
  }

  void SendToLogManager(int from, int to, const Envelope& req, int home) {
    senders_[from]->Send(req, to, kLogManagerChannel + home);
  }

  Transaction* ReceiveTxn(int i) {
    auto req_env = slogs_[i]->ReceiveFromOutputSocket(kSchedulerChannel);
    if (req_env == nullptr) {
      return nullptr;
    }
    if (req_env->request().type_case() != internal::Request::kForwardTxn) {
      return nullptr;
    }
    return req_env->mutable_request()->mutable_forward_txn()->release_txn();
  }

  unique_ptr<Sender> senders_[4];
  unique_ptr<TestSlog> slogs_[4];
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
    forward_batch_data->set_home(0);
    forward_batch_data->set_home_position(0);

    SendToLogManager(0, 0, req, 0);
    SendToLogManager(0, 1, req, 0);
  }
  // Distribute batch to remote replica
  {
    Envelope req;
    auto forward_batch_data = req.mutable_request()->mutable_forward_batch_data();
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->set_home(0);
    forward_batch_data->set_home_position(0);

    SendToLogManager(0, 2, req, 0);
  }

  // Then send local ordering
  {
    Envelope req;
    auto local_batch_order = req.mutable_request()->mutable_forward_batch_order()->mutable_local_batch_order();
    local_batch_order->set_queue_id(0);
    local_batch_order->set_slot(0);
    local_batch_order->set_leader(0);
    SendToLogManager(0, 0, req, 0);
    SendToLogManager(1, 1, req, 0);
  }

  // The batch order is replicated across all machines
  for (int i = 0; i < NUM_MACHINES; i++) {
    auto txn1 = ReceiveTxn(i);
    auto txn2 = ReceiveTxn(i);
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
    local_batch_order->set_queue_id(0);
    local_batch_order->set_slot(0);
    local_batch_order->set_leader(0);
    SendToLogManager(0, 0, req, 0);
    SendToLogManager(1, 1, req, 0);
  }

  // Replicate batch data to all machines
  // Distribute batch to local replica
  {
    Envelope req;
    auto forward_batch_data = req.mutable_request()->mutable_forward_batch_data();
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->set_home(0);
    forward_batch_data->set_home_position(0);

    SendToLogManager(0, 0, req, 0);
    SendToLogManager(0, 1, req, 0);
  }
  // Distribute batch to remote replica
  {
    Envelope req;
    auto forward_batch_data = req.mutable_request()->mutable_forward_batch_data();
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->mutable_batch_data()->Add()->CopyFrom(*batch);
    forward_batch_data->set_home(0);
    forward_batch_data->set_home_position(0);

    SendToLogManager(0, 2, req, 0);
  }

  // Both batch data and batch order are sent at the same time
  for (int i = 0; i < NUM_MACHINES; i++) {
    auto txn1 = ReceiveTxn(i);
    auto txn2 = ReceiveTxn(i);
    ASSERT_EQ(*txn1, *expected_txn_1);
    ASSERT_EQ(*txn2, *expected_txn_2);
    delete txn1;
    delete txn2;
  }

  delete batch;
}