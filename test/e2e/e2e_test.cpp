#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <thread>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "connection/broker.h"
#include "module/forwarder.h"
#include "module/server.h"
#include "proto/api.pb.h"
#include "storage/mem_only_storage.h"
#include "test/test_utils.h"

using namespace std;
using namespace slog;

const int kNumRegions = 2;
const int kNumReplicas = 3;
const int kNumPartitions = 2;
const int kNumMachines = kNumRegions * kNumReplicas * kNumPartitions;
const int kDataItems = 2;

class E2ETest : public ::testing::TestWithParam<tuple<bool, int, int, int>> {
 protected:
  virtual internal::Configuration CustomConfig() { return internal::Configuration(); }

  void SetUp() {
    auto param = GetParam();
    bool local_sync_rep = std::get<0>(param);

    auto custom_config = CustomConfig();
    custom_config.set_replication_factor(2);
    custom_config.add_replication_order("1");
    custom_config.add_replication_order("0");
    custom_config.set_num_workers(2);
    custom_config.set_num_log_managers(2);
    auto configs =
        MakeTestConfigurations("e2e", kNumRegions, kNumReplicas, kNumPartitions, custom_config, local_sync_rep);

    std::pair<Key, Record> data[kNumPartitions][kDataItems] = {{{"A", {"valA", 0, 0}}, {"C", {"valC", 1, 1}}},
                                                               {{"B", {"valB", 0, 1}}, {"X", {"valX", 1, 0}}}};

    int counter = 0;
    for (int reg = 0; reg < kNumRegions; reg++) {
      for (int rep = 0; rep < kNumReplicas; rep++) {
        for (int p = 0; p < kNumPartitions; p++) {
          auto id = MakeMachineId(reg, rep, p);
          auto config = configs[counter++];
          auto& slog = test_slogs_.emplace(id, config).first->second;
          slog.AddServerAndClient();
          slog.AddForwarder();
          slog.AddMultiHomeOrderer();
          slog.AddSequencer();
          slog.AddLogManagers();
          slog.AddScheduler();
          slog.AddLocalPaxos();
          // One region is selected to globally order the multihome batches
          if (config->leader_region_for_multi_home_ordering() == config->local_region()) {
            slog.AddGlobalPaxos();
          }

          for (int k = 0; k < kDataItems; k++) {
            const auto& item = data[p][k];
            slog.Data(Key(item.first), Record(item.second));
          }
        }
      }
    }

    auto main_reg = std::get<1>(param);
    auto main_rep = std::get<2>(param);
    auto main_part = std::get<3>(param);
    main_ = MakeMachineId(main_reg, main_rep, main_part);
    CHECK(test_slogs_.find(main_) != test_slogs_.end()) << "Cannot find machine " << MACHINE_ID_STR(main_);

    for (auto& [_, slog] : test_slogs_) {
      slog.StartInNewThreads();
    }
  }

  Transaction SendAndReceiveResult(Transaction* txn) {
    auto it = test_slogs_.find(main_);
    CHECK(it != test_slogs_.end());
    it->second.SendTxn(txn);
    return it->second.RecvTxnResult();
  }

  // Send the transaction and check the result using the given callback function
  // for all machines one by one.
  void SendAndCheckAllOneByOne(Transaction* txn, std::function<void(Transaction)> cb) {
    for (int reg = 0; reg < kNumRegions; reg++) {
      for (int p = 0; p < kNumPartitions; p++) {
        auto it = test_slogs_.find(MakeMachineId(reg, p % kNumReplicas, p));
        CHECK(it != test_slogs_.end());

        auto copied_txn = new Transaction(*txn);

        it->second.SendTxn(copied_txn);
        auto resp = it->second.RecvTxnResult();

        cb(resp);
      }
    }
  }

 private:
  unordered_map<MachineId, TestSlog> test_slogs_;
  MachineId main_;
};

TEST_P(E2ETest, SingleHomeSinglePartition) {
  auto txn1 = MakeTransaction({{"A", KeyType::WRITE}}, {{"SET", "A", "newA"}});
  auto resp = SendAndReceiveResult(txn1);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(resp.keys_size(), 1);
  ASSERT_EQ(TxnValueEntry(resp, "A").value(), "valA");
  ASSERT_EQ(TxnValueEntry(resp, "A").new_value(), "newA");

  auto txn2 = MakeTransaction({{"A", KeyType::READ}}, {{"GET", "A"}});
  SendAndCheckAllOneByOne(txn2, [this](Transaction resp) {
    ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(resp.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(resp, "A").value(), "newA");
  });
}

TEST_P(E2ETest, SingleHomeMultiPartition) {
  auto txn1 = MakeTransaction({{"A", KeyType::READ}, {"B", KeyType::WRITE}}, {{"GET", "A"}, {"SET", "B", "newB"}});
  auto resp = SendAndReceiveResult(txn1);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
  ASSERT_EQ(resp.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(resp, "A").value(), "valA");
  ASSERT_EQ(TxnValueEntry(resp, "B").new_value(), "newB");

  auto txn2 = MakeTransaction({{"B", KeyType::READ}}, {{"GET", "B"}});
  SendAndCheckAllOneByOne(txn2, [this](Transaction resp) {
    ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(resp.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(resp, "B").value(), "newB");
  });
}

TEST_P(E2ETest, MultiHomeSinglePartition) {
  auto txn1 = MakeTransaction({{"A", KeyType::READ}, {"C", KeyType::WRITE}}, {{"GET", "A"}, {"SET", "C", "newC"}});
  auto resp = SendAndReceiveResult(txn1);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(resp.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(resp, "A").value(), "valA");
  ASSERT_EQ(TxnValueEntry(resp, "C").new_value(), "newC");

  auto txn2 = MakeTransaction({{"C", KeyType::READ}}, {{"GET", "C"}});
  SendAndCheckAllOneByOne(txn2, [this](Transaction resp) {
    ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
    ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
    ASSERT_EQ(resp.keys_size(), 1);
    ASSERT_EQ(TxnValueEntry(resp, "C").value(), "newC");
  });
}

TEST_P(E2ETest, MultiHomeMultiPartition) {
  auto txn = MakeTransaction({{"A", KeyType::READ}, {"X", KeyType::READ}, {"C", KeyType::READ}});
  auto resp = SendAndReceiveResult(txn);
  ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
  ASSERT_EQ(resp.keys().size(), 3);
  ASSERT_EQ(TxnValueEntry(resp, "A").value(), "valA");
  ASSERT_EQ(TxnValueEntry(resp, "X").value(), "valX");
  ASSERT_EQ(TxnValueEntry(resp, "C").value(), "valC");
}

#ifdef ENABLE_REMASTER
TEST_P(E2ETest, RemasterTxn) {
  auto remaster_txn = MakeTransaction({{"A", KeyType::WRITE}}, {}, 1);

  auto remaster_txn_resp = SendAndReceiveResult(remaster_txn);

  ASSERT_EQ(TransactionStatus::COMMITTED, remaster_txn_resp.status());

#ifdef REMASTER_PROTOCOL_COUNTERLESS
  ASSERT_EQ(TransactionType::MULTI_HOME_OR_LOCK_ONLY, remaster_txn_resp.internal().type());
#else
  ASSERT_EQ(TransactionType::SINGLE_HOME, remaster_txn_resp.internal().type());
#endif /* REMASTER_PROTOCOL_COUNTERLESS */

  auto txn = MakeTransaction({{"A"}, {"X"}});

  // Since replication factor is set to 2 for all tests in this file, it is
  // guaranteed that this txn will see the changes made by the remaster txn
  auto txn_resp = SendAndReceiveResult(txn);
  ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn_resp.internal().type(), TransactionType::SINGLE_HOME);  // used to be MH
  ASSERT_EQ(txn_resp.keys().size(), 2);
  ASSERT_EQ(TxnValueEntry(txn_resp, "A").value(), "valA");
  ASSERT_EQ(TxnValueEntry(txn_resp, "X").value(), "valX");
}
#endif

TEST_P(E2ETest, AbortTxnBadCommand) {
  // Multi-partition transaction where one of the partition will abort
  auto aborted_txn = MakeTransaction({{"A"}, {"B", KeyType::WRITE}}, {{"SET", "B", "notB"}, {"EQ", "A", "notA"}});

  auto aborted_txn_resp = SendAndReceiveResult(aborted_txn);
  ASSERT_EQ(TransactionStatus::ABORTED, aborted_txn_resp.status());
  ASSERT_EQ(TransactionType::SINGLE_HOME, aborted_txn_resp.internal().type());

  auto txn = MakeTransaction({{"B"}}, {{"GET", "B"}});

  auto txn_resp = SendAndReceiveResult(txn);
  ASSERT_EQ(txn_resp.status(), TransactionStatus::COMMITTED);
  ASSERT_EQ(txn_resp.internal().type(), TransactionType::SINGLE_HOME);
  // Value of B must not change because the previous txn was aborted
  ASSERT_EQ(TxnValueEntry(txn_resp, "B").value(), "valB");
}

TEST_P(E2ETest, AbortTxnEmptyKeySets) {
  // Multi-partition transaction where one of the partition will abort
  auto aborted_txn = MakeTransaction({});

  auto aborted_txn_resp = SendAndReceiveResult(aborted_txn);
  ASSERT_EQ(TransactionStatus::ABORTED, aborted_txn_resp.status());
  ASSERT_EQ(TransactionType::UNKNOWN, aborted_txn_resp.internal().type());
}

// class E2ETestBypassMHOrderer : public E2ETest {
//   internal::Configuration CustomConfig() final {
//     internal::Configuration config;
//     config.set_bypass_mh_orderer(true);
// #ifdef LOCK_MANAGER_DDR
//     config.set_ddr_interval(10);
// #else
//     config.set_synchronized_batching(true);
//     // Artificially offset the txn timestamp far into the future to avoid abort
//     config.set_timestamp_buffer_us(500000);
// #endif
//     return config;
//   }
// };

// TEST_P(E2ETestBypassMHOrderer, MultiHomeSinglePartition) {
//   auto txn1 = MakeTransaction({{"A", KeyType::READ}, {"C", KeyType::WRITE}}, {{"GET", "A"}, {"SET", "C", "newC"}});
//   auto resp = SendAndReceiveResult(txn1);
//   ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
//   ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
//   ASSERT_EQ(resp.keys().size(), 2);
//   ASSERT_EQ(TxnValueEntry(resp, "A").value(), "valA");
//   ASSERT_EQ(TxnValueEntry(resp, "C").new_value(), "newC");

//   auto txn2 = MakeTransaction({{"C", KeyType::READ}}, {{"GET", "C"}});
//   SendAndCheckAllOneByOne(txn2, [this](Transaction resp) {
//     ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
//     ASSERT_EQ(resp.internal().type(), TransactionType::SINGLE_HOME);
//     ASSERT_EQ(resp.keys_size(), 1);
//     ASSERT_EQ(TxnValueEntry(resp, "C").value(), "newC");
//   });
// }

// TEST_P(E2ETestBypassMHOrderer, MultiHomeMultiPartition) {
//   auto txn = MakeTransaction({{"A", KeyType::READ}, {"X", KeyType::READ}, {"C", KeyType::READ}});
//   auto resp = SendAndReceiveResult(txn);
//   ASSERT_EQ(resp.status(), TransactionStatus::COMMITTED);
//   ASSERT_EQ(resp.internal().type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);
//   ASSERT_EQ(resp.keys().size(), 3);
//   ASSERT_EQ(TxnValueEntry(resp, "A").value(), "valA");
//   ASSERT_EQ(TxnValueEntry(resp, "X").value(), "valX");
//   ASSERT_EQ(TxnValueEntry(resp, "C").value(), "valC");
// }

INSTANTIATE_TEST_SUITE_P(AllE2ETests, E2ETest,
                         testing::Combine(testing::Values(false, true), testing::Range(0, kNumRegions),
                                          testing::Range(0, 1), testing::Range(0, kNumPartitions)),
                         [](const testing::TestParamInfo<E2ETest::ParamType>& info) {
                           bool local_sync_rep = std::get<0>(info.param);
                           auto reg = std::get<1>(info.param);
                           auto rep = std::get<2>(info.param);
                           auto part = std::get<3>(info.param);
                           std::string out;
                           out += (local_sync_rep ? "Sync" : "Async");
                           out += "_";
                           out += std::to_string(reg) + "_";
                           out += std::to_string(rep) + "_";
                           out += std::to_string(part);
                           return out;
                         });

int main(int argc, char* argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  google::InstallFailureSignalHandler();
  return RUN_ALL_TESTS();
}