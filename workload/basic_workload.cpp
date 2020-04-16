#include "workload/basic_workload.h"

#include <algorithm>
#include <iomanip>
#include <fcntl.h>
#include <random>
#include <sstream>
#include <unordered_set>
#include <vector>

#include <glog/logging.h>

#include "common/offline_data_reader.h"
#include "common/proto_utils.h"
#include "proto/offline_data.pb.h"

using std::discrete_distribution;
using std::shuffle;
using std::uniform_int_distribution;
using std::string;
using std::vector;
using std::unordered_set;

namespace slog {

KeyList::KeyList(size_t num_hot_keys) : num_hot_keys_(num_hot_keys) {}

void KeyList::AddKey(Key key) {
  if (hot_keys_.size() < num_hot_keys_) {
    hot_keys_.push_back(key);
    return;
  }
  cold_keys_.push_back(key);
}

Key KeyList::GetRandomHotKey() {
  if (hot_keys_.empty()) {
    throw std::runtime_error("There is no hot key to pick from. Please check your data");
  }
  return PickOne(hot_keys_, re_);
}

Key KeyList::GetRandomColdKey() {
  if (cold_keys_.empty()) {
    throw std::runtime_error("There is no cold key to pick from. Please check your data");
  }
  return PickOne(cold_keys_, re_);
}

BasicWorkload::BasicWorkload(
    ConfigurationPtr config,
    std::string data_dir,
    double multi_home_pct,
    double multi_partition_pct)
  : config_(config),
    multi_home_pct_(multi_home_pct),
    multi_partition_pct_(multi_partition_pct),
    partition_to_key_lists_(config->GetNumPartitions()),
    client_txn_id_counter_(0) {

  for (auto& key_lists : partition_to_key_lists_) {
    for (uint32_t rep = 0; rep < config->GetNumReplicas(); rep++) {
      // TODO (ctring): initialize each key list with some hot keys
      key_lists.emplace_back();
    }
  }

  // Load and index the initial data
  for (uint32_t partition = 0; partition < config->GetNumPartitions(); partition++) {
    auto data_file = data_dir + "/" + std::to_string(partition) + ".dat";
    auto fd = open(data_file.c_str(), O_RDONLY);
    if (fd < 0) {
      LOG(FATAL) << "Error while loading \"" << data_file << "\": " << strerror(errno);
    }

    OfflineDataReader reader(fd);
    LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums from " << data_file;
    while (reader.HasNextDatum()) {
      auto datum = reader.GetNextDatum();
      CHECK_LT(datum.master(), config->GetNumReplicas())
          << "Master number exceeds number of replicas";

      partition_to_key_lists_[partition][datum.master()].AddKey(datum.key());
    }
    close(fd);
  }
}

std::pair<Transaction*, TransactionProfile>
BasicWorkload::NextTransaction() {
  CHECK_LE(NUM_WRITES, NUM_RECORDS) 
      << "Number of writes cannot exceed number of records in a txn!";

  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  // Decide if this is a multi-partition txn or not
  discrete_distribution<> spmp({100 - multi_partition_pct_, multi_partition_pct_});
  pro.is_multi_partition = spmp(re_);

  // Select a number of partitions to choose from for each record
  auto candidate_partitions = Choose(
      config_->GetNumPartitions(),
      pro.is_multi_partition ? MP_NUM_PARTITIONS : 1,
      re_);

  // Decide if this is a multi-home txn or not
  discrete_distribution<> shmh({100 - multi_home_pct_, multi_home_pct_});
  pro.is_multi_home = shmh(re_);

  // Select a number of homes to choose from for each record
  auto candidate_homes = Choose(
      config_->GetNumReplicas(),
      pro.is_multi_home ? MH_NUM_HOMES : 1,
      re_);

  unordered_set<Key> read_set;
  unordered_set<Key> write_set;
  std::ostringstream code;

  // Fill txn with write operators
  for (size_t i = 0; i < NUM_WRITES; i++) {
    auto partition = candidate_partitions[i % candidate_partitions.size()];
    auto home = candidate_homes[i % candidate_homes.size()];
    // TODO: Add hot keys later
    auto key = partition_to_key_lists_[partition][home].GetRandomColdKey();
    code << "SET " << key << " " << RandomString(VALUE_SIZE, re_) << " ";
    write_set.insert(key);

    pro.key_to_home[key] = home;
    pro.key_to_partition[key] = partition;
  }

  // Fill txn with read operators
  for (size_t i = 0; i < NUM_RECORDS - NUM_WRITES; i++) {
    auto partition = candidate_partitions[(NUM_WRITES + i) % candidate_partitions.size()];
    auto home = candidate_homes[(NUM_WRITES + i) % candidate_homes.size()];
    // TODO: Add hot keys later
    auto key = partition_to_key_lists_[partition][home].GetRandomColdKey();
    code << "GET " << key << " ";
    read_set.insert(key);

    pro.key_to_home[key] = home;
    pro.key_to_partition[key] = partition;
  }

  auto txn = MakeTransaction(read_set, write_set, code.str());
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

} // namespace slog