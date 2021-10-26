#include "workload/cockroach.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <algorithm>
#include <random>
#include <sstream>

#include "common/proto_utils.h"

using std::bernoulli_distribution;
using std::sample;

namespace slog {
namespace {

// Percentage of multi-home transactions
constexpr char MH_PCT[] = "mh";
// Theta parameter of the zipf distribution
// See "Quickly Generating Billion-Record Synthetic Databases"
// by Gray, Sundaresan, Englert, Baclawski, and Weinberger, SIGMOD 1994.
constexpr char THETA[] = "theta";
// Number of records in a transaction
constexpr char RECORDS[] = "records";
// Number of write records in a transaction
constexpr char WRITES[] = "writes";
// Size of a written value in bytes
constexpr char VALUE_SIZE[] = "value_size";

const RawParamMap DEFAULT_PARAMS = {{MH_PCT, "0"}, {THETA, "0.1"}, {RECORDS, "10"}, {WRITES, "10"},  {VALUE_SIZE, "50"}};

}  // namespace

CockroachWorkload::CockroachWorkload(const ConfigurationPtr& config, uint32_t region,
                                     const string& params_str, const uint32_t seed)
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      local_region_(region),
      rg_(seed),
      rnd_str_(seed),
      client_txn_id_counter_(0) {
  name_ = "cockroach";
}

std::pair<Transaction*, TransactionProfile> BasicWorkload::NextTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  // Decide if this is a multi-home txn or not
  auto num_replicas = config_->num_replicas();
  auto multi_home_pct = params_.GetDouble(MH_PCT);
  bernoulli_distribution is_mh(multi_home_pct / 100);
  pro.is_multi_home = is_mh(rg_);

  // Select a number of homes to choose from for each record
  vector<uint32_t> selected_homes;
  selected_homes.push_back(local_region_);
  if (pro.is_multi_home) {
    CHECK_GE(num_replicas, 2) << "There must be at least 2 regions for MH txns";
    selected_homes.push_back(local_region_ + ...);
  }

  vector<KeyMetadata> keys;
  vector<vector<string>> code;

  auto writes = params_.GetUInt32(WRITES);
  auto records = params_.GetUInt32(RECORDS);
  auto value_size = params_.GetUInt32(VALUE_SIZE);

  CHECK_LE(writes, records) << "Number of writes cannot exceed number of records in a transaction!";

  for (size_t i = 0; i < records; i++) {
    auto home = selected_homes[i / ((records + 1) / selected_homes.size())];
    for (;;) {
      Key key;
      if (is_hot[i]) {
        key = partition_to_key_lists_[partition][home].GetRandomHotKey(rg_);
      } else {
        key = partition_to_key_lists_[partition][home].GetRandomColdKey(rg_);
      }

      auto ins = pro.records.try_emplace(key, TransactionProfile::Record());
      if (ins.second) {
        auto& record = ins.first->second;
        record.is_hot = is_hot[i];
        // Decide whether this is a read or a write record
        if (i < writes) {
          code.push_back({"SET", key, rnd_str_(value_size)});
          keys.emplace_back(key, KeyType::WRITE);
          record.is_write = true;
        } else {
          code.push_back({"GET", key});
          keys.emplace_back(key, KeyType::READ);
          record.is_write = false;
        }
        record.home = home;
        record.partition = partition;
        break;
      }
    }
  }

  // Construct a new transaction
  auto txn = MakeTransaction(keys, code);
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

}  // namespace slog