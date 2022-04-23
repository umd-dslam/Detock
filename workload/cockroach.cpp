#include "workload/cockroach.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <algorithm>
#include <random>
#include <sstream>
#include <unordered_set>

#include "common/proto_utils.h"

using std::bernoulli_distribution;
using std::vector;

namespace slog {
namespace {

// Percentage of multi-home transactions
constexpr char MH_PCT[] = "mh";
// Number of hot keys per region
constexpr char HOT[] = "hot";
// Number of records in a transaction
constexpr char RECORDS[] = "records";
// Size of a written value in bytes
constexpr char VALUE_SIZE[] = "value_size";
// Sort the keys
constexpr char SORT_KEYS[] = "sort_keys";

const RawParamMap DEFAULT_PARAMS = {
    {MH_PCT, "0"}, {HOT, "100"}, {RECORDS, "10"}, {VALUE_SIZE, "100"}, {SORT_KEYS, "0"}};

long long NumKeysPerRegion(const ConfigurationPtr& config) {
  auto simple_partitioning = config->proto_config().simple_partitioning2();
  auto num_records = static_cast<long long>(simple_partitioning.num_records());
  return num_records / config->num_regions();
}

}  // namespace

CockroachWorkload::CockroachWorkload(const ConfigurationPtr& config, uint32_t region, const string& params_str,
                                     const uint32_t seed)
    : Workload(DEFAULT_PARAMS, params_str),
      config_(config),
      local_region_(region),
      rg_(seed),
      rnd_str_(seed),
      client_txn_id_counter_(0) {
  name_ = "cockroach";
  hot_ = params_.GetInt32(HOT);
  records_ = params_.GetInt32(RECORDS);
  value_size_ = params_.GetInt32(VALUE_SIZE);
  sort_keys_ = params_.GetInt32(SORT_KEYS) > 0;

  CHECK_LE(hot_, NumKeysPerRegion(config)) << "Number of hot records cannot exceed number of records per region!";
}

std::pair<Transaction*, TransactionProfile> CockroachWorkload::NextTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  auto num_regions = config_->num_regions();
  auto num_partitions = config_->num_partitions();

  // Decide if this is a multi-home txn or not
  auto multi_home_pct = params_.GetDouble(MH_PCT);
  bernoulli_distribution is_mh(multi_home_pct / 100);
  pro.is_multi_home = is_mh(rg_);

  vector<uint64_t> numeric_keys(records_);
  vector<uint32_t> homes(records_);
  vector<bool> is_hot(records_, false);

  if (!pro.is_multi_home) {
    // Select keys from the local region
    numeric_keys = KeyBatch(records_, 2);
    CHECK(!numeric_keys.empty());
    for (size_t i = 0; i < numeric_keys.size(); i++) {
      numeric_keys[i] = numeric_keys[i] * num_regions + local_region_;
      homes[i] = local_region_;
    }
    is_hot[0] = is_hot[1] = true;
  } else {
    // Select a remote region
    std::uniform_int_distribution<> dis(1, num_regions - 1);
    uint32_t remote_region = (local_region_ + dis(rg_)) % num_regions;
    int num_local_keys = records_ / 2;

    // Select keys from the local region
    auto local_keys = KeyBatch(num_local_keys, 1);
    CHECK(!local_keys.empty());
    for (int i = 0; i < num_local_keys; i++) {
      numeric_keys[i] = local_keys[i] * num_regions + local_region_;
      homes[i] = local_region_;
    }

    // Select keys from a remote region
    auto remote_keys = KeyBatch(records_ - num_local_keys, 1);
    CHECK(!remote_keys.empty());
    for (int i = num_local_keys; i < records_; i++) {
      numeric_keys[i] = remote_keys[i - num_local_keys] * num_regions + remote_region;
      homes[i] = remote_region;
    }
    is_hot[0] = is_hot[num_local_keys] = true;
  }

  vector<KeyMetadata> keys;
  vector<vector<string>> code;

  int last_partition = -1;

  if (sort_keys_) {
    sort(numeric_keys.begin(), numeric_keys.end());
  }

  for (size_t i = 0; i < numeric_keys.size(); i++) {
    Key key = std::to_string(numeric_keys[i]);
    keys.emplace_back(key, KeyType::WRITE);
    code.push_back({"SET", key, rnd_str_(value_size_)});

    auto ins = pro.records.emplace(key, TransactionProfile::Record());
    auto& record = ins.first->second;
    record.is_write = true;
    record.home = homes[i];
    // See SimpleSharder2
    record.partition = numeric_keys[i] / num_regions % num_partitions;
    record.is_hot = is_hot[i];
    if (last_partition == -1) {
      last_partition = record.partition;
    } else if (record.partition != static_cast<uint32_t>(last_partition)) {
      pro.is_multi_partition = true;
    }
  }

  // Construct a new transaction
  auto txn = MakeTransaction(keys, code);
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

std::vector<uint64_t> CockroachWorkload::KeyBatch(int n, int n_hot) {
  if (n < 1) {
    return {};
  }

  std::unordered_set<uint64_t> exists;
  std::vector<uint64_t> res(n);
  int i = 0;

  std::uniform_int_distribution<> u_hot(0, hot_);
  for (; i < n_hot; i++) {
    for (;;) {
      res[i] = u_hot(rg_);
      if (exists.find(res[i]) == exists.end()) {
        break;
      }
    }
    exists.insert(res[i]);
  }

  int num_keys = NumKeysPerRegion(config_);
  std::uniform_int_distribution<> u_cold(0, num_keys - hot_);
  for (; i < n; i++) {
    for (;;) {
      res[i] = u_cold(rg_) + hot_;
      if (exists.find(res[i]) == exists.end()) {
        break;
      }
    }
    exists.insert(res[i]);
  }
  return res;
}

}  // namespace slog