#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/string_utils.h"
#include "common/types.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

using std::vector;

namespace slog {

class BasicWorkload : public Workload {
 public:
  BasicWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const std::string& data_dir,
                const std::string& params_str, const uint32_t seed = std::random_device()(),
                const RawParamMap& extra_default_params = {});

  std::pair<Transaction*, TransactionProfile> NextTransaction();

 protected:
  int local_region() { return config_->num_regions() == 1 ? local_replica_ : local_region_; }

  ConfigurationPtr config_;
  RegionId local_region_;
  ReplicaId local_replica_;
  std::vector<int> distance_ranking_;
  int zipf_coef_;

  // This is an index of keys by their partition and home.
  // Each partition holds a vector of homes, each of which
  // is a list of keys.
  vector<vector<KeyList>> partition_to_key_lists_;

  std::mt19937 rg_;
  RandomStringGenerator rnd_str_;

  TxnId client_txn_id_counter_;
};

}  // namespace slog