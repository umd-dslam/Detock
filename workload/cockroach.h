#pragma once

#include <vector>

#include "common/configuration.h"
#include "common/types.h"
#include "proto/transaction.pb.h"
#include "workload/workload.h"

namespace slog {

class CockroachWorkload : public Workload {
 public:
  CockroachWorkload(const ConfigurationPtr& config, uint32_t region, const std::string& params_str,
                    const uint32_t seed = std::random_device()());

  std::pair<Transaction*, TransactionProfile> NextTransaction();

 private:
  ConfigurationPtr config_;
  uint32_t local_region_;
  int hot_;
  int records_;
  int value_size_;
  bool sort_keys_;

  std::mt19937 rg_;
  RandomStringGenerator rnd_str_;

  TxnId client_txn_id_counter_;

  std::vector<uint64_t> KeyBatch(int nl, int n_hot);
};

}  // namespace slog