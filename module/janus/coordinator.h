#pragma once

#include <set>
#include <unordered_map>
#include <vector>

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/sharder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "module/janus/phase.h"
#include "proto/internal.pb.h"
#include "proto/transaction.pb.h"

namespace janus {

using slog::ConfigurationPtr;
using slog::EnvelopePtr;
using slog::MachineId;
using slog::MetricsRepositoryManagerPtr;
using slog::TxnId;

using Dependencies = std::set<TxnIdAndPartitionsBitmap>;

class Quorum {
 public:
  Quorum(int num_replicas) : num_replicas_(num_replicas), count_(0) {}

  void Inc() { count_++; }

  bool is_done() { return count_ >= (num_replicas_ + 1) / 2; }

 private:
  const int num_replicas_;
  int count_;
};

class QuorumDeps {
 public:
  QuorumDeps(int num_replicas) : num_replicas_(num_replicas), is_fast_quorum_(true), count_(0) {}

  void Add(const Dependencies& deps) {
    if (is_done()) {
      return;
    }

    if (!is_fast_quorum_ || (count_ > 0 && deps != this->deps)) {
      is_fast_quorum_ = false;
      this->deps.insert(deps.begin(), deps.end());
    }

    count_++;
  }

  bool is_done() {
    if (is_fast_quorum_) return count_ == num_replicas_;

    return count_ >= (num_replicas_ + 1) / 2;
  }

  bool is_fast_quorum() { return is_fast_quorum_; }

  Dependencies deps;

 private:
  const int num_replicas_;
  bool is_fast_quorum_;
  int count_;
};

class Coordinator : public slog::NetworkedModule {
 public:
  Coordinator(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
              const MetricsRepositoryManagerPtr& metrics_manager,
              std::chrono::milliseconds poll_timeout_ms = slog::kModuleTimeout);

  std::string name() const override { return "Coordinator"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;
  void OnInternalResponseReceived(EnvelopePtr&& env) final;

 private:
  struct CoordinatorTxnInfo {
    CoordinatorTxnInfo(TxnId txn_id, int num_partitions)
        : txn_id(txn_id), phase(Phase::PRE_ACCEPT), sharded_deps(num_partitions, std::nullopt) {}

    TxnId txn_id;
    Phase phase;
    std::vector<std::optional<QuorumDeps>> sharded_deps;
    std::vector<std::optional<Quorum>> quorums;
    std::vector<int> participants;
    std::vector<MachineId> destinations;
  };

  void StartNewTxn(EnvelopePtr&& env);
  void PreAcceptTxn(EnvelopePtr&& env);
  void AcceptTxn(EnvelopePtr&& env);
  void CommitTxn(CoordinatorTxnInfo& txn_info);

  const slog::SharderPtr sharder_;
  std::unordered_map<TxnId, CoordinatorTxnInfo> txns_;
};

}  // namespace janus