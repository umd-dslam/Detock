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

namespace slog {

class Quorum;
class QuorumDeps;

using Dependencies = std::set<TxnIdAndPartitionsBitmap>;

class JanusCoordinator : public NetworkedModule {
 public:
  JanusCoordinator(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                   const MetricsRepositoryManagerPtr& metrics_manager,
                   std::chrono::milliseconds poll_timeout_ms = kModuleTimeout);

  std::string name() const override { return "JanusCoordinator"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;
  void OnInternalResponseReceived(EnvelopePtr&& env) final;

 private:
  struct CoordinatorTxnInfo {
    CoordinatorTxnInfo(TxnId txn_id, int num_partitions) :
        txn_id(txn_id), phase(Phase::PRE_ACCEPT), sharded_deps(num_partitions) {}

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

  const SharderPtr sharder_;
  std::unordered_map<TxnId, CoordinatorTxnInfo> txns_;
};

}  // namespace slog