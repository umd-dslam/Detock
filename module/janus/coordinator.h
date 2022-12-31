#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/sharder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "module/janus/phase.h"
#include "proto/transaction.pb.h"

namespace slog {

using Dependency = std::unordered_set<TxnId>;

class QuorumDeps;

struct CoordinatorTxnInfo {
  CoordinatorTxnInfo(int num_partitions) : phase(Phase::PRE_ACCEPT), sharded_deps(num_partitions) {}

  Phase phase;
  std::vector<std::optional<QuorumDeps>> sharded_deps;
  std::vector<int> participants;
  std::vector<MachineId> destinations;
};

class QuorumDeps {
 public:
  QuorumDeps(const Dependency& deps, int num_replicas);
  void Add(const Dependency& deps);
  bool is_done();
  bool is_fast_quorum();

  Dependency deps;

 private:
  bool is_fast_quorum_;
  int count_;
  int num_replicas_;
};

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
  void StartNewTxn(EnvelopePtr&& env);
  void PreAcceptTxn(EnvelopePtr&& env);
  void AcceptTxn(EnvelopePtr&& env);

  const SharderPtr sharder_;
  std::unordered_map<TxnId, CoordinatorTxnInfo> txns_;
};

}  // namespace slog