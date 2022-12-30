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
#include "proto/transaction.pb.h"

namespace slog {

enum class Phase {
  PRE_ACCEPT,
  ACCEPT,
  COMMIT
};

using Dependency = std::unordered_set<TxnId>;

class QuorumDeps;

struct TransactionState {
  TransactionState() : phase(Phase::PRE_ACCEPT) {}

  Phase phase;
  std::vector<std::optional<QuorumDeps>> sharded_deps;
};

class QuorumDeps {
  QuorumDeps(const Dependency& deps, int num_replicas);
  void Add(const Dependency& deps);
  bool is_done();
  bool is_fast_quorum();

 private:
  Dependency deps_;
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

  const SharderPtr sharder_;
  std::unordered_map<TxnId, TransactionState> txns_;
};

}  // namespace slog