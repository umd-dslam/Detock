#pragma once

#include <glog/logging.h>

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/async_log.h"
#include "common/configuration.h"
#include "common/metrics.h"
#include "common/types.h"
#include "connection/broker.h"
#include "connection/sender.h"
#include "module/janus/horizon.h"
#include "module/janus/tarjan.h"
#include "module/scheduler_components/worker.h"
#include "storage/storage.h"

namespace slog {

class PendingIndex {
 public:
  PendingIndex(int local_partition);
  bool Add(const internal::JanusDependency& ancestor, TxnId descendant);
  std::optional<std::unordered_set<TxnId>> Remove(TxnId ancestor);

 private:
  int local_partition_;
  std::unordered_map<TxnId, std::unordered_set<TxnId>> index_;
};

class JanusScheduler : public NetworkedModule {
 public:
  JanusScheduler(const std::shared_ptr<Broker>& broker, const std::shared_ptr<Storage>& storage,
                 const MetricsRepositoryManagerPtr& metrics_manager,
                 std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "Scheduler"; }

 protected:
  void Initialize() final;

  void OnInternalRequestReceived(EnvelopePtr&& env) final;
  void OnInternalResponseReceived(EnvelopePtr&& env) final;

  // Handle responses from the workers
  bool OnCustomSocket() final;

 private:
  void ProcessTransaction(EnvelopePtr&& env);
  bool ProcessInquiry(EnvelopePtr&& env);
  void DispatchSCCs(const std::vector<SCC>& sccs);
  void InquireMissingDependencies(TxnId txn_id, const std::vector<internal::JanusDependency>& deps);
  void CheckPendingInquiry(TxnId txn_id);
  void CheckPendingTxns(TxnId txn_id);

  std::unordered_map<TxnId, Transaction*> txns_;

  Graph graph_;
  TarjanSCCsFinder sccs_finder_;
  TxnHorizon execution_horizon_;
  PendingIndex pending_txns_;
  std::unordered_map<TxnId, EnvelopePtr> pending_inquiries_;

  // This must be defined at the end so that the workers exit before any resources
  // in the scheduler is destroyed
  std::vector<std::unique_ptr<ModuleRunner>> workers_;
  int current_worker_;
};

}  // namespace slog