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
#include "module/janus/worker.h"
#include "storage/storage.h"

namespace janus {

using slog::Broker;
using slog::EnvelopePtr;
using slog::MetricsRepositoryManagerPtr;
using slog::Storage;
using slog::Transaction;
using slog::internal::JanusDependency;

class PendingIndex {
 public:
  bool Add(const JanusDependency& ancestor, TxnId descendant);
  std::optional<std::unordered_set<TxnId>> Remove(TxnId ancestor);
  std::string to_string() const;

 private:
  std::unordered_map<TxnId, std::unordered_set<TxnId>> index_;
};

class Scheduler : public slog::NetworkedModule {
 public:
  Scheduler(const std::shared_ptr<Broker>& broker, const std::shared_ptr<Storage>& storage,
            const MetricsRepositoryManagerPtr& metrics_manager,
            std::chrono::milliseconds poll_timeout = slog::kModuleTimeout);

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
  std::unordered_set<TxnId> FindAndResolveSCCs(Vertex& v, std::vector<TxnId>& ready_txns);
  void DispatchSCCs(const std::vector<SCC>& sccs);
  void CheckPendingInquiry(TxnId txn_id);
  void CheckPendingTxns(std::vector<TxnId>&& ready_txns);
  void PrintStats(EnvelopePtr&& env);

  std::unordered_map<TxnId, Transaction*> txns_;

  Graph graph_;
  TarjanSCCsFinder sccs_finder_;
  TxnHorizon execution_horizon_;
  PendingIndex pending_txns_;
  std::unordered_map<TxnId, std::vector<EnvelopePtr>> pending_inquiries_;

  // This must be defined at the end so that the workers exit before any resources
  // in the scheduler is destroyed
  std::vector<std::unique_ptr<slog::ModuleRunner>> workers_;
  int current_worker_;
};

}  // namespace janus