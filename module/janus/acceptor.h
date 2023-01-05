#pragma once

#include <unordered_map>

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/sharder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "module/janus/phase.h"
#include "proto/transaction.pb.h"

namespace janus {

using slog::ConfigurationPtr;
using slog::EnvelopePtr;
using slog::MetricsRepositoryManagerPtr;
using slog::Transaction;
using slog::TxnId;

class Acceptor : public slog::NetworkedModule {
 public:
  Acceptor(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
           const MetricsRepositoryManagerPtr& metrics_manager,
           std::chrono::milliseconds poll_timeout_ms = slog::kModuleTimeout);

  std::string name() const override { return "Acceptor"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  void ProcessPreAccept(EnvelopePtr&& env);
  void ProcessAccept(EnvelopePtr&& env);
  void ProcessCommit(EnvelopePtr&& env);
  void PrintStats();

  const slog::SharderPtr sharder_;

  struct AcceptorTxnInfo {
    AcceptorTxnInfo(Transaction* txn) : txn(txn), phase(Phase::PRE_ACCEPT), highest_ballot(0) {}

    Transaction* txn;
    Phase phase;
    int highest_ballot;
  };
  std::unordered_map<TxnId, AcceptorTxnInfo> txns_;

  std::unordered_map<slog::Key, TxnIdAndPartitionsBitmap> latest_writing_txns_;
};

}  // namespace janus