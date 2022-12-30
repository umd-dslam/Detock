#pragma once

#include <unordered_map>

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/sharder.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "proto/transaction.pb.h"

namespace slog {

class JanusAcceptor : public NetworkedModule {
 public:
  JanusAcceptor(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                const MetricsRepositoryManagerPtr& metrics_manager,
                std::chrono::milliseconds poll_timeout_ms = kModuleTimeout);

  std::string name() const override { return "JanusAcceptor"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  void ProcessPreAccept(EnvelopePtr&& env);

  const SharderPtr sharder_;
  std::unordered_map<Key, TxnId> latest_writing_txns_;
};

}  // namespace slog