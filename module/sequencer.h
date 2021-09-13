#pragma once

#include <list>
#include <queue>
#include <random>

#include "common/configuration.h"
#include "common/metrics.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/base/networked_module.h"
#include "module/sequencer_components/batcher.h"

namespace slog {

/**
 * A Sequencer batches transactions before sending to the Log Manager.
 *
 * INPUT:  ForwardTxn
 *
 * OUTPUT: For a single-home txn, it is put into a batch. The ID of this batch is
 *         sent to the local paxos process for ordering. Simultaneously, this batch
 *         is sent to the Log Manager of all machines across all regions.
 *
 *         For a multi-home txn, a corresponding lock-only txn is created and then goes
 *         through the same process as a single-home txn above.
 */
class Sequencer : public NetworkedModule {
 public:
  Sequencer(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
            const MetricsRepositoryManagerPtr& metrics_manager,
            std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "Sequencer"; }

 protected:
  void Initialize() final;
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  void ProcessForwardRequest(EnvelopePtr&& env);
  void ProcessPingRequest(EnvelopePtr&& env);

  std::shared_ptr<Batcher> batcher_;
  ModuleRunner batcher_runner_;
};

}  // namespace slog