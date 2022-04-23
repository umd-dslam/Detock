#pragma once

#include <unordered_map>

#include "common/clock.h"
#include "common/rolling_window.h"
#include "module/base/networked_module.h"

namespace slog {

class ClockSynchronizer : public NetworkedModule {
 public:
  ClockSynchronizer(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                    const MetricsRepositoryManagerPtr& metrics_manager,
                    std::chrono::milliseconds poll_timeout_ms = kModuleTimeout);

  std::string name() const override { return "ClockSynchronizer"; }

 protected:
  void Initialize() final;
  void OnInternalRequestReceived(EnvelopePtr&& env) final;
  void OnInternalResponseReceived(EnvelopePtr&& env) final;

 private:
  void ScheduleNextSync();

  std::unordered_map<MachineId, RollingWindow<int64_t>> latencies_ns_;
};

}  // namespace slog