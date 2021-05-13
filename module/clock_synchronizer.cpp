#include "module/clock_synchronizer.h"

#include "common/clock.h"
#include "common/proto_utils.h"

namespace slog {

ClockSynchronizer::ClockSynchronizer(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                                     const MetricsRepositoryManagerPtr& metrics_manager,
                                     std::chrono::milliseconds poll_timeout_ms)
    : NetworkedModule("ClockSynchronizer", context, config, config->clock_synchronizer_port(),
                      kClockSynchronizerChannel, metrics_manager, poll_timeout_ms) {
  CHECK_NE(config->clock_synchronizer_port(), 0) << "Cannot initialize clock synchronizer with port 0";
  for (uint32_t i = 0; i < config->num_replicas(); i++) {
    latencies_ns_.emplace_back(5);
  }
}

void ClockSynchronizer::Initialize() {
  if (config()->clock_sync_interval() > std::chrono::milliseconds(0)) {
    ScheduleNextSync();
  }
}

void ClockSynchronizer::ScheduleNextSync() {
  NewTimedCallback(config()->clock_sync_interval(), [this] {
    auto p = config()->leader_partition_for_multi_home_ordering();
    for (uint32_t r = 0; r < config()->num_replicas(); r++) {
      auto machine_id = config()->MakeMachineId(r, p);
      if (machine_id == config()->local_machine_id()) {
        continue;
      }
      internal::Envelope env;
      auto ping = env.mutable_request()->mutable_ping();
      ping->set_src_send_time(std::chrono::steady_clock::now().time_since_epoch().count());
      ping->set_dst(r);
      Send(env, machine_id, kClockSynchronizerChannel);
    }
    ScheduleNextSync();
  });
}

void ClockSynchronizer::OnInternalRequestReceived(EnvelopePtr&& env) {
  if (!env->request().has_ping()) {
    LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), internal::Request)
               << "\"";
    return;
  }
  internal::Envelope pong_env;
  auto pong = pong_env.mutable_response()->mutable_pong();
  pong->set_src_send_time(env->request().ping().src_send_time());
  pong->set_dst_send_time(slog_clock::now().time_since_epoch().count());
  pong->set_dst(env->request().ping().dst());
  Send(pong_env, env->from(), kClockSynchronizerChannel);
}

void ClockSynchronizer::OnInternalResponseReceived(EnvelopePtr&& env) {
  auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  auto slog_now = slog_clock::now().time_since_epoch().count();
  const auto& pong = env->response().pong();
  auto& latency_ns = latencies_ns_[pong.dst()];

  latency_ns.Add((now - pong.src_send_time()) / 2);

  auto avg_latency_ns = static_cast<int64_t>(latency_ns.avg());
  auto estimated_remote_clock = pong.dst_send_time() + avg_latency_ns;
  if (estimated_remote_clock > slog_now) {
    slog_clock::offset_ += estimated_remote_clock - slog_now;
  }

  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordClockSync(pong.dst(), pong.src_send_time(), pong.dst_send_time(), now, slog_now,
                                             avg_latency_ns, slog_clock::offset_.load());
  }
}

}  // namespace slog