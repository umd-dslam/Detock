#include "module/clock_synchronizer.h"

#include "common/clock.h"
#include "common/proto_utils.h"

namespace slog {

ClockSynchronizer::ClockSynchronizer(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                                     const MetricsRepositoryManagerPtr& metrics_manager,
                                     std::chrono::milliseconds poll_timeout_ms)
    : NetworkedModule(context, config, config->clock_synchronizer_port(), kClockSynchronizerChannel, metrics_manager,
                      poll_timeout_ms) {
  CHECK_NE(config->clock_synchronizer_port(), 0) << "Cannot initialize clock synchronizer with port 0";

  size_t avg_window;
  if (config->clock_sync_interval().count() == 0) {
    avg_window = 1;
  } else {
    avg_window = std::max(2, 1000 / static_cast<int>(config->clock_sync_interval().count()));
  }

  for (int p = 0; p < config->num_partitions(); p++) {
    auto m = MakeMachineId(config->local_region(), config->local_replica(), p);
    if (m != config->local_machine_id()) {
      latencies_ns_.emplace(m, avg_window);
    }
  }

  auto leader_partition = config->leader_partition_for_multi_home_ordering();
  if (config->local_partition() == leader_partition) {
    for (int r = 0; r < config->num_regions(); r++) {
      auto m = MakeMachineId(r, 0, leader_partition);
      if (m != config->local_machine_id()) {
        latencies_ns_.emplace(m, avg_window);
      }
    }
  }
}

void ClockSynchronizer::Initialize() {
  if (config()->clock_sync_interval() > std::chrono::milliseconds(0)) {
    ScheduleNextSync();
  }
}

void ClockSynchronizer::ScheduleNextSync() {
  NewTimedCallback(config()->clock_sync_interval(), [this] {
    for (auto kv : latencies_ns_) {
      internal::Envelope env;
      auto ping = env.mutable_request()->mutable_ping();
      ping->set_src_time(std::chrono::steady_clock::now().time_since_epoch().count());
      ping->set_dst(kv.first);
      Send(env, kv.first, kClockSynchronizerChannel);
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
  pong->set_src_time(env->request().ping().src_time());
  pong->set_dst_time(slog_clock::now().time_since_epoch().count());
  pong->set_dst(env->request().ping().dst());
  Send(pong_env, env->from(), kClockSynchronizerChannel);
}

void ClockSynchronizer::OnInternalResponseReceived(EnvelopePtr&& env) {
  auto now = std::chrono::steady_clock::now().time_since_epoch().count();
  auto slog_now = slog_clock::now().time_since_epoch().count();

  const auto& pong = env->response().pong();
  auto it = latencies_ns_.find(pong.dst());
  if (it == latencies_ns_.end()) {
    LOG(ERROR) << "Invalid clock sync peer: " << MACHINE_ID_STR(pong.dst());
    return;
  }

  auto& latency_ns = it->second;
  latency_ns.Add((now - pong.src_time()) / 2);

  auto avg_latency_ns = static_cast<int64_t>(latency_ns.avg());
  auto estimated_remote_clock = pong.dst_time() + avg_latency_ns;
  if (estimated_remote_clock > slog_now) {
    slog_clock::offset_ += estimated_remote_clock - slog_now;
  }

  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordClockSync(pong.dst(), pong.src_time(), pong.dst_time(), now, slog_now,
                                             avg_latency_ns, slog_clock::offset_.load());
  }
}

}  // namespace slog