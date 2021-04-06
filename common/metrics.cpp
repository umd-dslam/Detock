#include "metrics.h"

#include <algorithm>
#include <random>

#include "common/csv_writer.h"
#include "common/proto_utils.h"
#include "proto/internal.pb.h"

namespace slog {

/**
 *  MetricsRepository
 */

MetricsRepository::MetricsRepository(const ConfigurationPtr& config, const sample_mask_t& sample_mask)
    : config_(config), sample_mask_(sample_mask) {
  Reset();
}

std::chrono::system_clock::time_point MetricsRepository::RecordTxnEvent(TransactionEvent event) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->txn_event_metrics.RecordEvent(event);
}

void MetricsRepository::RecordDeadlockResolver(int64_t runtime, size_t unstable_total_graph_sz,
                                               size_t stable_total_graph_sz, size_t local_graph_sz,
                                               size_t stable_local_graph_sz, size_t deadlocks_resolved) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->deadlock_resolver_metrics.Record(runtime, unstable_total_graph_sz, stable_total_graph_sz,
                                                    local_graph_sz, stable_local_graph_sz, deadlocks_resolved);
}

std::unique_ptr<MetricsRepository::AllMetrics> MetricsRepository::Reset() {
  std::unique_ptr<MetricsRepository::AllMetrics> new_metrics(new MetricsRepository::AllMetrics(
      {.txn_event_metrics = TransactionEventMetrics(sample_mask_, config_->local_replica(), config_->local_partition()),
       .deadlock_resolver_metrics =
           DeadlockResolverMetrics(sample_mask_, config_->local_replica(), config_->local_partition())}));
  std::lock_guard<SpinLatch> guard(latch_);
  metrics_.swap(new_metrics);
  return new_metrics;
}

thread_local std::shared_ptr<MetricsRepository> per_thread_metrics_repo;

/**
 *  MetricsRepositoryManager
 */

MetricsRepositoryManager::MetricsRepositoryManager(const ConfigurationPtr& config) : config_(config) {
  sample_mask_.fill(false);
  for (uint32_t i = 0; i < config_->sample_rate() * kSampleMaskSize / 100; i++) {
    sample_mask_[i] = true;
  }
  auto rd = std::random_device{};
  auto rng = std::default_random_engine{rd()};
  std::shuffle(sample_mask_.begin(), sample_mask_.end(), rng);
}

void MetricsRepositoryManager::RegisterCurrentThread() {
  std::lock_guard<std::mutex> guard(mut_);
  const auto thread_id = std::this_thread::get_id();
  auto ins = metrics_repos_.try_emplace(thread_id, config_, new MetricsRepository(config_, sample_mask_));
  per_thread_metrics_repo = ins.first->second;
}

void MetricsRepositoryManager::AggregateAndFlushToDisk(const std::string& dir) {
  // Aggregate metrics
  std::list<TransactionEventMetrics::Data> txn_events_data;
  std::list<DeadlockResolverMetrics::Data> deadlock_resolver_data;
  {
    std::lock_guard<std::mutex> guard(mut_);
    for (auto& kv : metrics_repos_) {
      auto metrics = kv.second->Reset();
      txn_events_data.splice(txn_events_data.end(), metrics->txn_event_metrics.data());
      deadlock_resolver_data.splice(deadlock_resolver_data.end(), metrics->deadlock_resolver_metrics.data());
    }
  }

  // Write metrics to disk
  try {
    CSVWriter txn_events_csv(dir + "/txn_events.csv", {"event_id", "time", "partition", "replica"});
    CSVWriter event_names_csv(dir + "/event_names.csv", {"id", "event"});
    std::unordered_map<int, string> event_names;
    for (const auto& data : txn_events_data) {
      txn_events_csv << static_cast<int>(data.event) << data.time << data.partition << data.replica << csvendl;
      event_names[data.event] = ENUM_NAME(data.event, TransactionEvent);
    }
    for (auto e : event_names) {
      event_names_csv << e.first << e.second << csvendl;
    }

    CSVWriter deadlock_resolver_csv(
        dir + "/deadlock_resolver.csv",
        {"time", "partition", "replica", "runtime", "unstable_total_graph_sz", "stable_total_graph_sz",
         "unstable_local_graph_sz", "stable_local_graph_sz", "deadlocks_resolved"});
    for (const auto& data : deadlock_resolver_data) {
      deadlock_resolver_csv << data.time << data.partition << data.replica << data.runtime
                            << data.unstable_total_graph_sz << data.stable_total_graph_sz
                            << data.unstable_local_graph_sz << data.stable_local_graph_sz << data.deadlocks_resolved
                            << csvendl;
    }

    LOG(INFO) << "Metrics written to: \"" << dir << "/\"";
  } catch (std::runtime_error& e) {
    LOG(ERROR) << e.what();
  }
}

/**
 * Initialization
 */

uint32_t gLocalMachineId = 0;
uint64_t gDisabledEvents = 0;

void InitializeRecording(const ConfigurationPtr& config) {
  gLocalMachineId = config->local_machine_id();
  auto events = config->disabled_events();
  for (auto e : events) {
    if (e == TransactionEvent::ALL) {
      gDisabledEvents = ~0;
      return;
    }
    gDisabledEvents |= (1 << e);
  }
}

}  // namespace slog