#include "metrics.h"

#include <algorithm>

#include "common/csv_writer.h"
#include "common/proto_utils.h"
#include "common/string_utils.h"
#include "proto/internal.pb.h"

namespace slog {

/**
 *  MetricsRepository
 */

MetricsRepository::MetricsRepository(const ConfigurationPtr& config) : config_(config) { Reset(); }

std::chrono::system_clock::time_point MetricsRepository::RecordTxnEvent(TxnId txn_id, TransactionEvent event) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->txn_event_metrics.Record(txn_id, event);
}

void MetricsRepository::RecordDeadlockResolverRun(int64_t runtime, size_t unstable_graph_sz, size_t stable_graph_sz,
                                                  size_t deadlocks_resolved) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->deadlock_resolver_run_metrics.Record(runtime, unstable_graph_sz, stable_graph_sz,
                                                        deadlocks_resolved);
}

void MetricsRepository::RecordDeadlockResolverDeadlock(int num_vertices,
                                                       const std::vector<std::pair<uint64_t, uint64_t>>& edges_removed,
                                                       const std::vector<std::pair<uint64_t, uint64_t>>& edges_added) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->deadlock_resolver_deadlock_metrics.Record(num_vertices, edges_removed, edges_added);
}

std::unique_ptr<MetricsRepository::AllMetrics> MetricsRepository::Reset() {
  std::unique_ptr<MetricsRepository::AllMetrics> new_metrics(new MetricsRepository::AllMetrics(
      {.txn_event_metrics = TransactionEventMetrics(config_->sample_rate().txn_event(), config_->local_replica(),
                                                    config_->local_partition()),
       .deadlock_resolver_run_metrics = DeadlockResolverRunMetrics(
           config_->sample_rate().deadlock_resolver_run(), config_->local_replica(), config_->local_partition()),
       .deadlock_resolver_deadlock_metrics =
           DeadlockResolverDeadlockMetrics(config_->sample_rate().deadlock_resolver_deadlock(),
                                           config_->local_replica(), config_->local_partition())}));
  std::lock_guard<SpinLatch> guard(latch_);
  metrics_.swap(new_metrics);
  return new_metrics;
}

thread_local std::shared_ptr<MetricsRepository> per_thread_metrics_repo;

/**
 *  MetricsRepositoryManager
 */

MetricsRepositoryManager::MetricsRepositoryManager(const ConfigurationPtr& config) : config_(config) {}

void MetricsRepositoryManager::RegisterCurrentThread() {
  std::lock_guard<std::mutex> guard(mut_);
  const auto thread_id = std::this_thread::get_id();
  auto ins = metrics_repos_.try_emplace(thread_id, config_, new MetricsRepository(config_));
  per_thread_metrics_repo = ins.first->second;
}

void MetricsRepositoryManager::AggregateAndFlushToDisk(const std::string& dir) {
  // Aggregate metrics
  std::list<TransactionEventMetrics::Data> txn_events_data;
  std::list<DeadlockResolverRunMetrics::Data> deadlock_resolver_run_data;
  std::list<DeadlockResolverDeadlockMetrics::Data> deadlock_resolver_deadlock_data;
  {
    std::lock_guard<std::mutex> guard(mut_);
    for (auto& kv : metrics_repos_) {
      auto metrics = kv.second->Reset();
      txn_events_data.splice(txn_events_data.end(), metrics->txn_event_metrics.data());
      deadlock_resolver_run_data.splice(deadlock_resolver_run_data.end(),
                                        metrics->deadlock_resolver_run_metrics.data());
      deadlock_resolver_deadlock_data.splice(deadlock_resolver_deadlock_data.end(),
                                             metrics->deadlock_resolver_deadlock_metrics.data());
    }
  }

  // Write metrics to disk
  try {
    CSVWriter txn_events_csv(dir + "/txn_events.csv", {"txn_id", "event_id", "time", "partition", "replica"});
    CSVWriter event_names_csv(dir + "/event_names.csv", {"id", "event"});
    std::unordered_map<int, string> event_names;
    for (const auto& data : txn_events_data) {
      txn_events_csv << data.txn_id << static_cast<int>(data.event) << data.time << data.partition << data.replica
                     << csvendl;
      event_names[data.event] = ENUM_NAME(data.event, TransactionEvent);
    }
    for (auto e : event_names) {
      event_names_csv << e.first << e.second << csvendl;
    }

    CSVWriter deadlock_resolver_csv(
        dir + "/deadlock_resolver.csv",
        {"time", "partition", "replica", "runtime", "unstable_graph_sz", "stable_graph_sz", "deadlocks_resolved"});
    for (const auto& data : deadlock_resolver_run_data) {
      deadlock_resolver_csv << data.time << data.partition << data.replica << data.runtime << data.unstable_graph_sz
                            << data.stable_graph_sz << data.deadlocks_resolved << csvendl;
    }

    CSVWriter deadlocks_csv(dir + "/deadlocks.csv", {"time", "partition", "replica", "vertices", "removed", "added"});
    for (const auto& data : deadlock_resolver_deadlock_data) {
      deadlocks_csv << data.time << data.partition << data.replica << data.num_vertices << Join(data.edges_removed)
                    << Join(data.edges_added) << csvendl;
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