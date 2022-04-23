#pragma once

#include <atomic>
#include <list>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>
#include <vector>

#include "common/configuration.h"
#include "common/spin_latch.h"
#include "glog/logging.h"
#include "proto/transaction.pb.h"

namespace slog {

struct AllMetrics;

/**
 * Repository of metrics per thread
 */
class MetricsRepository {
 public:
  MetricsRepository(const ConfigurationPtr& config);

  std::chrono::system_clock::time_point RecordTxnEvent(TxnId txn_id, TransactionEvent event);
  void RecordDeadlockResolverRun(int64_t runtime, size_t unstable_graph_sz, size_t stable_graph_sz,
                                 size_t deadlocks_resolved, int64_t graph_update_time);
  void RecordDeadlockResolverDeadlock(int num_vertices, const std::vector<std::pair<uint64_t, uint64_t>>& edges_removed,
                                      const std::vector<std::pair<uint64_t, uint64_t>>& edges_added);
  void RecordLogManagerEntry(uint32_t region, BatchId batch_id, TxnId txn_id, int64_t txn_timestamp,
                             int64_t exit_forwarder_time, int64_t enter_sequencer_time, int64_t enter_local_batch_time);
  void RecordForwSequLatency(uint32_t region, int64_t src_time, int64_t dst_time, int64_t src_recv_time,
                             int64_t avg_time);
  void RecordClockSync(uint32_t dst, int64_t src_time, int64_t dst_time, int64_t src_recv_time, int64_t local_slog_time,
                       int64_t avg_latency, int64_t new_offset);
  void RecordForwarderBatch(size_t batch_size, int64_t batch_duration);
  void RecordSequencerBatch(BatchId batch_id, size_t batch_size, int64_t batch_duration);
  void RecordMHOrdererBatch(BatchId batch_id, size_t batch_size, int64_t batch_duration);
  void RecordTxnTimestamp(TxnId txn_id, uint32_t from, int64_t txn_timestamp, int64_t server_time);
  void RecordGeneric(int type, int64_t time, int64_t data);

  std::unique_ptr<AllMetrics> Reset();

 private:
  const ConfigurationPtr config_;
  SpinLatch latch_;

  std::unique_ptr<AllMetrics> metrics_;
};

extern thread_local std::shared_ptr<MetricsRepository> per_thread_metrics_repo;

/**
 * Handles thread registering, aggregates results, and output results to files
 */
class MetricsRepositoryManager {
 public:
  MetricsRepositoryManager(const std::string& config_name, const ConfigurationPtr& config);
  void RegisterCurrentThread();
  void AggregateAndFlushToDisk(const std::string& dir);

 private:
  const std::string config_name_;
  const ConfigurationPtr config_;
  std::unordered_map<std::thread::id, std::shared_ptr<MetricsRepository>> metrics_repos_;
  std::mutex mut_;
};

using MetricsRepositoryManagerPtr = std::shared_ptr<MetricsRepositoryManager>;

/**
 * Initialization and helper functions
 */

extern uint32_t gLocalMachineId;
extern uint64_t gEnabledEvents;

void InitializeRecording(const ConfigurationPtr& config);

template <typename TxnOrBatchInternalPtr>
inline void RecordTxnEvent(TxnOrBatchInternalPtr txn_internal, TransactionEvent event, int64_t time = -1) {
  if (!((gEnabledEvents >> event) & 1)) {
    return;
  }
  TxnId txn_id = 0;
  if (txn_internal != nullptr) {
    if (time < 0) {
      time = std::chrono::system_clock::now().time_since_epoch().count();
    }
    auto new_event = txn_internal->mutable_events()->Add();
    new_event->set_event(event);
    new_event->set_time(time);
    new_event->set_machine(gLocalMachineId);
    new_event->set_home(-1);
    txn_id = txn_internal->id();
  }
  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordTxnEvent(txn_id, event);
  }
}

#ifdef ENABLE_TXN_EVENT_RECORDING
#define INIT_RECORDING(config) slog::InitializeRecording(config)
#define RECORD(txn, event) RecordTxnEvent(txn, event)
#define RECORD_WITH_TIME(txn, event, t) RecordTxnEvent(txn, event, t)
#else
#define INIT_RECORDING(config)
#define RECORD(txn, event)
#define RECORD_WITH_TIME(txn, event, t)
#endif

// Helper function for quickly monitor throughput at a certain place
// TODO: use thread_local instead of static
#define MONITOR_THROUGHPUT()                                                                                \
  static int TP_COUNTER = 0;                                                                                \
  static int TP_LAST_COUNTER = 0;                                                                           \
  static std::chrono::steady_clock::time_point TP_LAST_LOG_TIME;                                            \
  TP_COUNTER++;                                                                                             \
  auto TP_LOG_SPAN = std::chrono::steady_clock::now() - TP_LAST_LOG_TIME;                                   \
  if (TP_LOG_SPAN > 1s) {                                                                                   \
    LOG(INFO) << "Throughput: "                                                                             \
              << (TP_COUNTER - TP_LAST_COUNTER) / std::chrono::duration_cast<seconds>(TP_LOG_SPAN).count(); \
    TP_LAST_COUNTER = TP_COUNTER;                                                                           \
    TP_LAST_LOG_TIME = std::chrono::steady_clock::now();                                                    \
  }

}  // namespace slog