#include "metrics.h"

#include <algorithm>

#include "common/csv_writer.h"
#include "common/proto_utils.h"
#include "common/string_utils.h"
#include "proto/internal.pb.h"

using std::list;
using std::pair;
using std::vector;
using std::chrono::system_clock;

namespace slog {

class Sampler {
  constexpr static uint32_t kSampleMaskSize = 1 << 8;
  using sample_mask_t = std::array<bool, kSampleMaskSize>;

  sample_mask_t sample_mask_;
  vector<uint8_t> sample_count_;

 public:
  Sampler(int sample_rate, size_t num_keys) : sample_count_(num_keys, 0) {
    sample_mask_.fill(false);
    for (uint32_t i = 0; i < sample_rate * kSampleMaskSize / 100; i++) {
      sample_mask_[i] = true;
    }
    auto rd = std::random_device{};
    auto rng = std::default_random_engine{rd()};
    std::shuffle(sample_mask_.begin(), sample_mask_.end(), rng);
  }

  bool IsChosen(size_t key) {
    DCHECK_LT(sample_count_[key], sample_mask_.size());
    return sample_mask_[sample_count_[key]++];
  }
};

class TransactionEventMetrics {
 public:
  TransactionEventMetrics(int sample_rate, uint32_t local_replica, uint32_t local_partition)
      : sampler_(sample_rate, TransactionEvent_descriptor()->value_count()),
        local_replica_(local_replica),
        local_partition_(local_partition) {}

  system_clock::time_point Record(TxnId txn_id, TransactionEvent event) {
    auto now = system_clock::now();
    auto sample_index = static_cast<size_t>(event);
    if (sampler_.IsChosen(sample_index)) {
      txn_events_.push_back({.time = now.time_since_epoch().count(),
                             .replica = local_replica_,
                             .partition = local_partition_,
                             .txn_id = txn_id,
                             .event = event});
    }
    return now;
  }

  struct Data {
    int64_t time;  // nanosecond since epoch
    uint32_t replica;
    uint32_t partition;
    TxnId txn_id;
    TransactionEvent event;
  };

  list<Data>& data() { return txn_events_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter txn_events_csv(dir + "/txn_events.csv", {"txn_id", "event_id", "time", "partition", "replica"});
    CSVWriter event_names_csv(dir + "/event_names.csv", {"id", "event"});
    std::unordered_map<int, string> event_names;
    for (const auto& d : data) {
      txn_events_csv << d.txn_id << static_cast<int>(d.event) << d.time << d.partition << d.replica << csvendl;
      event_names[d.event] = ENUM_NAME(d.event, TransactionEvent);
    }
    for (auto e : event_names) {
      event_names_csv << e.first << e.second << csvendl;
    }
  }

 private:
  Sampler sampler_;
  uint32_t local_replica_;
  uint32_t local_partition_;
  list<Data> txn_events_;
};

class DeadlockResolverRunMetrics {
 public:
  DeadlockResolverRunMetrics(int sample_rate, uint32_t local_replica, uint32_t local_partition)
      : sampler_(sample_rate, 2), local_replica_(local_replica), local_partition_(local_partition) {}

  void Record(int64_t runtime, size_t unstable_graph_sz, size_t stable_graph_sz, size_t deadlocks_resolved) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.time = system_clock::now().time_since_epoch().count(),
                       .partition = local_partition_,
                       .replica = local_replica_,
                       .runtime = runtime,
                       .unstable_graph_sz = unstable_graph_sz,
                       .stable_graph_sz = stable_graph_sz,
                       .deadlocks_resolved = deadlocks_resolved});
    }
  }

  struct Data {
    int64_t time;  // nanosecond since epoch
    uint32_t partition;
    uint32_t replica;
    int64_t runtime;  // nanosecond
    size_t unstable_graph_sz;
    size_t stable_graph_sz;
    size_t deadlocks_resolved;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter deadlock_resolver_csv(
        dir + "/deadlock_resolver.csv",
        {"time", "partition", "replica", "runtime", "unstable_graph_sz", "stable_graph_sz", "deadlocks_resolved"});
    for (const auto& d : data) {
      deadlock_resolver_csv << d.time << d.partition << d.replica << d.runtime << d.unstable_graph_sz
                            << d.stable_graph_sz << d.deadlocks_resolved << csvendl;
    }
  }

 private:
  Sampler sampler_;
  uint32_t local_replica_;
  uint32_t local_partition_;
  list<Data> data_;
};

class DeadlockResolverDeadlockMetrics {
 public:
  DeadlockResolverDeadlockMetrics(int sample_rate, uint32_t local_replica, uint32_t local_partition)
      : sampler_(sample_rate, 2), local_replica_(local_replica), local_partition_(local_partition) {}

  void Record(int num_vertices, const vector<pair<uint64_t, uint64_t>>& edges_removed,
              const vector<pair<uint64_t, uint64_t>>& edges_added) {
    if (sampler_.IsChosen(1)) {
      data_.push_back({.time = system_clock::now().time_since_epoch().count(),
                       .partition = local_partition_,
                       .replica = local_replica_,
                       .num_vertices = num_vertices,
                       .edges_removed = edges_removed,
                       .edges_added = edges_added});
    }
  }

  struct Data {
    int64_t time;  // nanosecond since epoch
    uint32_t partition;
    uint32_t replica;
    int num_vertices;
    vector<pair<uint64_t, uint64_t>> edges_removed;
    vector<pair<uint64_t, uint64_t>> edges_added;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter deadlocks_csv(dir + "/deadlocks.csv", {"time", "partition", "replica", "vertices", "removed", "added"});
    for (const auto& d : data) {
      deadlocks_csv << d.time << d.partition << d.replica << d.num_vertices << Join(d.edges_removed)
                    << Join(d.edges_added) << csvendl;
    }
  }

 private:
  Sampler sampler_;
  uint32_t local_replica_;
  uint32_t local_partition_;
  list<Data> data_;
};

class InterleaverLogs {
 public:
  void Record(uint32_t replica, BatchId batch_id, TxnId txn_id, int64_t enter_sequencer_time,
              int64_t enter_local_batch_time) {
    global_log_.push_back({.replica = replica,
                           .txn_id = txn_id,
                           .batch_id = batch_id,
                           .enter_sequencer_time = enter_sequencer_time,
                           .enter_local_batch_time = enter_local_batch_time});
  }
  struct Data {
    uint32_t replica;
    TxnId txn_id;
    BatchId batch_id;
    int64_t enter_sequencer_time;
    int64_t enter_local_batch_time;
  };
  const vector<Data>& global_log() const { return global_log_; }

  static void WriteToDisk(const std::string& dir, const vector<Data>& global_log) {
    CSVWriter global_log_csv(dir + "/global_log.csv",
                             {"replica", "batch_id", "txn_id", "enter_sequencer", "enter_local_batch"});
    for (const auto& e : global_log) {
      global_log_csv << e.replica << e.batch_id << e.txn_id << e.enter_sequencer_time << e.enter_local_batch_time
                     << csvendl;
    }
  }

 private:
  vector<Data> global_log_;
};

class CrossRegionLatencyProbe {
 public:
  CrossRegionLatencyProbe(int sample_rate): sampler_(sample_rate, 1) {}

  void Record(uint32_t replica, int64_t send_time, int64_t recv_time) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.replica = replica, .send_time = send_time, .recv_time = recv_time });
    }
  }

  struct Data {
    uint32_t replica;
    int64_t send_time;
    int64_t recv_time;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter latency_probe_csv(dir + "/latency_probe.csv", {"replica", "send_time", "recv_time"});
    for (const auto& d : data) {
      latency_probe_csv << d.replica << d.send_time << d.recv_time << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

struct AllMetrics {
  TransactionEventMetrics txn_event_metrics;
  DeadlockResolverRunMetrics deadlock_resolver_run_metrics;
  DeadlockResolverDeadlockMetrics deadlock_resolver_deadlock_metrics;
  InterleaverLogs interleaver_logs;
  CrossRegionLatencyProbe latency_probe;
};

/**
 *  MetricsRepository
 */

MetricsRepository::MetricsRepository(const ConfigurationPtr& config) : config_(config) { Reset(); }

system_clock::time_point MetricsRepository::RecordTxnEvent(TxnId txn_id, TransactionEvent event) {
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
                                                       const vector<pair<uint64_t, uint64_t>>& edges_removed,
                                                       const vector<pair<uint64_t, uint64_t>>& edges_added) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->deadlock_resolver_deadlock_metrics.Record(num_vertices, edges_removed, edges_added);
}

void MetricsRepository::RecordInterleaverLogEntry(uint32_t replica, BatchId batch_id, TxnId txn_id,
                                                  int64_t enter_sequencer_time, int64_t enter_local_batch_time) {
  if (!config_->sample_rate().interleaver_logs()) {
    return;
  }
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->interleaver_logs.Record(replica, batch_id, txn_id, enter_sequencer_time, enter_local_batch_time);
}

void MetricsRepository::RecordLatencyProbe(uint32_t replica, int64_t send_time, int64_t recv_time) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->latency_probe.Record(replica, send_time, recv_time);
}


std::unique_ptr<AllMetrics> MetricsRepository::Reset() {
  std::unique_ptr<AllMetrics> new_metrics(new AllMetrics(
      {.txn_event_metrics = TransactionEventMetrics(config_->sample_rate().txn_event(), config_->local_replica(),
                                                    config_->local_partition()),
       .deadlock_resolver_run_metrics = DeadlockResolverRunMetrics(
           config_->sample_rate().deadlock_resolver_run(), config_->local_replica(), config_->local_partition()),
       .deadlock_resolver_deadlock_metrics = DeadlockResolverDeadlockMetrics(
           config_->sample_rate().deadlock_resolver_deadlock(), config_->local_replica(), config_->local_partition()),
       .interleaver_logs = InterleaverLogs(),
       .latency_probe = CrossRegionLatencyProbe(config_->sample_rate().latency_probe())}));
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
  list<TransactionEventMetrics::Data> txn_events_data;
  list<DeadlockResolverRunMetrics::Data> deadlock_resolver_run_data;
  list<DeadlockResolverDeadlockMetrics::Data> deadlock_resolver_deadlock_data;
  vector<InterleaverLogs::Data> global_log;
  list<CrossRegionLatencyProbe::Data> latency_probe;
  {
    std::lock_guard<std::mutex> guard(mut_);
    for (auto& kv : metrics_repos_) {
      auto metrics = kv.second->Reset();
      txn_events_data.splice(txn_events_data.end(), metrics->txn_event_metrics.data());
      deadlock_resolver_run_data.splice(deadlock_resolver_run_data.end(),
                                        metrics->deadlock_resolver_run_metrics.data());
      deadlock_resolver_deadlock_data.splice(deadlock_resolver_deadlock_data.end(),
                                             metrics->deadlock_resolver_deadlock_metrics.data());
      // There is only one thread with local logs and global log data so there is no need to splice
      if (!metrics->interleaver_logs.global_log().empty()) {
        global_log = metrics->interleaver_logs.global_log();
      }
      latency_probe.splice(latency_probe.end(), metrics->latency_probe.data());
    }
  }

  // Write metrics to disk
  try {
    TransactionEventMetrics::WriteToDisk(dir, txn_events_data);
    DeadlockResolverRunMetrics::WriteToDisk(dir, deadlock_resolver_run_data);
    DeadlockResolverDeadlockMetrics::WriteToDisk(dir, deadlock_resolver_deadlock_data);
    InterleaverLogs::WriteToDisk(dir, global_log);
    CrossRegionLatencyProbe::WriteToDisk(dir, latency_probe);
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