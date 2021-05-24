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
    CSVWriter txn_events_csv(dir + "/events.csv", {"txn_id", "event", "time", "partition", "replica"});
    for (const auto& d : data) {
      txn_events_csv << d.txn_id << ENUM_NAME(d.event, TransactionEvent) << d.time << d.partition << d.replica << csvendl;
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

  void Record(int64_t runtime, size_t unstable_graph_sz, size_t stable_graph_sz, size_t deadlocks_resolved,
              int64_t graph_update_time) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.time = system_clock::now().time_since_epoch().count(),
                       .partition = local_partition_,
                       .replica = local_replica_,
                       .runtime = runtime,
                       .unstable_graph_sz = unstable_graph_sz,
                       .stable_graph_sz = stable_graph_sz,
                       .deadlocks_resolved = deadlocks_resolved,
                       .graph_update_time = graph_update_time});
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
    int64_t graph_update_time;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter deadlock_resolver_csv(dir + "/deadlock_resolver.csv",
                                    {"time", "partition", "replica", "runtime", "unstable_graph_sz", "stable_graph_sz",
                                     "deadlocks_resolved", "graph_update_time"});
    for (const auto& d : data) {
      deadlock_resolver_csv << d.time << d.partition << d.replica << d.runtime << d.unstable_graph_sz
                            << d.stable_graph_sz << d.deadlocks_resolved << d.graph_update_time << csvendl;
    }
  }

 private:
  Sampler sampler_;
  uint32_t local_replica_;
  uint32_t local_partition_;
  list<Data> data_;
};

class DeadlockResolverDeadlockMetrics {
  using edge_t = std::pair<uint64_t, uint64_t>;

 public:
  DeadlockResolverDeadlockMetrics(int sample_rate, bool with_details, uint32_t local_replica, uint32_t local_partition)
      : sampler_(sample_rate, 2),
        with_details_(with_details),
        local_replica_(local_replica),
        local_partition_(local_partition) {}

  void Record(int num_vertices, const vector<edge_t>& edges_removed, const vector<edge_t>& edges_added) {
    if (sampler_.IsChosen(1)) {
      data_.push_back({.time = system_clock::now().time_since_epoch().count(),
                       .partition = local_partition_,
                       .replica = local_replica_,
                       .num_vertices = num_vertices,
                       .edges_removed = (with_details_ ? edges_removed : vector<edge_t>{}),
                       .edges_added = (with_details_ ? edges_added : vector<edge_t>{})});
    }
  }

  struct Data {
    int64_t time;  // nanosecond since epoch
    uint32_t partition;
    uint32_t replica;
    int num_vertices;
    vector<edge_t> edges_removed;
    vector<edge_t> edges_added;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data, bool with_details) {
    if (with_details) {
      CSVWriter deadlocks_csv(dir + "/deadlocks.csv", {"time", "partition", "replica", "vertices", "removed", "added"});
      for (const auto& d : data) {
        deadlocks_csv << d.time << d.partition << d.replica << d.num_vertices << Join(d.edges_removed)
                      << Join(d.edges_added) << csvendl;
      }
    } else {
      CSVWriter deadlocks_csv(dir + "/deadlocks.csv", {"time", "partition", "replica", "vertices"});
      for (const auto& d : data) {
        deadlocks_csv << d.time << d.partition << d.replica << d.num_vertices << csvendl;
      }
    }
  }

 private:
  Sampler sampler_;
  bool with_details_;
  uint32_t local_replica_;
  uint32_t local_partition_;
  list<Data> data_;
};

class InterleaverLogs {
 public:
  void Record(uint32_t replica, BatchId batch_id, TxnId txn_id, int64_t txn_timestamp,
              int64_t mh_depart_from_coordinator_time, int64_t mh_arrive_at_home_time,
              int64_t mh_enter_local_batch_time) {
    global_log_.push_back({.replica = replica,
                           .txn_id = txn_id,
                           .batch_id = batch_id,
                           .txn_timestamp = txn_timestamp,
                           .mh_depart_from_coordinator_time = mh_depart_from_coordinator_time,
                           .mh_arrive_at_home_time = mh_arrive_at_home_time,
                           .mh_enter_local_batch_time = mh_enter_local_batch_time});
  }
  struct Data {
    uint32_t replica;
    TxnId txn_id;
    BatchId batch_id;
    int64_t txn_timestamp;
    int64_t mh_depart_from_coordinator_time;
    int64_t mh_arrive_at_home_time;
    int64_t mh_enter_local_batch_time;
  };
  const vector<Data>& global_log() const { return global_log_; }

  static void WriteToDisk(const std::string& dir, const vector<Data>& global_log) {
    CSVWriter global_log_csv(dir + "/global_log.csv",
                             {"replica", "batch_id", "txn_id", "timestamp", "depart_from_coordinator", "arrive_at_home",
                              "enter_local_batch"});
    for (const auto& e : global_log) {
      global_log_csv << e.replica << e.batch_id << e.txn_id << e.txn_timestamp << e.mh_depart_from_coordinator_time
                     << e.mh_arrive_at_home_time << e.mh_enter_local_batch_time << csvendl;
    }
  }

 private:
  vector<Data> global_log_;
};

class ForwSequLatencyMetrics {
 public:
  ForwSequLatencyMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(uint32_t dst, int64_t send_time, int64_t recv_time, int64_t avg_latency) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.dst = dst, .send_time = send_time, .recv_time = recv_time, .avg_latency = avg_latency});
    }
  }

  struct Data {
    uint32_t dst;
    int64_t send_time;
    int64_t recv_time;
    int64_t avg_latency;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter forw_sequ_latency_csv(dir + "/forw_sequ_latency.csv", {"dst", "send_time", "recv_time", "avg_latency"});
    for (const auto& d : data) {
      forw_sequ_latency_csv << d.dst << d.send_time << d.recv_time << d.avg_latency << csvendl;
    }
  }

 private:
  Sampler sampler_;
  list<Data> data_;
};

class ClockSyncMetrics {
 public:
  ClockSyncMetrics(int sample_rate) : sampler_(sample_rate, 1) {}

  void Record(uint32_t dst, int64_t src_send_time, int64_t dst_send_time, int64_t src_recv_time,
              int64_t local_slog_time, int64_t avg_latency, int64_t new_offset) {
    if (sampler_.IsChosen(0)) {
      data_.push_back({.dst = dst,
                       .src_send_time = src_send_time,
                       .dst_send_time = dst_send_time,
                       .src_recv_time = src_recv_time,
                       .local_slog_time = local_slog_time,
                       .avg_latency = avg_latency,
                       .new_offset = new_offset});
    }
  }

  struct Data {
    uint32_t dst;
    int64_t src_send_time;
    int64_t dst_send_time;
    int64_t src_recv_time;
    int64_t local_slog_time;
    int64_t avg_latency;
    int64_t new_offset;
  };
  list<Data>& data() { return data_; }

  static void WriteToDisk(const std::string& dir, const list<Data>& data) {
    CSVWriter clock_sync_csv(dir + "/clock_sync.csv", {"dst", "src_send_time", "dst_send_time", "src_recv_time",
                                                       "local_slog_time", "avg_latency", "new_offset"});
    for (const auto& d : data) {
      clock_sync_csv << d.dst << d.src_send_time << d.dst_send_time << d.src_recv_time << d.local_slog_time
                     << d.avg_latency << d.new_offset << csvendl;
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
  ForwSequLatencyMetrics forw_sequ_latency_metrics;
  ClockSyncMetrics clock_sync_metrics;
};

/**
 *  MetricsRepository
 */

MetricsRepository::MetricsRepository(const ConfigurationPtr& config) : config_(config) { Reset(); }

system_clock::time_point MetricsRepository::RecordTxnEvent(TxnId txn_id, TransactionEvent event) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->txn_event_metrics.Record(txn_id, event);
}

void MetricsRepository::RecordDeadlockResolverRun(int64_t running_time, size_t unstable_graph_sz,
                                                  size_t stable_graph_sz, size_t deadlocks_resolved,
                                                  int64_t graph_update_time) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->deadlock_resolver_run_metrics.Record(running_time, unstable_graph_sz, stable_graph_sz,
                                                        deadlocks_resolved, graph_update_time);
}

void MetricsRepository::RecordDeadlockResolverDeadlock(int num_vertices,
                                                       const vector<pair<uint64_t, uint64_t>>& edges_removed,
                                                       const vector<pair<uint64_t, uint64_t>>& edges_added) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->deadlock_resolver_deadlock_metrics.Record(num_vertices, edges_removed, edges_added);
}

void MetricsRepository::RecordInterleaverLogEntry(uint32_t replica, BatchId batch_id, TxnId txn_id,
                                                  int64_t txn_timestamp, int64_t mh_depart_from_coordinator_time,
                                                  int64_t mh_arrive_at_home_time, int64_t mh_enter_local_batch_time) {
  if (!config_->metric_options().interleaver_logs()) {
    return;
  }
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->interleaver_logs.Record(replica, batch_id, txn_id, txn_timestamp, mh_depart_from_coordinator_time,
                                           mh_arrive_at_home_time, mh_enter_local_batch_time);
}

void MetricsRepository::RecordForwSequLatency(uint32_t replica, int64_t send_time, int64_t recv_time,
                                              int64_t avg_latency) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->forw_sequ_latency_metrics.Record(replica, send_time, recv_time, avg_latency);
}

void MetricsRepository::RecordClockSync(uint32_t dst, int64_t src_send_time, int64_t dst_send_time,
                                        int64_t src_recv_time, int64_t local_slog_time, int64_t avg_latency,
                                        int64_t new_offset) {
  std::lock_guard<SpinLatch> guard(latch_);
  return metrics_->clock_sync_metrics.Record(dst, src_send_time, dst_send_time, src_recv_time, local_slog_time,
                                             avg_latency, new_offset);
}

std::unique_ptr<AllMetrics> MetricsRepository::Reset() {
  std::unique_ptr<AllMetrics> new_metrics(new AllMetrics(
      {.txn_event_metrics = TransactionEventMetrics(config_->metric_options().txn_events_sample(),
                                                    config_->local_replica(), config_->local_partition()),
       .deadlock_resolver_run_metrics =
           DeadlockResolverRunMetrics(config_->metric_options().deadlock_resolver_runs_sample(),
                                      config_->local_replica(), config_->local_partition()),
       .deadlock_resolver_deadlock_metrics =
           DeadlockResolverDeadlockMetrics(config_->metric_options().deadlock_resolver_deadlocks_sample(),
                                           config_->metric_options().deadlock_resolver_deadlock_details(),
                                           config_->local_replica(), config_->local_partition()),
       .interleaver_logs = InterleaverLogs(),
       .forw_sequ_latency_metrics = ForwSequLatencyMetrics(config_->metric_options().forw_sequ_latency_sample()),
       .clock_sync_metrics = ClockSyncMetrics(config_->metric_options().clock_sync_sample())}));

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
  list<ForwSequLatencyMetrics::Data> forw_sequ_latency_data;
  list<ClockSyncMetrics::Data> clock_sync_data;
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
      forw_sequ_latency_data.splice(forw_sequ_latency_data.end(), metrics->forw_sequ_latency_metrics.data());
      clock_sync_data.splice(clock_sync_data.end(), metrics->clock_sync_metrics.data());
    }
  }

  // Write metrics to disk
  try {
    TransactionEventMetrics::WriteToDisk(dir, txn_events_data);
    DeadlockResolverRunMetrics::WriteToDisk(dir, deadlock_resolver_run_data);
    DeadlockResolverDeadlockMetrics::WriteToDisk(dir, deadlock_resolver_deadlock_data,
                                                 config_->metric_options().deadlock_resolver_deadlock_details());
    InterleaverLogs::WriteToDisk(dir, global_log);
    ForwSequLatencyMetrics::WriteToDisk(dir, forw_sequ_latency_data);
    ClockSyncMetrics::WriteToDisk(dir, clock_sync_data);
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