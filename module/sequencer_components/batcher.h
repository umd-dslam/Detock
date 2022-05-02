#pragma once

#include <map>
#include <mutex>

#include "common/sharder.h"
#include "common/spin_latch.h"
#include "module/base/networked_module.h"

namespace slog {

class Batcher : public NetworkedModule {
 public:
  Batcher(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
          const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout);

  // Returns true if the earliest time has changed
  bool BufferFutureTxn(Transaction* txn);

  std::string name() const override { return "Batcher"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  using Timestamp = std::pair<int64_t, TxnId>;
  using PartitionedBatch = std::vector<std::unique_ptr<internal::Batch>>;

  void ProcessReadyFutureTxns();
  void StartOver();
  void NewBatch();
  void BatchTxn(Transaction* txn);
  BatchId batch_id() const;
  void SendBatches();
  EnvelopePtr NewBatchForwardingMessage(std::vector<internal::Batch*>&& batch, int generator_position);

  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

  const SharderPtr sharder_;
  SpinLatch future_txns_mut_;
  std::map<Timestamp, Transaction*> future_txns_;
  std::optional<Poller::Handle> process_future_txn_callback_handle_;

  std::vector<PartitionedBatch> batches_;
  BatchId batch_id_counter_;
  int current_batch_size_;
  int total_batch_size_;

  std::chrono::steady_clock::time_point batch_starting_time_;

  std::mt19937 rg_;
};

}  // namespace slog