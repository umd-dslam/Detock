#pragma once

#include <map>
#include <mutex>

#include "common/spin_latch.h"
#include "module/base/networked_module.h"

namespace slog {

class Batcher : public NetworkedModule {
 public:
  Batcher(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
          const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout);

  // Returns true if the earliest time has changed
  bool BufferFutureTxn(Transaction* txn);

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  using Timestamp = std::pair<int64_t, uint32_t>;

  void ProcessReadyFutureTxns();
  void NewBatch();
  void BatchTxn(Transaction* txn);
  BatchId batch_id() const;
  void SendBatch();
  bool SendBatchDelayed();
  EnvelopePtr NewBatchRequest(internal::Batch* batch);

  void ProcessStatsRequest(const internal::StatsRequest& stats_request);

  SpinLatch future_txns_mut_;
  std::map<Timestamp, Transaction*> future_txns_;
  std::optional<int> process_future_txn_callback_id_;

  std::vector<std::unique_ptr<internal::Batch>> partitioned_batch_;
  BatchId batch_id_counter_;
  int batch_size_;
  std::optional<int> send_batch_callback_id_;
  std::chrono::steady_clock::time_point batch_starting_time_;

  std::mt19937 rg_;
};

}  // namespace slog