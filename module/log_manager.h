#pragma once

#include <queue>
#include <unordered_map>

#include "common/batch_log.h"
#include "common/configuration.h"
#include "common/metrics.h"
#include "common/types.h"
#include "module/base/networked_module.h"
#include "proto/transaction.pb.h"

namespace slog {

/**
 * A LocalLog buffers batch data from local partitions and outputs
 * the next local batch in the order given by the local Paxos process
 */
class LocalLog {
 public:
  void AddBatchId(uint32_t queue_id, uint32_t position, BatchId batch_id);
  void AddSlot(SlotId slot_id, uint32_t queue_id, MachineId leader);

  bool HasNextBatch() const;
  std::pair<SlotId, std::pair<BatchId, MachineId>> NextBatch();

  /* For debugging */
  size_t NumBufferedSlots() const { return slots_.NumBufferredItems(); }

  /* For debugging */
  std::unordered_map<uint32_t, size_t> NumBufferedBatchesPerQueue() const {
    std::unordered_map<uint32_t, size_t> queue_sizes;
    for (const auto& [part, log] : batch_queues_) {
      queue_sizes.insert_or_assign(part, log.NumBufferredItems());
    }
    return queue_sizes;
  }

 private:
  void UpdateReadyBatches();

  // Used to decide the next queue to choose a batch from
  AsyncLog<std::pair<uint32_t, MachineId>> slots_;
  // Batches from a partition form a queue
  std::unordered_map<uint32_t, AsyncLog<BatchId>> batch_queues_;
  // Chosen batches
  std::queue<std::pair<SlotId, std::pair<BatchId, MachineId>>> ready_batches_;
};

class LogManager : public NetworkedModule {
 public:
  static uint64_t MakeLogChannel(RegionId region) { return kMaxChannel + region; }

  LogManager(int id, const std::vector<RegionId>& regions, const std::shared_ptr<Broker>& broker,
             const MetricsRepositoryManagerPtr& metrics_manager,
             std::chrono::milliseconds poll_timeout = kModuleTimeout);

  std::string name() const override { return "LogManager"; }

 protected:
  void OnInternalRequestReceived(EnvelopePtr&& env) final;

 private:
  void ProcessBatchReplicationAck(EnvelopePtr&& env);
  void ProcessForwardBatchData(EnvelopePtr&& env);
  void ProcessForwardBatchOrder(EnvelopePtr&& env);
  void AdvanceLog();
  void EmitBatch(BatchPtr&& batch);

  std::unordered_map<RegionId, BatchLog> single_home_logs_;
  LocalLog local_log_;
  std::vector<MachineId> other_partitions_;
  std::vector<MachineId> other_replicas_;
  std::vector<bool> need_ack_from_region_;
};

}  // namespace slog