#include "interleaver.h"

#include <glog/logging.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "proto/internal.pb.h"

using std::pair;

namespace slog {

using internal::Request;
using internal::Response;

void LocalLog::AddBatchId(
    uint32_t queue_id, uint32_t position, BatchId batch_id) {
  batch_queues_[queue_id].Insert(position, batch_id);
  UpdateReadyBatches();
}

void LocalLog::AddSlot(SlotId slot_id, uint32_t queue_id) {
  slots_.Insert(slot_id, queue_id);
  UpdateReadyBatches();
}

bool LocalLog::HasNextBatch() const {
  return !ready_batches_.empty();
}

pair<SlotId, BatchId> LocalLog::NextBatch() {
  if (!HasNextBatch()) {
    throw std::runtime_error("NextBatch() was called when there is no batch");
  }
  auto next_batch = ready_batches_.front();
  ready_batches_.pop();
  return next_batch;
}

void LocalLog::UpdateReadyBatches() {
  while (slots_.HasNext()) {
    auto next_queue_id = slots_.Peek();
    if (batch_queues_.count(next_queue_id) == 0) {
      break;
    }
    auto& next_queue = batch_queues_.at(next_queue_id);
    if (!next_queue.HasNext()) {
      break;
    }
    auto slot_id = slots_.Next().first;
    auto batch_id = next_queue.Next().second;
    ready_batches_.emplace(slot_id, batch_id);
  }
}

Interleaver::Interleaver(
    const ConfigurationPtr& config,
    const shared_ptr<Broker>& broker) :
    NetworkedModule(broker, INTERLEAVER_CHANNEL),
    config_(config),
    global_log_id(0) {}

void Interleaver::HandleInternalRequest(internal::Request&& req, string&& from_machine_id) {
  if (req.type_case() == Request::kLocalQueueOrder) {
    auto& order = req.local_queue_order();
    VLOG(1) << "Received local queue order. Slot id: "
            << order.slot() << ". Queue id: " << order.queue_id(); 

    local_log_.AddSlot(order.slot(), order.queue_id());

  } else if (req.type_case() == Request::kForwardBatch) {
    auto forward_batch = req.mutable_forward_batch();
    auto machine_id = from_machine_id.empty() 
        ? config_->GetLocalMachineIdAsProto() 
        : MakeMachineId(from_machine_id);
    auto from_replica = machine_id.replica();

    switch (forward_batch->part_case()) {
      case internal::ForwardBatch::kBatchData: {
        auto batch = forward_batch->mutable_batch_data();

        // This must be a SINGLE_HOME batch
        DCHECK_EQ(batch->transaction_type(), TransactionType::SINGLE_HOME);

        VLOG(1) << "Received data for SINGLE-HOME batch " << batch->id()
            << " from [" << from_machine_id
            << "]. Number of txns: " << batch->transactions_size();

        RecordTxnEvent(
            config_,
            batch,
            TransactionEvent::ENTER_INTERLEAVER_IN_BATCH);

        if (from_replica == config_->GetLocalReplica()) {
          local_log_.AddBatchId(
              machine_id.partition() /* queue_id */,
              // Batches generated by the same machine need to follow the order
              // of creation. This field is used to keep track of that order
              forward_batch->same_origin_position(),
              batch->id());
        } else {
          remote_logs_[from_replica].AddBatch(batch->id());
        }

        RecordTxnEvent(
            config_,
            batch,
            TransactionEvent::EXIT_INTERLEAVER_IN_BATCH);

        // Forward the batch data to the scheduler
        Send(std::move(req), SCHEDULER_CHANNEL);

        break;
      }
      case internal::ForwardBatch::kBatchOrder: {
        auto& batch_order = forward_batch->batch_order();

        VLOG(1) << "Received order for batch " << batch_order.batch_id()
                << " from [" << from_machine_id << "]. Slot: " << batch_order.slot();

        remote_logs_[from_replica].AddSlot(
            batch_order.slot(),
            batch_order.batch_id());
        break;
      }
      default:
        break;
    }
  }

  AdvanceLogs();
}

void Interleaver::AdvanceLogs() {
  // Advance local log
  auto local_partition = config_->GetLocalPartition();
  while (local_log_.HasNextBatch()) {
    auto next_batch = local_log_.NextBatch();
    auto slot_id = next_batch.first;
    auto batch_id = next_batch.second;

    // Replicate the batch and slot id to the corresponding partition in other regions
    Request request;
    auto forward_batch_order = request.mutable_forward_batch()->mutable_batch_order();
    forward_batch_order->set_batch_id(batch_id);
    forward_batch_order->set_slot(slot_id);
    auto num_replicas = config_->GetNumReplicas();
    for (uint32_t rep = 0; rep < num_replicas; rep++) {
      Send(
          request,
          INTERLEAVER_CHANNEL,
          MakeMachineIdAsString(rep, local_partition));
    }

    EmitNextBatchInGlobalLog(batch_id);
  }

  // Advance remote logs
  for (auto& pair : remote_logs_) {
    auto& log = pair.second;
    while (log.HasNextBatch()) {
      EmitNextBatchInGlobalLog(log.NextBatchId().second);
    }
  }
}

void Interleaver::EmitNextBatchInGlobalLog(BatchId batch_id) {
  Request request;
  auto batch_order = request.mutable_forward_batch()->mutable_batch_order();
  batch_order->set_batch_id(batch_id);
  batch_order->set_slot(global_log_id);
  Send(request, SCHEDULER_CHANNEL);

  global_log_id++;
}

} // namespace slog