#include "common/batch_log.h"

#include <glog/logging.h>

#include "common/metrics.h"

using std::make_pair;
using std::move;

namespace slog {

BatchLog::BatchLog() {}

void BatchLog::AddBatch(BatchPtr&& batch) {
  auto batch_id = batch->id();
  batches_.insert_or_assign(batch_id, move(batch));
  UpdateReadyBatches();
}

void BatchLog::AckReplication(BatchId batch_id) {
  replication_[batch_id]--;
  UpdateReadyBatches();
}

void BatchLog::AddSlot(SlotId slot_id, BatchId batch_id, int replication_factor) {
  auto time = std::chrono::system_clock::now().time_since_epoch().count();
  slots_.Insert(slot_id, {batch_id, time});
  replication_[batch_id] += replication_factor;
  UpdateReadyBatches();
}

bool BatchLog::HasNextBatch() const { return !ready_batches_.empty(); }

std::pair<SlotId, BatchPtr> BatchLog::NextBatch() {
  if (!HasNextBatch()) {
    throw std::runtime_error("NextBatch() was called when there is no ready batch");
  }
  auto next_slot = ready_batches_.front().first;
  auto [next_batch_id, time] = ready_batches_.front().second;
  ready_batches_.pop();

  auto it = batches_.find(next_batch_id);

  RECORD_WITH_TIME(it->second.get(), TransactionEvent::ENTER_LOG_MANAGER_ORDER, time);

  auto res = make_pair(move(next_slot), move(it->second));
  batches_.erase(it);

  return res;
}

void BatchLog::UpdateReadyBatches() {
  while (slots_.HasNext()) {
    auto next_batch_id = slots_.Peek().first;
    auto batch_it = batches_.find(next_batch_id);
    auto rep_it = replication_.find(next_batch_id);
    if (batch_it != batches_.end() && rep_it != replication_.end() && rep_it->second == 0) {
      replication_.erase(rep_it);
      ready_batches_.push(slots_.Next());
    } else {
      break;
    }
  }
}

}  // namespace slog