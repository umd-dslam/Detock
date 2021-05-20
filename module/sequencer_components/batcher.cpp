#include "module/sequencer_components/batcher.h"

#include "common/clock.h"
#include "common/proto_utils.h"

using namespace std::chrono;

namespace slog {

using internal::Batch;
using internal::Request;

Batcher::Batcher(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                 const MetricsRepositoryManagerPtr& metrics_manager, milliseconds poll_timeout)
    : NetworkedModule("Batcher", context, config, kBatcherChannel, metrics_manager, poll_timeout),
      batch_id_counter_(0),
      batch_size_(0),
      rg_(std::random_device()()) {
  partitioned_batch_.resize(config->num_partitions());
  NewBatch();
}

bool Batcher::BufferFutureTxn(Transaction* txn) {
  auto timestamp = std::make_pair(txn->internal().timestamp(), txn->internal().coordinating_server());

  std::lock_guard<SpinLatch> guard(future_txns_mut_);
  bool earliest_txn_changed = future_txns_.empty() || txn->internal().timestamp() < future_txns_.begin()->first.first;
  future_txns_.emplace(timestamp, txn);
  return earliest_txn_changed;
}

void Batcher::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kForwardTxn:
      BatchTxn(env->mutable_request()->mutable_forward_txn()->release_txn());
      break;
    case Request::kSignal: {
      if (process_future_txn_callback_id_.has_value()) {
        RemoveTimedCallback(process_future_txn_callback_id_.value());
      }
      ProcessReadyFutureTxns();
      break;
    }
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
      break;
  }
}

void Batcher::ProcessReadyFutureTxns() {
  auto now = slog_clock::now().time_since_epoch().count();

  std::optional<int64_t> earliest_timestamp;
  std::vector<Transaction*> ready_txns;
  {
    std::lock_guard<SpinLatch> guard(future_txns_mut_);
    auto it = future_txns_.begin();
    for (; it != future_txns_.end(); it++) {
      if (it->first.first > now) {
        break;
      }
      ready_txns.push_back(it->second);
    }
    future_txns_.erase(future_txns_.begin(), it);
    if (!future_txns_.empty()) {
      earliest_timestamp = future_txns_.begin()->first.first;
    }
  }

  for (auto txn : ready_txns) {
    txn->mutable_internal()->set_mh_enter_local_batch_time(now);
    BatchTxn(txn);
  }

  process_future_txn_callback_id_.reset();
  if (earliest_timestamp.has_value()) {
    auto delay = duration_cast<microseconds>(nanoseconds(earliest_timestamp.value() - now)) + 1us;
    process_future_txn_callback_id_ = NewTimedCallback(delay, [this]() { ProcessReadyFutureTxns(); });
  }
}

BatchId Batcher::batch_id() const { return batch_id_counter_ * kMaxNumMachines + config()->local_machine_id(); }

void Batcher::NewBatch() {
  ++batch_id_counter_;
  batch_size_ = 0;
  for (auto& batch : partitioned_batch_) {
    if (batch == nullptr) {
      batch.reset(new Batch());
    }
    batch->Clear();
    batch->set_transaction_type(TransactionType::SINGLE_HOME);
    batch->set_id(batch_id());
  }
}

void Batcher::BatchTxn(Transaction* txn) {
  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_LOCAL_BATCH);

  if (txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY) {
    txn = GenerateLockOnlyTxn(txn, config()->local_replica(), true /* in_place */);
  }

  auto num_involved_partitions = txn->internal().involved_partitions_size();
  for (int i = 0; i < num_involved_partitions; ++i) {
    bool in_place = i == (num_involved_partitions - 1);
    auto p = txn->internal().involved_partitions(i);
    auto new_txn = GeneratePartitionedTxn(config(), txn, p, in_place);
    if (new_txn != nullptr) {
      partitioned_batch_[p]->mutable_transactions()->AddAllocated(new_txn);
    }
  }

  ++batch_size_;

  // If this is the first txn in the batch, schedule to send the batch at a later time
  if (batch_size_ == 1) {
    send_batch_callback_id_ = NewTimedCallback(config()->sequencer_batch_duration(), [this]() {
      SendBatch();
      NewBatch();
      send_batch_callback_id_.reset();
    });
  }

  // Batch size is larger than the maximum size, send the batch immediately
  auto max_batch_size = config()->sequencer_max_batch_size();
  if (max_batch_size > 0 && batch_size_ >= max_batch_size) {
    RemoveTimedCallback(send_batch_callback_id_.value());
    send_batch_callback_id_.reset();

    SendBatch();
    NewBatch();
  }
}

void Batcher::SendBatch() {
  VLOG(3) << "Finished batch " << batch_id() << " of size " << batch_size_
          << ". Sending out for ordering and replicating";

  auto local_replica = config()->local_replica();
  auto local_partition = config()->local_partition();
  auto paxos_env = NewEnvelope();
  auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
  paxos_propose->set_value(local_partition);
  Send(move(paxos_env), kLocalPaxos);

  auto num_partitions = config()->num_partitions();

  if (!SendBatchDelayed()) {
    auto num_replicas = config()->num_replicas();
    for (uint32_t part = 0; part < num_partitions; part++) {
      auto env = NewBatchRequest(partitioned_batch_[part].release());

      RECORD(env->mutable_request()->mutable_forward_batch()->mutable_batch_data(),
             TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

      bool send_local = false;
      std::vector<MachineId> destinations;
      destinations.reserve(num_replicas);
      for (uint32_t rep = 0; rep < num_replicas; rep++) {
        if (part == local_partition && rep == local_replica) {
          send_local = true;
        } else {
          destinations.push_back(config()->MakeMachineId(rep, part));
        }
      }
      Send(*env, destinations, kInterleaverChannel);
      if (send_local) {
        Send(move(env), kLocalLogChannel);
      }
    }
  }
}

bool Batcher::SendBatchDelayed() {
  if (!config()->replication_delay_pct()) {
    return false;
  }

  std::bernoulli_distribution is_delayed(config()->replication_delay_pct() / 100.0);
  if (!is_delayed(rg_)) {
    return false;
  }

  auto delay_ms = config()->replication_delay_amount_ms();

  VLOG(3) << "Delay batch " << batch_id() << " for " << delay_ms << " ms";

  for (uint32_t part = 0; part < config()->num_partitions(); part++) {
    auto env = NewBatchRequest(partitioned_batch_[part].release());

    // Send to the partition in the local replica immediately
    if (part == config()->local_partition()) {
      auto copied_env = std::make_unique<internal::Envelope>(*env);
      Send(move(copied_env), kLocalLogChannel);
    } else {
      Send(*env, config()->MakeMachineId(config()->local_replica(), part), kInterleaverChannel);
    }

    NewTimedCallback(milliseconds(delay_ms), [this, part, delayed_env = env.release()]() {
      VLOG(3) << "Sending delayed batch " << delayed_env->request().forward_batch().batch_data().id();
      // Replicate batch to all replicas EXCEPT local replica
      std::vector<MachineId> destinations;
      destinations.reserve(config()->num_replicas());
      for (uint32_t rep = 0; rep < config()->num_replicas(); rep++) {
        if (rep != config()->local_replica()) {
          destinations.push_back(config()->MakeMachineId(rep, part));
        }
      }
      Send(*delayed_env, destinations, kInterleaverChannel);
      delete delayed_env;
    });
  }

  return true;
}

EnvelopePtr Batcher::NewBatchRequest(internal::Batch* batch) {
  auto env = NewEnvelope();
  auto forward_batch = env->mutable_request()->mutable_forward_batch();
  // Minus 1 so that batch id counter starts from 0
  forward_batch->set_same_origin_position(batch_id_counter_ - 1);
  forward_batch->set_allocated_batch_data(batch);
  return env;
}

}  // namespace slog