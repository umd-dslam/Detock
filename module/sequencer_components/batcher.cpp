#include "module/sequencer_components/batcher.h"

#include "common/clock.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "module/log_manager.h"

using namespace std::chrono;

namespace slog {

using internal::Batch;
using internal::Request;

Batcher::Batcher(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                 const MetricsRepositoryManagerPtr& metrics_manager, milliseconds poll_timeout)
    : NetworkedModule(context, config, kBatcherChannel, metrics_manager, poll_timeout, true /* is_long_sender */),
      sharder_(Sharder::MakeSharder(config)),
      batch_id_counter_(0),
      rg_(std::random_device()()) {
  StartOver();
}

bool Batcher::BufferFutureTxn(Transaction* txn) {
  auto timestamp = std::make_pair(txn->internal().timestamp(), txn->internal().id());

  std::lock_guard<SpinLatch> guard(future_txns_mut_);
  bool earliest_txn_changed = future_txns_.empty() || txn->internal().timestamp() < future_txns_.begin()->first.first;
  auto res = future_txns_.emplace(timestamp, txn);
  CHECK(res.second) << "Conflicting timestamps: (" << timestamp.first << ", " << timestamp.second << ")";
  return earliest_txn_changed;
}

void Batcher::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kForwardTxn:
      BatchTxn(env->mutable_request()->mutable_forward_txn()->release_txn());
      break;
    case Request::kSignal: {
      if (process_future_txn_callback_handle_.has_value()) {
        RemoveTimedCallback(process_future_txn_callback_handle_.value());
      }
      ProcessReadyFutureTxns();
      break;
    }
    case Request::kStats:
      ProcessStatsRequest(request->stats());
      break;
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

  process_future_txn_callback_handle_.reset();
  if (earliest_timestamp.has_value()) {
    auto delay = duration_cast<microseconds>(nanoseconds(earliest_timestamp.value() - now)) + 1us;
    process_future_txn_callback_handle_ = NewTimedCallback(delay, [this]() { ProcessReadyFutureTxns(); });
  }
}

BatchId Batcher::batch_id() const { return (batch_id_counter_ << kMachineIdBits) | config()->local_machine_id(); }

void Batcher::StartOver() {
  total_batch_size_ = 0;
  batches_.clear();
  NewBatch();
}

void Batcher::NewBatch() {
  current_batch_size_ = 0;
  ++batch_id_counter_;

  PartitionedBatch new_batch(config()->num_partitions());
  for (auto& partition : new_batch) {
    partition.reset(new Batch());
    partition->set_transaction_type(TransactionType::SINGLE_HOME);
    partition->set_id(batch_id());
  }

  batches_.push_back(std::move(new_batch));
}

void Batcher::BatchTxn(Transaction* txn) {
  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_LOCAL_BATCH);

  if (txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY) {
    txn = GenerateLockOnlyTxn(txn, config()->local_region(), true /* in_place */);
  }

  auto& current_batch = batches_.back();
  auto num_involved_partitions = txn->internal().involved_partitions_size();
  for (int i = 0; i < num_involved_partitions; ++i) {
    bool in_place = i == (num_involved_partitions - 1);
    auto p = txn->internal().involved_partitions(i);
    auto new_txn = GeneratePartitionedTxn(sharder_, txn, p, in_place);
    if (new_txn != nullptr) {
      current_batch[p]->mutable_transactions()->AddAllocated(new_txn);
    }
  }

  ++current_batch_size_;
  ++total_batch_size_;

  // If this is the first txn after starting over, schedule to send the batch at a later time
  if (total_batch_size_ == 1) {
    NewTimedCallback(config()->sequencer_batch_duration(), [this]() {
      SendBatches();
      StartOver();
    });

    batch_starting_time_ = std::chrono::steady_clock::now();
  }

  auto max_batch_size = config()->sequencer_batch_size();
  if (max_batch_size > 0 && current_batch_size_ >= max_batch_size) {
    NewBatch();
  }
}

void Batcher::SendBatches() {
  VLOG(1) << "Finished up to batch " << TXN_ID_STR(batch_id()) << " with " << total_batch_size_
          << " txns to be replicated. "
          << "Sending out for ordering and replicating";

  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordSequencerBatch(batch_id(), total_batch_size_,
                                                  (std::chrono::steady_clock::now() - batch_starting_time_).count());
  }

  auto local_region = config()->local_region();
  auto local_replica = config()->local_replica();
  auto local_machine_id = config()->local_machine_id();
  auto num_regions = config()->num_regions();
  auto num_replicas = config()->num_replicas(local_region);
  auto num_partitions = config()->num_partitions();

  int generator_position = batch_id_counter_ - batches_.size();
  for (auto& batch : batches_) {
    auto batch_id = batch[0]->id();

    // Propose a new batch
    auto paxos_env = NewEnvelope();
    auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
    paxos_propose->set_value(local_machine_id);
    Send(move(paxos_env), kLocalPaxos);

    // Distribute the batch data to other partitions in the same replica
    std::vector<internal::Batch*> batch_partitions;
    for (int p = 0; p < num_partitions; p++) {
      auto batch_partition = batch[p].release();

      RECORD(batch_partition, TransactionEvent::EXIT_SEQUENCER_IN_BATCH);

      auto env = NewBatchForwardingMessage({batch_partition}, generator_position);
      Send(*env, MakeMachineId(local_region, local_replica, p), LogManager::MakeLogChannel(local_region));
      // Collect back the batch partition to send to other regions
      batch_partitions.push_back(
          env->mutable_request()->mutable_forward_batch_data()->mutable_batch_data()->ReleaseLast());
    }

    // Distribute the batch data to other regions and other replicas in the local region.
    // All partitions of current batch are contained in a single message
    auto env = NewBatchForwardingMessage(move(batch_partitions), generator_position);
    std::vector<MachineId> destinations;
    destinations.reserve(num_regions);
    for (int reg = 0; reg < num_regions; reg++) {
      if (reg != local_region) {
        int part = 0;
        if (config()->sequencer_rrr()) {
          part = generator_position % num_partitions;
        } else {
          part = (reg + num_regions - local_region) % num_regions % num_partitions;
        }
        destinations.push_back(MakeMachineId(reg, 0, part));
      } else {
        for (int rep = 0; rep < num_replicas; rep++) {
          if (rep != local_replica) {
            int part = 0;
            if (config()->sequencer_rrr()) {
              part = generator_position % num_partitions;
            } else {
              part = (rep + num_replicas - local_replica) % num_replicas % num_partitions;
            }
            destinations.push_back(MakeMachineId(reg, rep, part));
          }
        }
      }
    }

    // Deliberately delay the batch as specified in the config
    if (config()->replication_delay_pct()) {
      std::bernoulli_distribution is_delayed(config()->replication_delay_pct() / 100.0);
      if (is_delayed(rg_)) {
        auto delay_ms = config()->replication_delay_amount_ms();

        VLOG(1) << "Delay batch " << TXN_ID_STR(batch_id) << " for " << delay_ms << " ms";

        NewTimedCallback(milliseconds(delay_ms),
                         [this, destinations, local_region, batch_id, delayed_env = env.release()]() {
                           VLOG(1) << "Sending delayed batch " << TXN_ID_STR(batch_id);
                           Send(*delayed_env, destinations, LogManager::MakeLogChannel(local_region));
                           delete delayed_env;
                         });

        return;
      }
    }

    Send(*env, destinations, LogManager::MakeLogChannel(local_region));

    generator_position++;
  }
}

EnvelopePtr Batcher::NewBatchForwardingMessage(std::vector<internal::Batch*>&& batch, int generator_position) {
  auto env = NewEnvelope();
  auto forward_batch = env->mutable_request()->mutable_forward_batch_data();
  forward_batch->set_generator(config()->local_machine_id());
  forward_batch->set_generator_position(generator_position);
  for (auto b : batch) {
    forward_batch->mutable_batch_data()->AddAllocated(b);
  }
  return env;
}

/**
 * {
 *    seq_num_future_txns: int,
 *    seq_process_future_txn_callback_id: int,
 *    seq_future_txns: [[int64, uint64]],
 *    seq_batch_size: int,
 * }
 */
void Batcher::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  if (process_future_txn_callback_handle_.has_value()) {
    stats.AddMember(StringRef(SEQ_PROCESS_FUTURE_TXN_CALLBACK_ID), process_future_txn_callback_handle_.value().second,
                    alloc);
  } else {
    stats.AddMember(StringRef(SEQ_PROCESS_FUTURE_TXN_CALLBACK_ID), -1, alloc);
  }
  stats.AddMember(StringRef(SEQ_BATCH_SIZE), total_batch_size_, alloc);
  {
    std::lock_guard<SpinLatch> guard(future_txns_mut_);
    stats.AddMember(StringRef(SEQ_NUM_FUTURE_TXNS), future_txns_.size(), alloc);
    if (level > 0) {
      stats.AddMember(StringRef(SEQ_FUTURE_TXNS),
                      ToJsonArray(
                          future_txns_,
                          [&alloc](const std::pair<Timestamp, Transaction*>& item) {
                            rapidjson::Value entry(rapidjson::kArrayType);
                            entry.PushBack(item.first.first, alloc).PushBack(item.second->internal().id(), alloc);
                            return entry;
                          },
                          alloc),
                      alloc);
    }
  }

  // Write JSON object to a buffer and send back to the server
  rapidjson::StringBuffer buf;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buf);
  stats.Accept(writer);

  auto env = NewEnvelope();
  env->mutable_response()->mutable_stats()->set_id(stats_request.id());
  env->mutable_response()->mutable_stats()->set_stats_json(buf.GetString());
  Send(move(env), kServerChannel);
}

}  // namespace slog