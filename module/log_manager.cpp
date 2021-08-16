#include "log_manager.h"

#include <glog/logging.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "proto/internal.pb.h"

using std::shared_ptr;

namespace slog {

namespace {
std::vector<uint64_t> RegionsToTags(const std::vector<uint32_t>& regions) {
  std::vector<uint64_t> tags;
  for (auto r : regions) {
    tags.push_back(LogManager::MakeTag(r));
  }
  return tags;
}
}  // namespace

using internal::Envelope;
using internal::Request;
using internal::Response;

void LocalLog::AddBatchId(uint32_t queue_id, uint32_t position, BatchId batch_id) {
  batch_queues_[queue_id].Insert(position, batch_id);
  UpdateReadyBatches();
}

void LocalLog::AddSlot(SlotId slot_id, uint32_t queue_id, MachineId leader) {
  slots_.Insert(slot_id, std::make_pair(queue_id, leader));
  UpdateReadyBatches();
}

bool LocalLog::HasNextBatch() const { return !ready_batches_.empty(); }

std::pair<SlotId, std::pair<BatchId, MachineId>> LocalLog::NextBatch() {
  if (!HasNextBatch()) {
    throw std::runtime_error("NextBatch() was called when there is no batch");
  }
  auto next_batch = ready_batches_.front();
  ready_batches_.pop();
  return next_batch;
}

void LocalLog::UpdateReadyBatches() {
  while (slots_.HasNext()) {
    auto [next_queue_id, leader] = slots_.Peek();
    if (batch_queues_.count(next_queue_id) == 0) {
      break;
    }
    auto& next_queue = batch_queues_.at(next_queue_id);
    if (!next_queue.HasNext()) {
      break;
    }
    auto slot_id = slots_.Next().first;
    auto batch_id = next_queue.Next().second;
    ready_batches_.emplace(slot_id, std::make_pair(batch_id, leader));
  }
}

LogManager::LogManager(int id, const std::vector<uint32_t>& regions, const shared_ptr<Broker>& broker,
                       const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, Broker::ChannelOption(kLogManagerChannel + id, true, RegionsToTags(regions)),
                      metrics_manager, poll_timeout) {
  for (uint32_t p = 0; p < config()->num_partitions(); p++) {
    if (p != config()->local_partition()) {
      other_partitions_.push_back(config()->MakeMachineId(config()->local_replica(), p));
    }
  }

  need_ack_from_replica_.resize(config()->num_replicas());
  auto replication_factor = static_cast<size_t>(config()->replication_factor());
  const auto& replication_order = config()->replication_order();
  for (size_t r = 0; r < std::min(replication_order.size(), replication_factor - 1); r++) {
    need_ack_from_replica_[replication_order[r]] = true;
  }
}

void LogManager::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kBatchReplicationAck:
      ProcessBatchReplicationAck(std::move(env));
      break;
    case Request::kForwardBatchData:
      ProcessForwardBatchData(std::move(env));
      break;
    case Request::kForwardBatchOrder:
      ProcessForwardBatchOrder(std::move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
  }

  AdvanceLog();
}

void LogManager::ProcessBatchReplicationAck(EnvelopePtr&& env) {
  auto from_replica = config()->UnpackMachineId(env->from()).first;
  auto local_replica = config()->local_replica();
  // If this ack comes from another replica, propagate the ack to
  // other machines in the same replica
  if (from_replica != local_replica) {
    Send(*env, other_partitions_, MakeTag(local_replica));
  }

  auto batch_id = env->request().batch_replication_ack().batch_id();
  single_home_logs_[local_replica].AckReplication(batch_id);
}

void LogManager::ProcessForwardBatchData(EnvelopePtr&& env) {
  auto local_replica = config()->local_replica();
  auto forward_batch_data = env->mutable_request()->mutable_forward_batch_data();
  auto [from_replica, from_partition] = config()->UnpackMachineId(env->from());
  BatchPtr my_batch;
  if (from_replica == local_replica) {
    my_batch = BatchPtr(forward_batch_data->mutable_batch_data()->ReleaseLast());
  } else {
    // If this batch comes from a remote replica, distribute the batch partitions to the
    // corresponding local partitions
    CHECK_EQ(forward_batch_data->batch_data_size(), config()->num_partitions());
    uint32_t p = config()->num_partitions() - 1;
    while (!forward_batch_data->mutable_batch_data()->empty()) {
      auto batch_partition = forward_batch_data->mutable_batch_data()->ReleaseLast();
      if (p == config()->local_partition()) {
        my_batch = BatchPtr(batch_partition);
      } else {
        auto home = forward_batch_data->home();
        auto new_env = NewEnvelope();
        auto new_forward_batch = new_env->mutable_request()->mutable_forward_batch_data();
        new_forward_batch->set_home(home);
        new_forward_batch->set_home_position(forward_batch_data->home_position());
        new_forward_batch->mutable_batch_data()->AddAllocated(batch_partition);
        Send(*new_env, config()->MakeMachineId(local_replica, p), MakeTag(home));
      }
      p--;
    }
  }

  RECORD(my_batch.get(), TransactionEvent::ENTER_LOG_MANAGER_IN_BATCH);

  VLOG(1) << "Received data for batch " << my_batch->id() << " from [" << env->from()
          << "]. Number of txns: " << my_batch->transactions_size();

  if (forward_batch_data->home() == local_replica) {
    local_log_.AddBatchId(from_partition /* queue_id */,
                          // Batches generated by the same machine need to follow the order
                          // of creation. This field is used to keep track of that order
                          forward_batch_data->home_position(), my_batch->id());
  }

  single_home_logs_[forward_batch_data->home()].AddBatch(move(my_batch));
}

void LogManager::ProcessForwardBatchOrder(EnvelopePtr&& env) {
  auto forward_batch_order = env->mutable_request()->mutable_forward_batch_order();
  auto from_replica = config()->UnpackMachineId(env->from()).first;
  switch (forward_batch_order->locality_case()) {
    case internal::ForwardBatchOrder::kLocalBatchOrder: {
      auto& order = forward_batch_order->local_batch_order();
      VLOG(1) << "Received local batch order. Slot id: " << order.slot() << ". Queue id: " << order.queue_id();

      local_log_.AddSlot(order.slot(), order.queue_id(), order.leader());
      break;
    }
    case internal::ForwardBatchOrder::kRemoteBatchOrder: {
      auto& batch_order = forward_batch_order->remote_batch_order();
      auto batch_id = batch_order.batch_id();
      auto home = batch_order.home();

      VLOG(1) << "Received remote batch order " << batch_id << " (home = " << home << ") from [" << env->from()
              << "]. Slot: " << batch_order.slot();

      // If this batch order comes from another replica, send this order to other partitions in the local replica
      if (from_replica != config()->local_replica()) {
        Send(*env, other_partitions_, MakeTag(home));
        // Ack back if needed
        if (batch_order.need_ack()) {
          Envelope env_ack;
          env_ack.mutable_request()->mutable_batch_replication_ack()->set_batch_id(batch_id);
          Send(env_ack, env->from(), MakeTag(home));
        }
      }

      single_home_logs_[home].AddSlot(batch_order.slot(), batch_id);
      break;
    }
    default:
      break;
  }
}

void LogManager::AdvanceLog() {
  // Advance local log
  auto local_replica = config()->local_replica();
  while (local_log_.HasNextBatch()) {
    auto next_batch = local_log_.NextBatch();
    auto slot_id = next_batch.first;
    auto [batch_id, leader] = next_batch.second;

    // Each entry in the local log is associated with a leader. If the current machine is the leader, it
    // is in charged of replicating the batch id and slot id to other regions
    if (config()->local_machine_id() == leader) {
      Envelope env;
      auto forward_batch_order = env.mutable_request()->mutable_forward_batch_order()->mutable_remote_batch_order();
      forward_batch_order->set_batch_id(batch_id);
      forward_batch_order->set_slot(slot_id);
      forward_batch_order->set_home(local_replica);
      forward_batch_order->set_need_ack(false);

      auto num_replicas = config()->num_replicas();
      auto num_partitions = config()->num_partitions();

      std::vector<MachineId> destinations, ack_destinations;
      destinations.reserve(num_replicas);
      ack_destinations.reserve(config()->replication_factor());
      for (uint32_t rep = 0; rep < num_replicas; rep++) {
        if (rep == local_replica) {
          continue;
        }
        // Send to a fixed partition of the destination replica to avoid reordering.
        // The partition is selected such that the logs are evenly distributed over
        // all partitions
        auto part = (rep + num_replicas - local_replica) % num_replicas % num_partitions;
        if (need_ack_from_replica_[rep]) {
          ack_destinations.push_back(config()->MakeMachineId(rep, part));
        } else {
          destinations.push_back(config()->MakeMachineId(rep, part));
        }
      }

      Send(env, destinations, MakeTag(local_replica));

      forward_batch_order->set_need_ack(true);
      Send(env, ack_destinations, MakeTag(local_replica));
    }

    single_home_logs_[local_replica].AddSlot(slot_id, batch_id, config()->replication_factor() - 1);
  }

  for (auto& [r, log] : single_home_logs_) {
    // Advance the single-home log
    while (log.HasNextBatch()) {
      auto next_batch = log.NextBatch().second;

      // Record the log for debugging
      if (per_thread_metrics_repo != nullptr && config()->metric_options().logs()) {
        for (auto& txn : next_batch->transactions()) {
          const auto& internal = txn.internal();
          per_thread_metrics_repo->RecordLogManagerEntry(
              r, next_batch->id(), internal.id(), internal.timestamp(), internal.mh_depart_from_coordinator_time(),
              internal.mh_arrive_at_home_time(), internal.mh_enter_local_batch_time());
        }
      }

      EmitBatch(move(next_batch));
    }
  }
}

void LogManager::EmitBatch(BatchPtr&& batch) {
  VLOG(1) << "Processing batch " << batch->id() << " from global log";

  auto transactions = Unbatch(batch.get());
  for (auto txn : transactions) {
    RECORD(txn->mutable_internal(), TransactionEvent::EXIT_LOG_MANAGER);
    auto env = NewEnvelope();
    auto forward_txn = env->mutable_request()->mutable_forward_txn();
    forward_txn->set_allocated_txn(txn);
    Send(move(env), kSchedulerChannel);
  }
}

}  // namespace slog