#include "interleaver.h"

#include <glog/logging.h>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/proto_utils.h"
#include "proto/internal.pb.h"

using std::shared_ptr;

namespace slog {

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

Interleaver::Interleaver(const shared_ptr<Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
                         std::chrono::milliseconds poll_timeout)
    : NetworkedModule("Interleaver", broker, kInterleaverChannel, metrics_manager, poll_timeout),
      rg_(std::random_device()()) {
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

void Interleaver::Initialize() {
  zmq::socket_t local_queue_socket(*context(), ZMQ_PULL);
  local_queue_socket.set(zmq::sockopt::rcvhwm, 0);
  local_queue_socket.bind(MakeInProcChannelAddress(kLocalLogChannel));

  SetMainVsCustomSocketWeights(config()->interleaver_remote_to_local_ratio());

  AddCustomSocket(std::move(local_queue_socket));
}

/**
 * The local queue order messages are received via a dedicated socket so that we can control the
 * priority between it and other message types
 */
bool Interleaver::OnCustomSocket() {
  auto& socket = GetCustomSocket(0);
  auto env = RecvEnvelope(socket, true /* dont_wait */);
  if (env == nullptr) {
    return false;
  }
  if (env->request().type_case() != Request::kForwardBatch) {
    LOG(ERROR) << "Only local queue order messages are expected on this socket";
    return false;
  }

  ProcessForwardBatch(std::move(env));

  AdvanceLogs();

  return true;
}

void Interleaver::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kBatchReplicationAck: {
      auto from_replica = config()->UnpackMachineId(env->from()).first;

      // If this ack comes from another replica, propagate the ack to
      // other machines in the same replica
      if (from_replica != config()->local_replica()) {
        Send(*env, other_partitions_, kInterleaverChannel);
      }

      auto local_replica = config()->local_replica();
      auto batch_id = request->batch_replication_ack().batch_id();
      single_home_logs_[local_replica].AckReplication(batch_id);
      break;
    }
    case Request::kForwardBatch: {
      ProcessForwardBatch(std::move(env));
      break;
    }
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
  }

  AdvanceLogs();
}

void Interleaver::ProcessForwardBatch(EnvelopePtr&& env) {
  auto forward_batch = env->mutable_request()->mutable_forward_batch();
  auto [from_replica, from_partition] = config()->UnpackMachineId(env->from());
  switch (forward_batch->part_case()) {
    case internal::ForwardBatch::kBatchData: {
      auto batch = BatchPtr{forward_batch->release_batch_data()};

      RECORD(batch.get(), TransactionEvent::ENTER_INTERLEAVER_IN_BATCH);

      VLOG(1) << "Received data for batch " << batch->id() << " from [" << env->from()
              << "]. Number of txns: " << batch->transactions_size();

      if (from_replica == config()->local_replica()) {
        local_log_.AddBatchId(from_partition /* queue_id */,
                              // Batches generated by the same machine need to follow the order
                              // of creation. This field is used to keep track of that order
                              forward_batch->same_origin_position(), batch->id());
      }

      single_home_logs_[from_replica].AddBatch(move(batch));
      break;
    }
    case internal::ForwardBatch::kLocalBatchOrder: {
      auto& order = forward_batch->local_batch_order();
      VLOG(1) << "Received local batch order. Slot id: " << order.slot() << ". Queue id: " << order.queue_id();

      local_log_.AddSlot(order.slot(), order.queue_id(), order.leader());
      break;
    }
    case internal::ForwardBatch::kRemoteBatchOrder: {
      auto& batch_order = forward_batch->remote_batch_order();
      auto batch_id = batch_order.batch_id();

      VLOG(1) << "Received remote batch order " << batch_id << " (home = " << batch_order.home() << ") from ["
              << env->from() << "]. Slot: " << batch_order.slot();

      // If this batch order comes from another replica, send this order to other partitions in the local replica
      if (from_replica != config()->local_replica()) {
        Send(*env, other_partitions_, kInterleaverChannel);
        // Ack back if needed
        if (batch_order.need_ack()) {
          Envelope env_ack;
          env_ack.mutable_request()->mutable_batch_replication_ack()->set_batch_id(batch_id);
          Send(env_ack, env->from(), kInterleaverChannel);
        }
      }

      single_home_logs_[batch_order.home()].AddSlot(batch_order.slot(), batch_id);
      break;
    }
    default:
      break;
  }
}

void Interleaver::AdvanceLogs() {
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
      auto forward_batch_order = env.mutable_request()->mutable_forward_batch()->mutable_remote_batch_order();
      forward_batch_order->set_batch_id(batch_id);
      forward_batch_order->set_slot(slot_id);
      forward_batch_order->set_home(local_replica);
      forward_batch_order->set_need_ack(false);

      std::uniform_int_distribution<> rnd(0, config()->num_partitions() - 1);
      auto part = rnd(rg_);

      vector<MachineId> destinations, ack_destinations;
      destinations.reserve(config()->num_replicas());
      ack_destinations.reserve(config()->replication_factor());
      for (uint32_t rep = 0; rep < config()->num_replicas(); rep++) {
        if (rep == local_replica) {
          continue;
        }
        if (need_ack_from_replica_[rep]) {
          ack_destinations.push_back(config()->MakeMachineId(rep, part));
        } else {
          destinations.push_back(config()->MakeMachineId(rep, part));
        }
      }

      Send(env, destinations, kInterleaverChannel);

      forward_batch_order->set_need_ack(true);
      Send(env, ack_destinations, kInterleaverChannel);
    }

    single_home_logs_[local_replica].AddSlot(slot_id, batch_id, config()->replication_factor() - 1);
  }

  // Advance single-home logs
  for (auto& [r, log] : single_home_logs_) {
    while (log.HasNextBatch()) {
      auto next_batch = log.NextBatch().second;

      // Record the log for debugging
      if (per_thread_metrics_repo != nullptr && config()->metric_options().interleaver_logs()) {
        for (auto& txn : next_batch->transactions()) {
          const auto& internal = txn.internal();
          int64_t enter_sequencer_time = 0, enter_local_batch_time = 0, exit_forwarder_time = 0;
          for (int i = 0; i < internal.events_size(); i++) {
            if (internal.events(i) == TransactionEvent::ENTER_SEQUENCER) {
              enter_sequencer_time = internal.event_times(i);
            } else if (internal.events(i) == TransactionEvent::ENTER_LOCAL_BATCH) {
              enter_local_batch_time = internal.event_times(i);
            } else if (internal.events(i) == TransactionEvent::EXIT_FORWARDER_TO_MULTI_HOME_ORDERER) {
              exit_forwarder_time = internal.event_times(i);
            }
          }
          per_thread_metrics_repo->RecordInterleaverLogEntry(r, next_batch->id(), internal.id(), internal.timestamp(),
                                                             exit_forwarder_time, enter_sequencer_time,
                                                             enter_local_batch_time);
        }
      }

      EmitBatch(move(next_batch));
    }
  }
}

void Interleaver::EmitBatch(BatchPtr&& batch) {
  VLOG(1) << "Processing batch " << batch->id() << " from global log";

  auto transactions = Unbatch(batch.get());
  for (auto txn : transactions) {
    RECORD(txn->mutable_internal(), TransactionEvent::EXIT_INTERLEAVER);
    auto env = NewEnvelope();
    auto forward_txn = env->mutable_request()->mutable_forward_txn();
    forward_txn->set_allocated_txn(txn);
    Send(move(env), kSchedulerChannel);
  }
}

}  // namespace slog