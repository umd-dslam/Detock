#include "module/multi_home_orderer.h"

#include <glog/logging.h>

#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "paxos/simulated_multi_paxos.h"

using std::shared_ptr;

namespace slog {

using internal::Batch;
using internal::Envelope;
using internal::Request;

MultiHomeOrderer::MultiHomeOrderer(const shared_ptr<Broker>& broker, const MetricsRepositoryManagerPtr& metrics_manager,
                                   std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, kMultiHomeOrdererChannel, metrics_manager, poll_timeout, true /* is_long_sender */),
      batch_id_counter_(0) {
  auto local_region = config()->local_region();
  auto local_replica = config()->local_replica();
  auto local_partition = config()->local_partition();

  for (int p = 0; p < config()->num_partitions(); p++) {
    if (static_cast<PartitionId>(p) != local_partition) {
      other_partitions_.push_back(MakeMachineId(local_region, local_replica, p));
    }
  }

  batch_per_reg_.resize(config()->num_regions());
  for (auto& batch : batch_per_reg_) {
    batch.resize(config()->num_partitions());
  }
  NewBatch();
}

void MultiHomeOrderer::NewBatch() {
  ++batch_id_counter_;
  batch_size_ = 0;
  for (auto& reg : batch_per_reg_) {
    for (auto& batch : reg) {
      if (batch == nullptr) {
        batch.reset(new Batch());
      }
      batch->Clear();
      batch->set_transaction_type(TransactionType::MULTI_HOME_OR_LOCK_ONLY);
      batch->set_id(batch_id());
    }
  }
}

void MultiHomeOrderer::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kForwardTxn: {
      // Received a new multi-home txn
      auto txn = request->mutable_forward_txn()->release_txn();

      RECORD(txn->mutable_internal(), TransactionEvent::ENTER_MULTI_HOME_ORDERER);

      AddToBatch(txn);
      break;
    }
    case Request::kForwardBatchData:
      // Received a batch of multi-home txn replicated from another region
      ProcessForwardBatchData(move(env));
      break;
    case Request::kForwardBatchOrder:
      ProcessForwardBatchOrder(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
      break;
  }

  AdvanceLog();
}

void MultiHomeOrderer::ProcessForwardBatchData(EnvelopePtr&& env) {
  auto local_region = config()->local_region();
  auto local_replica = config()->local_replica();
  auto local_partition = config()->local_partition();
  auto [from_region, from_replica, from_partition] = UnpackMachineId(env->from());
  auto forward_batch_data = env->mutable_request()->mutable_forward_batch_data();
  BatchPtr my_batch;
  if (from_region != local_region) {
    // If this is the first time the batch reaches our region, distribute the batch partitions
    // to the local partitions of current replica
    CHECK_EQ(forward_batch_data->batch_data_size(), config()->num_partitions());
    PartitionId p = config()->num_partitions() - 1;
    while (!forward_batch_data->mutable_batch_data()->empty()) {
      auto batch_partition = forward_batch_data->mutable_batch_data()->ReleaseLast();
      if (p == local_partition) {
        my_batch = BatchPtr(batch_partition);
      } else {
        auto new_env = NewEnvelope();
        auto new_forward_batch = new_env->mutable_request()->mutable_forward_batch_data();
        new_forward_batch->mutable_batch_data()->AddAllocated(batch_partition);
        Send(*new_env, MakeMachineId(local_region, local_replica, p), kMultiHomeOrdererChannel);
      }
      p--;
    }
  } else {
    // If the batch comes from the same region and replica, no need to distribute further
    my_batch = BatchPtr(forward_batch_data->mutable_batch_data()->ReleaseLast());
  }

  RECORD(my_batch.get(), TransactionEvent::ENTER_MULTI_HOME_ORDERER_IN_BATCH);

  VLOG(1) << "Received data for MULTI-HOME batch " << TXN_ID_STR(my_batch->id()) << " from " << MACHINE_ID_STR(env->from())
          << ". Number of txns: " << my_batch->transactions_size();

  multi_home_batch_log_.AddBatch(std::move(my_batch));
}

void MultiHomeOrderer::ProcessForwardBatchOrder(EnvelopePtr&& env) {
  auto forward_batch_order = env->mutable_request()->mutable_forward_batch_order();
  auto [from_region, from_replica, from_partition] = UnpackMachineId(env->from());
  auto local_region = config()->local_region();

  auto& batch_order = forward_batch_order->remote_batch_order();
  auto batch_id = batch_order.batch_id();

  // If this is the first time this order reaches the current region,
  // send it to other partitions in current replica
  if (from_region != local_region) {
    Send(*env, other_partitions_, kMultiHomeOrdererChannel);
  }

  VLOG(1) << "Received order for batch " << TXN_ID_STR(batch_id) << " from "
          << MACHINE_ID_STR(env->from()) << ". Slot: " << batch_order.slot();

  multi_home_batch_log_.AddSlot(batch_order.slot(), batch_order.batch_id());
}

void MultiHomeOrderer::AdvanceLog() {
  auto local_region = config()->local_region();
  auto local_replica = config()->local_replica();
  auto local_partition = config()->local_partition();
  auto generator = MakeMachineId(local_region, local_replica, kLockOnlyTxnGenerator);
  while (multi_home_batch_log_.HasNextBatch()) {
    auto [slot_id, batch] = multi_home_batch_log_.NextBatch();

    VLOG(1) << "Processing batch " << TXN_ID_STR(batch->id());

    RECORD(batch.get(), TransactionEvent::EXIT_MULTI_HOME_ORDERER);

    // One of the partition is in charge of proposing the new lock-only batch
    if (local_partition == 0) {
      auto paxos_env = NewEnvelope();
      auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
      paxos_propose->set_value(generator);
      Send(move(paxos_env), kLocalPaxos);
    }

    batch->set_id((1LL << (kMachineIdBits + kBatchIdCounterBits)) | (slot_id << kMachineIdBits) | generator);

    auto env = NewBatchForwardingMessage({batch.release()}, generator, slot_id);
    
    Send(move(env), kSequencerChannel);
  }
}

void MultiHomeOrderer::AddToBatch(Transaction* txn) {
  DCHECK(txn->internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY)
      << "Multi-home orderer batch can only contain multi-home txn. ";

  auto& regions = txn->internal().involved_regions();
  auto& partitions = txn->internal().involved_partitions();
  bool first = true;
  for (int i = 0; i < regions.size(); i++) {
    for (int j = 0; j < partitions.size(); j++) {
      auto& batch = batch_per_reg_[regions[i]][partitions[j]];
      if (first) {
        batch->mutable_transactions()->AddAllocated(txn);
        first = false;
      } else {
        batch->add_transactions()->CopyFrom(*txn);
      }
    }
  }

  ++batch_size_;

  // If this is the first txn in the batch, schedule to send the batch at a later time
  if (batch_size_ == 1) {
    NewTimedCallback(config()->mh_orderer_batch_duration(), [this]() {
      SendBatch();
      NewBatch();
    });

    batch_starting_time_ = std::chrono::steady_clock::now();
  }
}

void MultiHomeOrderer::SendBatch() {
  VLOG(3) << "Finished multi-home batch " << TXN_ID_STR(batch_id()) << " of size " << batch_size_;

  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordMHOrdererBatch(batch_id(), batch_size_,
                                                  (std::chrono::steady_clock::now() - batch_starting_time_).count());
  }

  auto paxos_env = NewEnvelope();
  auto paxos_propose = paxos_env->mutable_request()->mutable_paxos_propose();
  paxos_propose->set_value(batch_id());
  Send(move(paxos_env), MakeMachineId(config()->leader_region_for_multi_home_ordering(), 0, 0), kGlobalPaxos);

  auto num_regions = config()->num_regions();
  auto num_partitions = config()->num_partitions();
  auto local_region = config()->local_region();
  auto local_replica = config()->local_replica();

  std::vector<MachineId> destinations;
  destinations.reserve(num_regions);
  for (int reg = 0; reg < num_regions; reg++) {
    if (reg == local_region) {
      // Distribute the batch data to other partitions in the same replica
      for (int p = 0; p < num_partitions; p++) {
        auto env = NewBatchForwardingMessage({batch_per_reg_[reg][p].release()});
        Send(*env, MakeMachineId(local_region, local_replica, p), kMultiHomeOrdererChannel);
      }
    } else {
      // Distribute the batch data to other regions.
      // All partitions of current batch are contained in a single message
      std::vector<internal::Batch*> batch_partitions;
      for (int p = 0; p < num_partitions; p++) {
        batch_partitions.push_back(batch_per_reg_[reg][p].release());
      }
      auto env = NewBatchForwardingMessage(move(batch_partitions));
      int part = batch_id_counter_ % num_partitions;
      Send(move(env), MakeMachineId(reg, local_replica, part), kMultiHomeOrdererChannel);
    }
  }
}

}  // namespace slog