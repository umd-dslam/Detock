#include "module/sequencer.h"

#include <glog/logging.h>

#include <algorithm>

#include "common/clock.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "module/log_manager.h"
#include "paxos/simulated_multi_paxos.h"

using std::move;

namespace slog {

using internal::Batch;
using internal::Request;
using std::chrono::milliseconds;

Sequencer::Sequencer(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                     const MetricsRepositoryManagerPtr& metrics_manager, milliseconds poll_timeout)
    : NetworkedModule(context, config, config->sequencer_port(), kSequencerChannel, metrics_manager, poll_timeout),
      batcher_(std::make_shared<Batcher>(context, config, metrics_manager, poll_timeout)),
      batcher_runner_(std::static_pointer_cast<Module>(batcher_)),
      sharder_(Sharder::MakeSharder(config)) {}

void Sequencer::Initialize() { batcher_runner_.StartInNewThread(); }

void Sequencer::OnInternalRequestReceived(EnvelopePtr&& env) {
  auto request = env->mutable_request();
  switch (request->type_case()) {
    case Request::kForwardTxn:
      ProcessForwardRequest(move(env));
      break;
    case Request::kForwardBatchData:
      ProcessForwardBatchData(move(env));
      break;
    case Request::kPing:
      ProcessPingRequest(move(env));
      break;
    case Request::kStats:
      Send(move(env), kBatcherChannel);
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(request->type_case(), Request) << "\"";
      break;
  }
}

void Sequencer::ProcessForwardRequest(EnvelopePtr&& env) {
  auto now = slog_clock::now().time_since_epoch().count();
  auto txn = env->mutable_request()->mutable_forward_txn()->mutable_txn();
  auto txn_internal = txn->mutable_internal();

  RECORD(txn_internal, TransactionEvent::ENTER_SEQUENCER);

  txn_internal->set_mh_arrive_at_home_time(now);

  if (config()->bypass_mh_orderer() && per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordTxnTimestamp(txn_internal->id(), env->from(), txn_internal->timestamp(), now);
  }

  if (config()->bypass_mh_orderer() && config()->synchronized_batching()) {
    if (txn_internal->timestamp() <= now) {
      VLOG(3) << "Txn " << TXN_ID_STR(txn_internal->id()) << " has a timestamp "
              << (now - txn_internal->timestamp()) / 1000 << " us in the past";

#if !defined(LOCK_MANAGER_DDR)
      // If not using DDR, restart the transaction
      txn->set_status(TransactionStatus::ABORTED);
      txn->set_abort_code(AbortCode::RESTARTED);
#endif
    } else {
      VLOG(3) << "Txn " << TXN_ID_STR(txn_internal->id()) << " has a timestamp "
              << (txn_internal->timestamp() - now) / 1000 << " us into the future";

      RECORD_WITH_TIME(txn_internal, TransactionEvent::EXPECTED_WAIT_TIME_UNTIL_ENTER_LOCAL_BATCH,
                       txn_internal->timestamp() - now);
    }
    // Put into a sorted buffer and wait until local clock reaches the txn's timestamp.
    // Send a signal to the batcher if the earliest time in the buffer has changed, so that
    // the batcher is rescheduled to wake up at this ealier time
    bool signal_needed = batcher_->BufferFutureTxn(env->mutable_request()->mutable_forward_txn()->release_txn());
    if (signal_needed) {
      auto env = NewEnvelope();
      env->mutable_request()->mutable_signal();
      Send(move(env), kBatcherChannel);
    }
  } else {
    // Put to batch immediately
    txn_internal->set_mh_enter_local_batch_time(now);
    Send(move(env), kBatcherChannel);
  }
}

void Sequencer::ProcessForwardBatchData(EnvelopePtr&& env) {
  auto forward_batch_data = env->mutable_request()->mutable_forward_batch_data();
  auto local_region = config()->local_region();
  auto local_partition = config()->local_partition();
  auto num_regions = config()->num_regions();
  auto num_replicas = config()->num_replicas(local_region);

  CHECK_EQ(forward_batch_data->batch_data_size(), 1);

  auto batch = forward_batch_data->mutable_batch_data()->ReleaseLast();
  CHECK_EQ(batch->transaction_type(), TransactionType::MULTI_HOME_OR_LOCK_ONLY);

  VLOG(3) << "Got batch " << TXN_ID_STR(batch->id()) << " of " << batch->transactions_size()
          << " multi-home txns to transform to lock-only txns";

  // Go over the batch and turn each transaction into a lock-only txn
  auto lo_batch = forward_batch_data->mutable_batch_data()->Add();
  lo_batch->set_id(batch->id());
  lo_batch->set_transaction_type(batch->transaction_type());
  lo_batch->mutable_events()->CopyFrom(batch->events());  
  for (int i = 0; i < batch->transactions_size(); i++) {
    auto txn = batch->mutable_transactions()->ReleaseLast();
    txn = GenerateLockOnlyTxn(txn, local_region, true /* in_place */);
    txn = GeneratePartitionedTxn(sharder_, txn, local_partition, true /* in_place */);
    if (txn != nullptr) {
      lo_batch->mutable_transactions()->AddAllocated(txn);
    }
  }
  delete batch;

  int l = 0, r = lo_batch->transactions_size() - 1;
  for (;l < r; l++, r--) {
    lo_batch->mutable_transactions(l)->Swap(lo_batch->mutable_transactions(r));
  }

  // Replicate to the corresponding partitions in other replicas
  std::vector<MachineId> destinations;
  for (int reg = 0; reg < num_regions; reg++) {
    if (reg != local_region) {
      destinations.push_back(MakeMachineId(reg, 0, local_partition));
    } else {
      for (int rep = 0; rep < num_replicas; rep++) {
        destinations.push_back(MakeMachineId(reg, rep, local_partition));
      }
    }
  }
  Send(*env, destinations, LogManager::MakeLogChannel(local_region));
}

void Sequencer::ProcessPingRequest(EnvelopePtr&& env) {
  auto now = slog_clock::now().time_since_epoch().count();
  auto pong_env = NewEnvelope();
  auto pong = pong_env->mutable_response()->mutable_pong();
  pong->set_src_time(env->request().ping().src_time());
  pong->set_dst_time(now);
  pong->set_dst(env->request().ping().dst());
  Send(move(pong_env), env->from(), kForwarderChannel);
}

}  // namespace slog