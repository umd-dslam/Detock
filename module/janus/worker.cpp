#include "module/janus/worker.h"

#include <glog/logging.h>

#include <thread>

#include "common/proto_utils.h"

using std::make_pair;

namespace janus {

using slog::kServerChannel;
using slog::kWorkerChannel;
using slog::MachineId;
using slog::MakeMachineId;
using slog::Metadata;
using slog::Record;
using slog::Sharder;
using slog::TransactionEvent;
using slog::TransactionStatus;
using slog::UnpackMachineId;
using slog::internal::Envelope;
using slog::internal::Request;
using slog::internal::Response;
using std::make_unique;

Worker::Worker(int id, const std::shared_ptr<Broker>& broker, const std::shared_ptr<Storage>& storage,
               const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, kWorkerChannel + id, metrics_manager, poll_timeout),
      id_(id),
      storage_(storage),
      sharder_(slog::Sharder::MakeSharder(config())) {
  switch (config()->execution_type()) {
    case slog::internal::ExecutionType::KEY_VALUE:
      execution_ = make_unique<slog::KeyValueExecution>(sharder_, storage);
      break;
    case slog::internal::ExecutionType::TPC_C:
      execution_ = make_unique<slog::TPCCExecution>(sharder_, storage);
      break;
    default:
      execution_ = make_unique<slog::NoopExecution>();
      break;
  }
}

void Worker::Initialize() {
  zmq::socket_t sched_socket(*context(), ZMQ_PAIR);
  sched_socket.set(zmq::sockopt::rcvhwm, 0);
  sched_socket.set(zmq::sockopt::sndhwm, 0);
  sched_socket.connect(kSchedWorkerAddress + std::to_string(id_));

  AddCustomSocket(std::move(sched_socket));
}

void Worker::OnInternalRequestReceived(EnvelopePtr&& env) {
  CHECK_EQ(env->request().type_case(), Request::kRemoteReadResult) << "Invalid request for worker";
  auto& read_result = env->request().remote_read_result();
  auto txn_id = read_result.txn_id();
  auto state_it = txn_states_.find(txn_id);
  if (state_it == txn_states_.end()) {
    LOG(WARNING) << "Transaction " << txn_id << " does not exist for remote read result";
    return;
  }

  VLOG(3) << "Got remote read result for txn " << txn_id;

  auto& state = state_it->second;
  auto txn = state.txn;

  if (read_result.deadlocked()) {
    RECORD(txn->mutable_internal(), TransactionEvent::GOT_REMOTE_READS_DEADLOCKED);
  } else {
    RECORD(txn->mutable_internal(), TransactionEvent::GOT_REMOTE_READS);
  }

  if (txn->status() != TransactionStatus::ABORTED) {
    if (read_result.will_abort()) {
      // TODO: optimize by returning an aborting transaction to the scheduler immediately.
      // later remote reads will need to be garbage collected.
      txn->set_status(TransactionStatus::ABORTED);
      txn->set_abort_code(read_result.abort_code());
      txn->set_abort_reason(read_result.abort_reason());
    } else {
      // Apply remote reads.
      for (const auto& kv : read_result.reads()) {
        txn->mutable_keys()->Add()->CopyFrom(kv);
      }
    }
  }

  state.remote_reads_waiting_on--;

  // Move the transaction to a new phase if all remote reads arrive
  if (state.remote_reads_waiting_on == 0) {
    if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
      state.phase = TransactionState::Phase::EXECUTE;

      // Remove the redirection at broker for this txn
      StopRedirection(txn_id);

      VLOG(3) << "Execute txn " << txn_id << " after receving all remote read results";
    } else {
      LOG(FATAL) << "Invalid phase";
    }
  }

  AdvanceTransaction(txn_id);
}

bool Worker::OnCustomSocket() {
  auto& sched_socket = GetCustomSocket(0);

  zmq::message_t msg;
  if (!sched_socket.recv(msg, zmq::recv_flags::dontwait)) {
    return false;
  }

  auto txn = *msg.data<Transaction*>();
  auto txn_id = txn->internal().id();

  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_WORKER);

  // Create a state for the new transaction
  auto [iter, ok] = txn_states_.try_emplace(txn_id, txn);

  CHECK(ok) << "Transaction " << txn_id << " has already been dispatched to this worker";

  iter->second.phase = TransactionState::Phase::READ_LOCAL_STORAGE;

  VLOG(3) << "Initialized state for txn " << txn_id;

  AdvanceTransaction(txn_id);

  return true;
}

void Worker::AdvanceTransaction(TxnId txn_id) {
  auto& state = TxnState(txn_id);
  switch (state.phase) {
    case TransactionState::Phase::READ_LOCAL_STORAGE:
      ReadLocalStorage(txn_id);
      [[fallthrough]];
    case TransactionState::Phase::WAIT_REMOTE_READ:
      if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
        // The only way to get out of this phase is through remote messages
        break;
      }
      [[fallthrough]];
    case TransactionState::Phase::EXECUTE:
      if (state.phase == TransactionState::Phase::EXECUTE) {
        Execute(txn_id);
      }
      [[fallthrough]];
    case TransactionState::Phase::FINISH:
      Finish(txn_id);
      // Never fallthrough after this point because Finish and PreAbort
      // has already destroyed the state object
      break;
  }
}

void Worker::ReadLocalStorage(TxnId txn_id) {
  auto& state = TxnState(txn_id);
  auto txn = state.txn;

  if (txn->status() != TransactionStatus::ABORTED) {
    for (auto& kv : *(txn->mutable_keys())) {
      const auto& key = kv.key();
      if (sharder_->is_local_key(key)) {
        auto value = kv.mutable_value_entry();
        if (Record record; storage_->Read(key, record)) {
          value->set_value(record.to_string());
        }
      }
    }
  }

  VLOG(3) << "Broadcasting local reads to other partitions";
  BroadcastReads(txn_id);

  // Set the number of remote reads that this partition needs to wait for
  state.remote_reads_waiting_on = 0;

  const auto& waiting_partitions = txn->internal().involved_partitions();

  if (std::find(waiting_partitions.begin(), waiting_partitions.end(), config()->local_partition()) !=
      waiting_partitions.end()) {
    // Waiting partition needs remote reads from all partitions
    state.remote_reads_waiting_on = txn->internal().involved_partitions_size() - 1;
  }
  if (state.remote_reads_waiting_on == 0) {
    VLOG(3) << "Execute txn " << txn_id << " without remote reads";
    state.phase = TransactionState::Phase::EXECUTE;
  } else {
    // Establish a redirection at broker for this txn so that we can receive remote reads
    StartRedirection(txn_id);

    VLOG(3) << "Defer executing txn " << txn_id << " until having enough remote reads";
    state.phase = TransactionState::Phase::WAIT_REMOTE_READ;
  }
}

void Worker::Execute(TxnId txn_id) {
  auto& state = TxnState(txn_id);
  auto txn = state.txn;

  switch (txn->program_case()) {
    case Transaction::kCode: {
      if (txn->status() != TransactionStatus::ABORTED) {
        execution_->Execute(*txn);
      }
      if (txn->status() == TransactionStatus::ABORTED) {
        VLOG(3) << "Txn " << txn_id << " aborted with reason: " << txn->abort_reason();
      } else {
        VLOG(3) << "Committed txn " << txn_id;
      }
      break;
    }
    default:
      LOG(FATAL) << "Invalid procedure";
  }
  state.phase = TransactionState::Phase::FINISH;
}

void Worker::Finish(TxnId txn_id) {
  auto& state = TxnState(txn_id);
  auto txn = state.txn;

  RECORD(txn->mutable_internal(), TransactionEvent::EXIT_WORKER);

  auto coordinator = txn->internal().coordinating_server();
  auto [coord_reg, coord_rep, _] = UnpackMachineId(coordinator);
  if (coord_reg == config()->local_region() && coord_rep == config()->local_replica()) {
    Envelope env;
    auto finished_sub_txn = env.mutable_request()->mutable_finished_subtxn();
    finished_sub_txn->set_partition(config()->local_partition());
    finished_sub_txn->set_allocated_txn(txn);
    Send(env, txn->internal().coordinating_server(), kServerChannel);
  } else {
    delete txn;
  }

  // Notify the scheduler that we're done
  zmq::message_t msg(sizeof(TxnId));
  *msg.data<TxnId>() = txn_id;
  GetCustomSocket(0).send(msg, zmq::send_flags::none);

  // Done with this txn. Remove it from the state map
  txn_states_.erase(txn_id);

  VLOG(3) << "Finished with txn " << txn_id;
}

void Worker::BroadcastReads(TxnId txn_id) {
  auto& state = TxnState(txn_id);
  auto& txn = state.txn;

  const auto& waiting_partitions = txn->internal().involved_partitions();

  auto local_region = config()->local_region();
  auto local_replica = config()->local_replica();
  auto local_partition = config()->local_partition();

  std::vector<MachineId> destinations;
  for (auto p : waiting_partitions) {
    if (p != local_partition) {
      destinations.push_back(MakeMachineId(local_region, local_replica, p));
    }
  }

  if (destinations.empty()) {
    return;
  }

  auto aborted = txn->status() == TransactionStatus::ABORTED;

  // Send abort result and local reads to all remote active partitions
  Envelope env;
  auto rrr = env.mutable_request()->mutable_remote_read_result();
  rrr->set_txn_id(txn_id);
  rrr->set_partition(local_partition);
  rrr->set_will_abort(aborted);
  rrr->set_abort_code(txn->abort_code());
  rrr->set_abort_reason(txn->abort_reason());
  if (!aborted) {
    auto reads_to_be_sent = rrr->mutable_reads();
    for (const auto& kv : txn->keys()) {
      reads_to_be_sent->Add()->CopyFrom(kv);
    }
  }

  Send(env, destinations, txn_id);
}

TransactionState& Worker::TxnState(TxnId txn_id) {
  auto state_it = txn_states_.find(txn_id);
  CHECK(state_it != txn_states_.end());
  return state_it->second;
}

void Worker::StartRedirection(TxnId txn_id) {
  auto redirect_env = NewEnvelope();
  redirect_env->mutable_request()->mutable_broker_redirect()->set_tag(txn_id);
  redirect_env->mutable_request()->mutable_broker_redirect()->set_channel(channel());
  Send(move(redirect_env), Broker::MakeChannel(config()->broker_ports_size() - 1));
}

void Worker::StopRedirection(TxnId txn_id) {
  auto redirect_env = NewEnvelope();
  redirect_env->mutable_request()->mutable_broker_redirect()->set_tag(txn_id);
  redirect_env->mutable_request()->mutable_broker_redirect()->set_stop(true);
  Send(move(redirect_env), Broker::MakeChannel(config()->broker_ports_size() - 1));
}

}  // namespace janus