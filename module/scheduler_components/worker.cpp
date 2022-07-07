#include "module/scheduler_components/worker.h"

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
#include "module/scheduler_components/remaster_manager.h"
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

#include <glog/logging.h>

#include <thread>

#include "common/proto_utils.h"

using std::make_pair;

namespace slog {

namespace {
uint64_t MakeTag(const RunId& run_id) { return run_id.first * 10 + run_id.second; }

inline std::ostream& operator<<(std::ostream& os, const RunId& run_id) {
  os << "(" << TXN_ID_STR(run_id.first) << ", " << run_id.second << ")";
  return os;
}
}  // namespace

using internal::Envelope;
using internal::Request;
using internal::Response;
using std::make_unique;

Worker::Worker(int id, const std::shared_ptr<Broker>& broker, const std::shared_ptr<Storage>& storage,
               const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, kWorkerChannel + id, metrics_manager, poll_timeout), id_(id), storage_(storage) {
  switch (config()->execution_type()) {
    case internal::ExecutionType::KEY_VALUE:
      execution_ = make_unique<KeyValueExecution>(Sharder::MakeSharder(config()), storage);
      break;
    case internal::ExecutionType::TPC_C:
      execution_ = make_unique<TPCCExecution>(Sharder::MakeSharder(config()), storage);
      break;
    default:
      execution_ = make_unique<NoopExecution>();
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
  auto run_id = make_pair(read_result.txn_id(), read_result.deadlocked());
  auto state_it = txn_states_.find(run_id);
  if (state_it == txn_states_.end()) {
    LOG(WARNING) << "Transaction " << run_id << " does not exist for remote read result";
    return;
  }

  VLOG(3) << "Got remote read result for txn " << run_id;

  auto& state = state_it->second;
  auto& txn = state.txn_holder->txn();

  if (read_result.deadlocked()) {
    RECORD(txn.mutable_internal(), TransactionEvent::GOT_REMOTE_READS_DEADLOCKED);
  } else {
    RECORD(txn.mutable_internal(), TransactionEvent::GOT_REMOTE_READS);
  }

  if (txn.status() != TransactionStatus::ABORTED) {
    if (read_result.will_abort()) {
      // TODO: optimize by returning an aborting transaction to the scheduler immediately.
      // later remote reads will need to be garbage collected.
      txn.set_status(TransactionStatus::ABORTED);
      txn.set_abort_code(read_result.abort_code());
      txn.set_abort_reason(read_result.abort_reason());
    } else {
      // Apply remote reads.
      for (const auto& kv : read_result.reads()) {
        txn.mutable_keys()->Add()->CopyFrom(kv);
      }
    }
  }

  state.remote_reads_waiting_on--;

  // Move the transaction to a new phase if all remote reads arrive
  if (state.remote_reads_waiting_on == 0) {
    if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
      state.phase = TransactionState::Phase::EXECUTE;

      // Remove the redirection at broker for this txn
      StopRedirection(run_id);

      VLOG(3) << "Execute txn " << run_id << " after receving all remote read results";
    } else {
      LOG(FATAL) << "Invalid phase";
    }
  }

  AdvanceTransaction(run_id);
}

bool Worker::OnCustomSocket() {
  auto& sched_socket = GetCustomSocket(0);

  zmq::message_t msg;
  if (!sched_socket.recv(msg, zmq::recv_flags::dontwait)) {
    return false;
  }

  auto [txn_holder, deadlocked] = *msg.data<std::pair<TxnHolder*, bool>>();
  auto& txn = txn_holder->txn();
  auto run_id = std::make_pair(txn.internal().id(), deadlocked);
  if (deadlocked) {
    auto old_run_id = std::make_pair(txn.internal().id(), false);
    // Clean up any transaction state created before the deadlock was detected
    if (txn_states_.erase(old_run_id)) {
      StopRedirection(old_run_id);
    }
  }

  RECORD(txn.mutable_internal(), TransactionEvent::ENTER_WORKER);

  // Create a state for the new transaction
  // TODO: Clean up txns that got into a deadlock
  auto [iter, ok] = txn_states_.try_emplace(run_id, txn_holder);

  CHECK(ok) << "Transaction " << run_id << " has already been dispatched to this worker";

  iter->second.phase = TransactionState::Phase::READ_LOCAL_STORAGE;

  VLOG(3) << "Initialized state for txn " << run_id;

  AdvanceTransaction(run_id);

  return true;
}

void Worker::AdvanceTransaction(const RunId& run_id) {
  auto& state = TxnState(run_id);
  switch (state.phase) {
    case TransactionState::Phase::READ_LOCAL_STORAGE:
      ReadLocalStorage(run_id);
      [[fallthrough]];
    case TransactionState::Phase::WAIT_REMOTE_READ:
      if (state.phase == TransactionState::Phase::WAIT_REMOTE_READ) {
        // The only way to get out of this phase is through remote messages
        break;
      }
      [[fallthrough]];
    case TransactionState::Phase::EXECUTE:
      if (state.phase == TransactionState::Phase::EXECUTE) {
        Execute(run_id);
      }
      [[fallthrough]];
    case TransactionState::Phase::FINISH:
      Finish(run_id);
      // Never fallthrough after this point because Finish and PreAbort
      // has already destroyed the state object
      break;
  }
}

void Worker::ReadLocalStorage(const RunId& run_id) {
  auto& state = TxnState(run_id);
  auto txn_holder = state.txn_holder;
  auto& txn = txn_holder->txn();

  if (txn.status() != TransactionStatus::ABORTED) {
#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
    switch (RemasterManager::CheckCounters(txn, false, storage_)) {
      case VerifyMasterResult::VALID: {
        break;
      }
      case VerifyMasterResult::ABORT: {
        txn.set_status(TransactionStatus::ABORTED);
        txn.set_abort_reason("outdated counter");
        break;
      }
      case VerifyMasterResult::WAITING: {
        LOG(FATAL) << "Transaction " << run_id << " was sent to worker with a high counter";
        break;
      }
      default:
        LOG(FATAL) << "Unrecognized check counter result";
        break;
    }
#endif

    // We don't need to check if keys are in partition here since the assumption is that
    // the out-of-partition keys have already been removed
    for (auto& kv : *(txn.mutable_keys())) {
      const auto& key = kv.key();
      auto value = kv.mutable_value_entry();
      if (Record record; storage_->Read(key, record)) {
        // Check whether the stored master metadata matches with the information
        // stored in the transaction
        if (value->metadata().master() != record.metadata().master) {
          txn.set_status(TransactionStatus::ABORTED);
          txn.set_abort_reason("outdated master");
          break;
        }
        value->set_value(record.to_string());
      } else if (txn.program_case() == Transaction::kRemaster) {
        txn.set_status(TransactionStatus::ABORTED);
        txn.set_abort_reason("remaster non-existent key " + key);
        break;
      }
    }
  }

  VLOG(3) << "Broadcasting local reads to other partitions";
  BroadcastReads(run_id);

  // Set the number of remote reads that this partition needs to wait for
  state.remote_reads_waiting_on = 0;

#ifdef LOCK_MANAGER_DDR
  // If DDR is used, all partitions have to wait
  const auto& waiting_partitions = txn.internal().involved_partitions();
#else
  const auto& waiting_partitions = txn.internal().active_partitions();
#endif
  if (std::find(waiting_partitions.begin(), waiting_partitions.end(), config()->local_partition()) !=
      waiting_partitions.end()) {
    // Waiting partition needs remote reads from all partitions
    state.remote_reads_waiting_on = txn.internal().involved_partitions_size() - 1;
  }
  if (state.remote_reads_waiting_on == 0) {
    VLOG(3) << "Execute txn " << run_id << " without remote reads";
    state.phase = TransactionState::Phase::EXECUTE;
  } else {
    // Establish a redirection at broker for this txn so that we can receive remote reads
    StartRedirection(run_id);

    VLOG(3) << "Defer executing txn " << run_id << " until having enough remote reads";
    state.phase = TransactionState::Phase::WAIT_REMOTE_READ;
  }
}

void Worker::Execute(const RunId& run_id) {
  auto& state = TxnState(run_id);
  auto& txn = state.txn_holder->txn();

  switch (txn.program_case()) {
    case Transaction::kCode: {
      if (txn.status() != TransactionStatus::ABORTED) {
        execution_->Execute(txn);
      }

      if (txn.status() == TransactionStatus::ABORTED) {
        VLOG(3) << "Txn " << run_id << " aborted with reason: " << txn.abort_reason();
      } else {
        VLOG(3) << "Committed txn " << run_id;
      }
      break;
    }
    case Transaction::kRemaster: {
      txn.set_status(TransactionStatus::COMMITTED);
      auto it = txn.keys().begin();
      const auto& key = it->key();
      Record record;
      storage_->Read(key, record);
      auto new_counter = it->value_entry().metadata().counter() + 1;
      record.SetMetadata(Metadata(txn.remaster().new_master(), new_counter));
      storage_->Write(key, record);

      state.txn_holder->SetRemasterResult(key, new_counter);
      break;
    }
    default:
      LOG(FATAL) << "Procedure is not set";
  }
  state.phase = TransactionState::Phase::FINISH;
}

void Worker::Finish(const RunId& run_id) {
  auto& state = TxnState(run_id);
  auto txn = state.txn_holder->FinalizeAndRelease();

  RECORD(txn->mutable_internal(), TransactionEvent::EXIT_WORKER);

  // Send the txn back to the coordinating server if it is in the same replica.
  // This must happen before the sending to scheduler below. Otherwise,
  // the scheduler may destroy the transaction holder before we can
  // send the transaction to the server.
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
  *msg.data<TxnId>() = run_id.first;
  GetCustomSocket(0).send(msg, zmq::send_flags::none);

  // Done with this txn. Remove it from the state map
  txn_states_.erase(run_id);

  VLOG(3) << "Finished with txn " << run_id;
}

void Worker::BroadcastReads(const RunId& run_id) {
  auto& state = TxnState(run_id);
  auto txn_holder = state.txn_holder;
  auto& txn = txn_holder->txn();

#ifdef LOCK_MANAGER_DDR
  // If DDR is used, all partitions have to wait
  const auto& waiting_partitions = txn.internal().involved_partitions();
#else
  const auto& waiting_partitions = txn.internal().active_partitions();
#endif

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

  auto aborted = txn.status() == TransactionStatus::ABORTED;

  // Send abort result and local reads to all remote active partitions
  Envelope env;
  auto rrr = env.mutable_request()->mutable_remote_read_result();
  rrr->set_txn_id(run_id.first);
  rrr->set_deadlocked(run_id.second);
  rrr->set_partition(local_partition);
  rrr->set_will_abort(aborted);
  rrr->set_abort_code(txn.abort_code());
  rrr->set_abort_reason(txn.abort_reason());
  if (!aborted) {
    auto reads_to_be_sent = rrr->mutable_reads();
    for (const auto& kv : txn.keys()) {
      reads_to_be_sent->Add()->CopyFrom(kv);
    }
  }

  Send(env, destinations, MakeTag(run_id));
}

TransactionState& Worker::TxnState(const RunId& run_id) {
  auto state_it = txn_states_.find(run_id);
  CHECK(state_it != txn_states_.end());
  return state_it->second;
}

void Worker::StartRedirection(const RunId& run_id) {
  auto redirect_env = NewEnvelope();
  redirect_env->mutable_request()->mutable_broker_redirect()->set_tag(MakeTag(run_id));
  redirect_env->mutable_request()->mutable_broker_redirect()->set_channel(channel());
  Send(move(redirect_env), Broker::MakeChannel(config()->broker_ports_size() - 1));
}

void Worker::StopRedirection(const RunId& run_id) {
  auto redirect_env = NewEnvelope();
  redirect_env->mutable_request()->mutable_broker_redirect()->set_tag(MakeTag(run_id));
  redirect_env->mutable_request()->mutable_broker_redirect()->set_stop(true);
  Send(move(redirect_env), Broker::MakeChannel(config()->broker_ports_size() - 1));
}

}  // namespace slog