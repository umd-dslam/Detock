/*
  For a new transaction T, the scheduler create a txn holder to keep track additional information
  relating to T. After acquiring all the necessary locks, T is dispatched to a worker, which will
  assume ownership of T from that point. Scheduler makes sure that it does not access T during this
  time. On completion, the worker frees up T's memory and signals the scheduler about T's completion
  so that the scheduler can unblocks other transactions waiting on T, if any.

  The scheduler may dispatch T before the deadlock resolver detects a deadlock involving T. This case
  happens in a distributed deadlock situation. For example, say there are two partitions owning the keys {A} and
  {B}. A and B are mastered by 2 different regions. Let T1 and T2 be 2 transactions that write to both A and B.
  It is possible that on the first partition, T1 is before T2 in the log, however in the second partition,
  T2 is before T1. In the first partition, T1 enters the scheduler and is able to get all the locks then
  dispatched to a worker; then the second partition sends the T2 -> T1 edge to the first partition, which
  detect a deadlock after T1 has already been dispatched.

  In such a situation, the scheduler redispatch T1 to the same worker. That workers now has 2 "dispatches"
  of T1, which are differentiated by the flag 'deadlocked': it is set to false for the first dispatch and
  true for the second dispatch. A worker also includes this flag when it sends its reads to other partitions,
  and the remote reads will only be applied to the dispatch with the same 'deadlocked' flag.
  In a distributed deadlock, the first dispatch will never get all of its remote reads (otherwise, there is no deadlock
  to begin with) and therefore will never finish. The second dispatch will replace the first dispatch and get all
  of its remote reads.
 */

#include "module/scheduler.h"

#include <algorithm>

#include "common/json_utils.h"
#include "common/proto_utils.h"
#include "common/types.h"
#include "proto/internal.pb.h"

using std::make_shared;
using std::move;
using std::shared_ptr;
using std::chrono::milliseconds;

namespace slog {

using internal::Request;
using internal::Response;

Scheduler::Scheduler(const shared_ptr<Broker>& broker, const shared_ptr<Storage>& storage,
                     const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, {kSchedulerChannel, false /* is_raw */}, metrics_manager, poll_timeout),
      current_worker_(0),
      global_log_counter_(0) {
  for (int i = 0; i < config()->num_workers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(i, broker, storage, metrics_manager, poll_timeout));
  }

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  remaster_manager_.SetStorage(storage);
#endif

#ifdef LOCK_MANAGER_DDR
  if (config()->ddr_interval() > milliseconds(0)) {
    lock_manager_.InitializeDeadlockResolver(broker, metrics_manager, kSchedulerChannel, poll_timeout);
  }
#endif
}

void Scheduler::Initialize() {
#ifdef LOCK_MANAGER_DDR
  lock_manager_.StartDeadlockResolver();
#endif

  auto cpus = config()->cpu_pinnings(ModuleId::WORKER);
  size_t i = 0;
  for (auto& worker : workers_) {
    std::optional<uint32_t> cpu = {};
    if (i < cpus.size()) {
      cpu = cpus[i];
    }
    worker->StartInNewThread(cpu);

    zmq::socket_t worker_socket(*context(), ZMQ_PAIR);
    worker_socket.set(zmq::sockopt::rcvhwm, 0);
    worker_socket.set(zmq::sockopt::sndhwm, 0);
    worker_socket.bind(kSchedWorkerAddress + std::to_string(i));

    AddCustomSocket(move(worker_socket));

    i++;
  }
}

void Scheduler::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kForwardTxn:
      ProcessTransaction(move(env));
      break;
#ifdef LOCK_MANAGER_DDR
    case Request::kSignal: {
      auto ready_txns = lock_manager_.GetReadyTxns();
      for (auto ready_txn : ready_txns) {
        auto it = active_txns_.find(ready_txn);
        DCHECK(it != active_txns_.end());

        VLOG(3) << "Txn " << TXN_ID_STR(ready_txn) << " became ready after resolving a deadlock";

        // The ready txns list contains txns that got into a resolved deadlock
        Dispatch(ready_txn, true /* deadlocked */, false /* is_fast */);
      }
      break;
    }
#endif
    case Request::kStats:
      ProcessStatsRequest(env->request().stats());
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
      break;
  }
}

// Handle responses from the workers
bool Scheduler::OnCustomSocket() {
  bool has_msg = false;
  bool stop = false;
  while (!stop) {
    stop = true;
    for (size_t i = 0; i < workers_.size(); i++) {
      if (zmq::message_t msg; GetCustomSocket(i).recv(msg, zmq::recv_flags::dontwait)) {
        stop = false;
        has_msg = true;
        auto txn_id = *msg.data<TxnId>();
        // Release locks held by this txn then dispatch the txns that become ready thanks to this release.
        auto unblocked_txns = lock_manager_.ReleaseLocks(txn_id);
        VLOG(3) << "Released locks of txn " << TXN_ID_STR(txn_id);

        for (auto unblocked_txn : unblocked_txns) {
#ifdef LOCK_MANAGER_DDR
          auto it = active_txns_.find(unblocked_txn.first);
          DCHECK(it != active_txns_.end());
          Dispatch(unblocked_txn.first, unblocked_txn.second /* deadlocked */, false /* is_fast */);
#else
          Dispatch(unblocked_txn, false /* deadlocked */, false /* is_fast */);
#endif
        }

        auto it = active_txns_.find(txn_id);
        CHECK(it != active_txns_.end());
        auto& txn_holder = it->second;

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
        auto remaster_result = txn_holder.remaster_result();
        // If a remaster transaction, trigger any unblocked txns
        if (remaster_result.has_value()) {
          ProcessRemasterResult(remaster_manager_.RemasterOccured(remaster_result->first, remaster_result->second));
        }
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || \
          defined(REMASTER_PROTOCOL_PER_KEY) */

        txn_holder.SetDone();

        if (txn_holder.is_ready_for_gc()) {
          active_txns_.erase(it);
        }
      }
    }
  };

  return has_msg;
}

void Scheduler::ProcessTransaction(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->release_txn();
  auto txn_id = txn->internal().id();
  auto ins = active_txns_.try_emplace(txn_id, config(), txn);
  auto holder_it = ins.first;
  auto& holder = holder_it->second;

  global_log_counter_++;

  if (ins.second) {
    RECORD(holder.txn().mutable_internal(), TransactionEvent::ENTER_SCHEDULER);

    VLOG(3) << "Accepted " << ENUM_NAME(txn->internal().type(), TransactionType) << " transaction ("
            << TXN_ID_STR(txn_id) << ", " << txn->internal().home() << ")";
  } else {
    if (!holder.AddLockOnlyTxn(txn)) {
      LOG(ERROR) << "Already received txn: (" << TXN_ID_STR(txn_id) << ", " << txn->internal().home() << ")";
      return;
    }

    if (holder.is_aborting()) {
      if (holder.is_ready_for_gc()) {
        active_txns_.erase(holder_it);
      }
      return;
    }

    RECORD(holder.txn().mutable_internal(), TransactionEvent::ENTER_SCHEDULER_LO);

    VLOG(3) << "Added " << ENUM_NAME(txn->internal().type(), TransactionType) << " transaction (" << TXN_ID_STR(txn_id)
            << ", " << txn->internal().home() << ")";
  }

  holder.txn().mutable_internal()->add_global_log_positions(txn->internal().home());
  holder.txn().mutable_internal()->add_global_log_positions(global_log_counter_);

  if (txn->status() == TransactionStatus::ABORTED) {
    TriggerPreDispatchAbort(txn_id, txn->abort_reason());
    return;
  }

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  SendToRemasterManager(*txn);
#else
  SendToLockManager(*txn);
#endif
}

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
void Scheduler::SendToRemasterManager(Transaction& txn) {
  switch (remaster_manager_.VerifyMaster(txn)) {
    case VerifyMasterResult::VALID: {
      SendToLockManager(txn);
      break;
    }
    case VerifyMasterResult::ABORT: {
      TriggerPreDispatchAbort(txn.internal().id(), "failed remaster validation");
      break;
    }
    case VerifyMasterResult::WAITING: {
      VLOG(3) << "Txn waiting on remaster: " << TXN_ID_STR(txn.internal().id());
      // Do nothing
      break;
    }
    default:
      LOG(ERROR) << "Unknown VerifyMaster type";
      break;
  }
}

void Scheduler::ProcessRemasterResult(RemasterOccurredResult result) {
  for (auto unblocked_lo : result.unblocked) {
    SendToLockManager(*unblocked_lo);
  }
  // Check for duplicates
  // TODO: remove this set and check
  unordered_set<TxnId> aborting_txn_ids;
  for (auto unblocked_lo : result.should_abort) {
    aborting_txn_ids.insert(unblocked_lo->internal().id());
  }
  CHECK_EQ(result.should_abort.size(), aborting_txn_ids.size()) << "Duplicate transactions returned for abort";
  for (auto txn_id : aborting_txn_ids) {
    TriggerPreDispatchAbort(txn_id);
  }
}
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || \
          defined(REMASTER_PROTOCOL_PER_KEY) */

void Scheduler::SendToLockManager(Transaction& txn) {
  auto txn_id = txn.internal().id();

  VLOG(3) << "Trying to acquires locks of txn " << TXN_ID_STR(txn_id);

  RECORD(txn.mutable_internal(), TransactionEvent::ENTER_LOCK_MANAGER);

  switch (lock_manager_.AcquireLocks(txn)) {
    case AcquireLocksResult::ACQUIRED:
      Dispatch(txn_id, false /* deadlocked */, true /* is_fast */);
      break;
    case AcquireLocksResult::ABORT:
      TriggerPreDispatchAbort(txn_id);
      break;
    case AcquireLocksResult::WAITING:
      VLOG(3) << "Txn " << TXN_ID_STR(txn_id) << " cannot be dispatched yet";
      break;
    default:
      LOG(ERROR) << "Unknown lock result type";
      break;
  }
}

void Scheduler::Dispatch(TxnId txn_id, bool deadlocked, bool is_fast) {
  auto it = active_txns_.find(txn_id);
  CHECK(it != active_txns_.end()) << "Txn " << txn_id << " does not exist for dispatching";
  auto& txn_holder = it->second;

  CHECK(txn_holder.dispatchable()) << "Can no longer dispatch txn " << txn_holder.txn_id()
                                   << " (deadlocked = " << deadlocked << ")";
  // If we did not detect a deadlock in the first dispatch, there is still a second chance to dispatch after a deadlock
  // is found. There will be no other chance to dispatch after that.
  if (deadlocked) {
    txn_holder.SetUndispatchable();
  }

  txn_holder.IncNumDispatches();

  if (is_fast) {
    RECORD(txn_holder.txn().mutable_internal(), TransactionEvent::DISPATCHED_FAST);
  } else {
    if (deadlocked) {
      RECORD(txn_holder.txn().mutable_internal(), TransactionEvent::DISPATCHED_SLOW_DEADLOCKED);
    } else {
      RECORD(txn_holder.txn().mutable_internal(), TransactionEvent::DISPATCHED_SLOW);
    }
  }
  auto data = std::make_pair(&txn_holder, deadlocked);
  zmq::message_t msg(sizeof(data));
  *msg.data<decltype(data)>() = data;

  int worker = current_worker_;
  current_worker_ = (current_worker_ + 1) % workers_.size();
  // If this txn was dispatched to a worker before, dispatch to the same worker again
  if (txn_holder.worker().has_value()) {
    worker = txn_holder.worker().value();
  }
  txn_holder.SetWorker(worker);
  GetCustomSocket(worker).send(msg, zmq::send_flags::none);

  VLOG(3) << "Dispatched txn " << TXN_ID_STR(txn_id) << " (deadlocked = " << deadlocked << ")";
}

// Disable pre-dispatch abort when DDR is used. Removing this method is sufficient to disable the
// whole mechanism
#ifdef LOCK_MANAGER_DDR
void Scheduler::TriggerPreDispatchAbort(TxnId, const std::string&) {}
#else
void Scheduler::TriggerPreDispatchAbort(TxnId txn_id, const std::string& abort_reason) {
  auto active_txn_it = active_txns_.find(txn_id);
  CHECK(active_txn_it != active_txns_.end());
  auto& txn_holder = active_txn_it->second;

  if (txn_holder.is_aborting()) {
    return;
  }

  VLOG(3) << "Triggering pre-dispatch abort of txn " << TXN_ID_STR(txn_id);

  txn_holder.SetAborting();

  auto& txn = txn_holder.txn();

#if defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY)
  // Release txn from remaster manager and lock manager.
  //
  // If the abort was triggered by a remote partition,
  // then the single-home or multi-home transaction may still
  // be in one of the managers, and needs to be removed.
  //
  // This also releases any lock-only transactions.
  ProcessRemasterResult(remaster_manager_.ReleaseTransaction(txn));
#endif /* defined(REMASTER_PROTOCOL_SIMPLE) || defined(REMASTER_PROTOCOL_PER_KEY) */

  // Release locks held by this txn. Enqueue the txns that
  // become ready thanks to this release.
  auto unblocked_txns = lock_manager_.ReleaseLocks(txn_id);
  for (auto unblocked_txn : unblocked_txns) {
    Dispatch(unblocked_txn, false, false);
  }

  // Let a worker handle notifying other partitions and send back to the server.
  txn.set_status(TransactionStatus::ABORTED);
  txn.set_abort_reason(abort_reason);
  Dispatch(txn_id, false, false);
}
#endif /* LOCK_MANAGER_DDR */

/**
 * {
 *    num_all_txns: <number of active txns>,
 *    all_txns (lvl == 0): [<txn id>, ...],
 *    all_txns (lvl >= 1): [
 *      {
 *        id: <uint64>,
 *        done: <bool>,
 *        aborting: <bool>,
 *        num_lo: <int>,
 *        expected_num_lo: <int>,
 *      },
 *      ...
 *    ],
 *    ...<stats from lock manager>...
 * }
 */
void Scheduler::ProcessStatsRequest(const internal::StatsRequest& stats_request) {
  using rapidjson::StringRef;

  int level = stats_request.level();

  rapidjson::Document stats;
  stats.SetObject();
  auto& alloc = stats.GetAllocator();

  // Add stats for current transactions in the system
  stats.AddMember(StringRef(NUM_ALL_TXNS), active_txns_.size(), alloc);
  if (level == 0) {
    stats.AddMember(StringRef(ALL_TXNS),
                    ToJsonArray(
                        active_txns_, [](const auto& p) { return p.first; }, alloc),
                    alloc);
  }

  if (level >= 1) {
    rapidjson::Value txns(rapidjson::kArrayType);
    for (const auto& [txn_id, txn_holder] : active_txns_) {
      rapidjson::Value txn_obj(rapidjson::kObjectType);
      txn_obj.AddMember(StringRef(TXN_ID), txn_id, alloc)
          .AddMember(StringRef(TXN_DONE), txn_holder.is_done(), alloc)
          .AddMember(StringRef(TXN_ABORTING), txn_holder.is_aborting(), alloc)
          .AddMember(StringRef(TXN_NUM_LO), txn_holder.num_lock_only_txns(), alloc)
          .AddMember(StringRef(TXN_EXPECTED_NUM_LO), txn_holder.expected_num_lock_only_txns(), alloc)
          .AddMember(StringRef(TXN_NUM_DISPATCHES), txn_holder.num_dispatches(), alloc)
          .AddMember(StringRef(TXN_MULTI_HOME),
                     txn_holder.txn().internal().type() == TransactionType::MULTI_HOME_OR_LOCK_ONLY, alloc)
          .AddMember(StringRef(TXN_MULTI_PARTITION), txn_holder.txn().internal().involved_partitions_size() > 1, alloc);
      txns.PushBack(txn_obj, alloc);
    }
    stats.AddMember(StringRef(ALL_TXNS), txns, alloc);
  }

  // Add stats from the lock manager
  lock_manager_.GetStats(stats, level);

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