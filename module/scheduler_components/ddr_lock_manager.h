#pragma once

// Prevent mixing with other versions
#ifdef LOCK_MANAGER
#error "Only one lock manager can be included"
#endif
#define LOCK_MANAGER

#include <atomic>
#include <list>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <zmq.hpp>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/metrics.h"
#include "common/spin_latch.h"
#include "common/types.h"
#include "module/base/networked_module.h"
#include "module/scheduler_components/txn_holder.h"

namespace slog {

/**
 * An object of this class represents the tail of the lock queue.
 * We don't update this structure when a transaction releases its
 * locks. Therefore, this structure might contain released transactions
 * so we need to verify any result returned from it.
 */
class LockQueueTail {
 public:
  std::optional<TxnId> AcquireReadLock(TxnId txn_id);
  std::vector<TxnId> AcquireWriteLock(TxnId txn_id);

  /* For debugging */
  std::optional<TxnId> write_lock_requester() const { return write_lock_requester_; }

  /* For debugging */
  std::vector<TxnId> read_lock_requesters() const { return read_lock_requesters_; }

 private:
  std::optional<TxnId> write_lock_requester_;
  std::vector<TxnId> read_lock_requesters_;
};

class DeadlockResolver;

/**
 * This is a deterministic lock manager which grants locks for transactions
 * in the order that they request. If transaction X, appears before
 * transaction Y in the log, X always gets all locks before Y.
 *
 * DDR stands for Deterministic Deadlock Resolving. This lock manager is
 * remaster-aware like the RMA lock manager. However, for each lock wait
 * queue, it only keeps track of the tail of the queue. The dependencies
 * between the txns are tracked in a graph, which can be used to deterministically
 * detect and resolve deadlocks.
 *
 * Transactions coming into this lock manager must have unique ids. For example,
 * after txn 100 acquires and releases its locks, the txn id 100 cannot be used
 * again for any future txns coming into this lock manager.
 *
 * Remastering:
 * Locks are taken on the tuple <key, region>, using the transaction's
 * master metadata. The masters are checked in the worker, so if two
 * transactions hold separate locks for the same key, then one has an
 * incorrect master and will be aborted. Remaster transactions request the
 * locks for both <key, old region> and <key, new region>.
 */
class DDRLockManager {
 public:
  DDRLockManager();

  /**
   * Initializes the deadlock resolver
   * @param broker       A broker to help the resolver to send/receive external messages
   * @param signal_chan  Channel to receive signal from the deadlock resolver when there are new ready txns after
   *                     resolving deadlocks
   * @param poll_timeout Timeout for polling in the resolver
   */
  void InitializeDeadlockResolver(const std::shared_ptr<Broker>& broker,
                                  const MetricsRepositoryManagerPtr& metrics_manager, Channel signal_chan,
                                  std::optional<std::chrono::milliseconds> poll_timeout = kModuleTimeout);

  /**
   * Starts the deadlock resolver in a new thread
   */
  void StartDeadlockResolver();

  /**
   * Runs the deadlock resolving algorithm manually. Return false if the resolver is not initialized yet or
   * it is already running in a background thread. For testing only.
   */
  bool ResolveDeadlock(bool dont_recv_remote_msg = false);

  /**
   * Gets the list of txns that become ready after resolving deadlocks
   */
  std::vector<TxnId> GetReadyTxns();

  /**
   * Tries to acquire all locks for a given transaction. If not
   * all locks are acquired, the transaction is queued up to wait
   * for the current holders to release.
   *
   * @param txn The transaction whose locks are acquired.
   * @return    true if all locks are acquired, false if not and
   *            the transaction is queued up.
   */
  AcquireLocksResult AcquireLocks(const Transaction& txn);

  /**
   * Releases all locks that a transaction is holding or waiting for.
   *
   * @param txn_holder Holder of the transaction whose locks are released.
   *            LockOnly txn is not accepted.
   * @return    A list of <txn_id, deadlocked>, where txn_id is the id of
   *            the txn being unblocked and deadlocked indicates whether
   *            the txn was in a deadlock.
   */
  std::vector<std::pair<TxnId, bool>> ReleaseLocks(TxnId txn_id);

  /**
   * Gets current statistics of the lock manager
   *
   * @param stats A JSON object where the statistics are stored into
   */
  void GetStats(rapidjson::Document& stats, uint32_t level) const;

 private:
  friend class DeadlockResolver;

  struct TxnInfo {
    TxnInfo(TxnId txn_id, int unarrived)
        : id(txn_id), num_waiting_for(0), unarrived_lock_requests(unarrived), deadlocked(false) {
      // Add an empty slot in case the deadlock resolver need to add a new edge but there is no
      // existing slot to replace. This happens when there is no edge coming out of the current txn
      // at the current partition but there are such edges in other partitions. If this list does not
      // have any element, there will be no slot to fill the new edge in.
      waited_by.push_back(kSentinelTxnId);
    }

    const TxnId id;
    // This list must only grow
    std::vector<TxnId> waited_by;
    int num_waiting_for;
    int unarrived_lock_requests;
    bool deadlocked;

    bool is_ready() const { return num_waiting_for == 0 && unarrived_lock_requests == 0; }
  };

  std::unordered_map<KeyRegion, LockQueueTail> lock_table_;
  std::unordered_map<TxnId, TxnInfo> txn_info_;
  mutable SpinLatch txn_info_latch_;

  class LogEntry {
   public:
    LogEntry() : txn_id_(0), num_partitions_(0), is_complete_(false) {}

    LogEntry(TxnId txn_id, int num_partitions, bool is_complete, const std::vector<TxnId>& incoming_edges)
        : txn_id_(txn_id),
          num_partitions_(num_partitions),
          is_complete_(is_complete),
          incoming_edges_(incoming_edges) {}

    LogEntry(LogEntry&& other)
        : txn_id_(other.txn_id_),
          num_partitions_(other.num_partitions_),
          is_complete_(other.is_complete_),
          incoming_edges_(std::move(other.incoming_edges_)) {}

    LogEntry* operator=(LogEntry&& other) {
      txn_id_ = other.txn_id_;
      num_partitions_ = other.num_partitions_;
      is_complete_ = other.is_complete_;
      incoming_edges_ = std::move(other.incoming_edges_);
      return this;
    }

    TxnId txn_id() const { return txn_id_; }
    int num_partitions() const { return num_partitions_; }
    bool is_complete() const { return is_complete_; }
    const std::vector<TxnId>& incoming_edges() const { return incoming_edges_; }

   private:
    TxnId txn_id_;
    int num_partitions_;
    bool is_complete_;
    std::vector<TxnId> incoming_edges_;
  };
  std::vector<LogEntry> log_[2];
  int log_index_ = 0;
  SpinLatch log_latch_;

  std::vector<TxnId> ready_txns_;
  SpinLatch ready_txns_latch_;

  // For stats
  std::atomic<long> num_deadlocks_resolved_ = 0;

  // This must defined the end so that it is destroyed before the shared resources
  std::unique_ptr<ModuleRunner> dl_resolver_;
};

}  // namespace slog