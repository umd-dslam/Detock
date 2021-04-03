#pragma once

// Prevent mixing with other versions
#ifdef LOCK_MANAGER
#error "Only one lock manager can be included"
#endif
#define LOCK_MANAGER

#include <atomic>
#include <list>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <zmq.hpp>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/txn_holder.h"
#include "common/types.h"
#include "module/base/networked_module.h"

using std::list;
using std::optional;
using std::pair;
using std::shared_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

/**
 * An object of this class represents the tail of the lock queue.
 * We don't update this structure when a transaction releases its
 * locks. Therefore, this structure might contain released transactions
 * so we need to verify any result returned from it.
 */
class LockQueueTail {
 public:
  optional<TxnId> AcquireReadLock(TxnId txn_id);
  vector<TxnId> AcquireWriteLock(TxnId txn_id);

  /* For debugging */
  optional<TxnId> write_lock_requester() const { return write_lock_requester_; }

  /* For debugging */
  vector<TxnId> read_lock_requesters() const { return read_lock_requesters_; }

 private:
  optional<TxnId> write_lock_requester_;
  vector<TxnId> read_lock_requesters_;
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
 * Locks are taken on the tuple <key, replica>, using the transaction's
 * master metadata. The masters are checked in the worker, so if two
 * transactions hold separate locks for the same key, then one has an
 * incorrect master and will be aborted. Remaster transactions request the
 * locks for both <key, old replica> and <key, new replica>.
 */
class DDRLockManager {
 public:
  /**
   * Initializes the deadlock resolver
   * @param broker       A broker to help the resolver to send/receive external messages
   * @param signal_chan  Channel to receive signal from the deadlock resolver when there are new ready txns after
   *                     resolving deadlocks
   * @param poll_timeout Timeout for polling in the resolver
   */
  void InitializeDeadlockResolver(const shared_ptr<Broker>& broker, Channel signal_chan,
                                  optional<milliseconds> poll_timeout = kModuleTimeout);

  /**
   * Starts the deadlock resolver in a new thread
   */
  void StartDeadlockResolver();

  /**
   * Runs the deadlock resolving algorithm manually. Return false if the resolver is not initialized yet or
   * it is already running in a background thread. For testing only.
   */
  bool ResolveDeadlock(bool recv_remote_message = false, bool resolve_deadlock = true);

  /**
   * Gets the list of txns that become ready after resolving deadlocks
   */
  vector<TxnId> GetReadyTxns();

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
  vector<pair<TxnId, bool>> ReleaseLocks(TxnId txn_id);

  /**
   * Gets current statistics of the lock manager
   *
   * @param stats A JSON object where the statistics are stored into
   */
  void GetStats(rapidjson::Document& stats, uint32_t level) const;

 private:
  friend class DeadlockResolver;

  struct TxnInfo {
    TxnInfo(TxnId txn_id, int num_partitions, int unarrived)
        : id(txn_id),
          num_partitions(num_partitions),
          num_waiting_for(0),
          unarrived_lock_requests(unarrived),
          deadlocked(false) {
      // Add an empty slot in case the deadlock resolver need to add a new edge
      waited_by.push_back(kSentinelTxnId);
    }

    const TxnId id;
    const int num_partitions;
    // This list must only grow
    vector<TxnId> waited_by;
    int num_waiting_for;
    int unarrived_lock_requests;
    bool deadlocked;

    bool is_stable() const { return unarrived_lock_requests == 0; }
    bool is_ready() const { return num_waiting_for == 0 && unarrived_lock_requests == 0; }
  };

  unordered_map<TxnId, TxnInfo> txn_info_;
  unordered_map<KeyReplica, LockQueueTail> lock_table_;
  mutable std::mutex mut_txn_info_;

  vector<TxnId> ready_txns_;
  std::mutex mut_ready_txns_;

  // For stats
  std::atomic<long> num_deadlocks_resolved_ = 0;

  // This must defined the end so that it is destroyed before the shared resources
  std::unique_ptr<ModuleRunner> dl_resolver_;
};

}  // namespace slog