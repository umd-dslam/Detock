#include "module/scheduler_components/ddr_lock_manager.h"

#include <glog/logging.h>

#include <algorithm>

#include "connection/zmq_utils.h"

using std::lock_guard;
using std::make_pair;
using std::make_unique;
using std::move;
using std::mutex;
using std::optional;
using std::unique_ptr;

namespace slog {

/**
 * Periodically, the deadlock resolver wakes up, takes a snapshot of the dependency graph,
 * deterministically resolve the deadlocks, if any, and applies any changes to the original graph.
 * It finds strongly connected components in the graph and only resolves the "stable" components.
 * The original graph might still grow while the resolver is running so care must be taken such that
 * we don't remove new addition of the orignal graph while applying back the modified-but-outdated
 * snapshot.
 *
 * We keep track the dependencies with respect to a txn via its waited-by list and waiting-for counter.
 * For all txns in a "stable" component, it is guaranteed that the waiting-for counter will never change
 * and the waited-by list will only grow. Therefore, it is safe to the resolver to make any change to
 * the waiting-for counter, and the snapshotted prefix of the waited-by list.
 */
class DeadlockResolver : public Module {
  enum class ComponentType { UNASSIGNED, STABLE, UNSTABLE };
  struct Node {
    explicit Node(TxnId txn_id, bool is_complete)
        : id(txn_id), is_complete(is_complete), visited(false), comp_type(ComponentType::UNASSIGNED) {}

    const TxnId id;
    const bool is_complete;

    vector<TxnId> redges;
    bool visited;
    ComponentType comp_type;
  };

 public:
  DeadlockResolver(DDRLockManager& lock_manager, zmq::context_t& context, Channel signal_chan,
                   milliseconds check_interval)
      : Module("DeadlockResolver"),
        lm_(lock_manager),
        signal_(context, ZMQ_PUSH),
        signal_chan_(signal_chan),
        check_interval_(check_interval) {}

  void SetUp() final { signal_.connect(MakeInProcChannelAddress(signal_chan_)); }

  bool Loop() final {
    std::this_thread::sleep_for(check_interval_);

    // Take a snapshot of the txn dependency graph
    {
      lock_guard<mutex> guard(lm_.mut_txn_info_);
      txn_info_ = lm_.txn_info_;
    }

    // The resolver uses num_waiting_for field to record the amount that the actual value of
    // num_waiting_for in the lock manager would increase/decrease after resolving deadlocks
    for (auto& it : txn_info_) {
      it.second.num_waiting_for = 0;
    }

    // Find topological order and build the tranpose graph
    topo_order_.clear();
    aux_graph_.clear();
    for (auto& it : txn_info_) {
      auto txn = it.second;
      auto ins = aux_graph_.try_emplace(txn.id, txn.id, txn.is_complete());
      auto& node = ins.first->second;
      if (!node.visited) {
        node.visited = true;
        FindTopoOrderAndTranspose(txn);
      }
    }
    std::reverse(topo_order_.begin(), topo_order_.end());

    vector<TxnId> to_be_updated;
    int num_sscs = 0;
    // Form the strongly connected components. This time, We traverse on
    // the tranpose graph. For each stable component with more than 1 member,
    // perform deterministic deadlock resolving
    for (auto txn_id : topo_order_) {
      auto it = aux_graph_.find(txn_id);
      CHECK(it != aux_graph_.end()) << "Topo order contains unknown txn: " << txn_id;

      auto& node = it->second;
      if (node.comp_type == ComponentType::UNASSIGNED) {
        ssc_.clear();
        auto is_stable = FormStronglyConnectedComponent(node);
        if (!is_stable) {
          // Set all nodes in the current component to unstable
          for (auto id : ssc_) {
            auto node_it = aux_graph_.find(id);
            DCHECK(node_it != aux_graph_.end());
            node_it->second.comp_type = ComponentType::UNSTABLE;
          }
        } else if (ssc_.size() > 1) {
          // If this component is stable and has more than 1 element, resolve the deadlock
          ResolveDeadlock();
          // The info of txns in this scc will be updated in the lock manager
          to_be_updated.insert(to_be_updated.end(), ssc_.begin(), ssc_.end());
          num_sscs++;
        }
      }
    }

    vector<TxnId> ready_txns;
    // Update the txn info table in the lock manager with deadlock-free dependencies if needed
    if (!to_be_updated.empty()) {
      lock_guard<mutex> guard(lm_.mut_txn_info_);

      for (auto txn_id : to_be_updated) {
        auto new_txn_it = txn_info_.find(txn_id);
        DCHECK(new_txn_it != txn_info_.end());
        auto& new_txn = new_txn_it->second;

        auto txn_it = lm_.txn_info_.find(txn_id);
        DCHECK(txn_it != lm_.txn_info_.end());
        auto& txn = txn_it->second;

        // Replace the prefix of the waited-by list by the deadlock-resolved waited-by list
        std::copy(new_txn.waited_by.begin(), new_txn.waited_by.end(), txn.waited_by.begin());
        // The resolver stores the amount that num_waiting_for increases/decreases so it is
        // added here instead of assigning
        txn.num_waiting_for += new_txn.num_waiting_for;
        // Check if any txn becomes ready after deadlock resolving. This must be performed
        // in this critical region. Otherwise, we might run into a race condition where both
        // the resolver and lock manager see num_waiting_for as larger than 0, while it is not
        // because they work on two different snapshots of the txn_info table
        if (txn.is_ready()) {
          ready_txns.push_back(txn_id);
        }
      }
    }

    if (!ready_txns.empty()) {
      // Update the ready txns list in the lock manager
      {
        lock_guard<mutex> guard(lm_.mut_ready_txns_);
        lm_.ready_txns_.insert(lm_.ready_txns_.end(), ready_txns.begin(), ready_txns.end());
      }

      // Send signal that there are new ready txns
      auto env = make_unique<internal::Envelope>();
      env->mutable_request()->mutable_signal();
      SendEnvelope(signal_, move(env));
    }

    if (num_sscs) {
      VLOG(3) << "Deadlock group(s) found and resolved: " << num_sscs;
      if (ready_txns.empty()) {
        VLOG(3) << "No txn becomes ready after resolving deadlock";
      } else {
        VLOG(3) << "New ready txns after resolving deadlocks: " << ready_txns.size();
      }
    } else {
      VLOG_EVERY_N(4, 100) << "No stable deadlock found";
    }
    return false;
  }

 private:
  void FindTopoOrderAndTranspose(TxnInfo& txn) {
    for (auto n : txn.waited_by) {
      if (n == kSentinelTxnId) {
        continue;
      }

      auto it = txn_info_.find(n);
      CHECK(it != txn_info_.end()) << "Corrupted graph. Unknown txn: " << n;

      auto& neighbor = it->second;
      // Build tranpose graph
      auto ins = aux_graph_.try_emplace(neighbor.id, neighbor.id, neighbor.is_complete());
      auto& node = ins.first->second;
      node.redges.push_back(txn.id);
      if (!node.visited) {
        node.visited = true;
        FindTopoOrderAndTranspose(neighbor);
      }
    }
    // Establish topological order
    topo_order_.push_back(txn.id);
  }

  // Returns true if the component is stable
  bool FormStronglyConnectedComponent(Node& node) {
    ssc_.push_back(node.id);
    // Assume the current component is stable
    node.comp_type = ComponentType::STABLE;

    bool is_stable = node.is_complete;
    for (auto n : node.redges) {
      auto it = aux_graph_.find(n);
      CHECK(it != aux_graph_.end()) << "Corrupted auxiliary graph. Unknown node: " << n;

      auto& neighbor = it->second;
      if (neighbor.comp_type == ComponentType::UNASSIGNED) {
        is_stable &= FormStronglyConnectedComponent(neighbor);
      } else if (neighbor.comp_type == ComponentType::UNSTABLE) {
        is_stable = false;
      }
    }
    return is_stable;
  }

  void ResolveDeadlock() {
    DCHECK_GE(ssc_.size(), 2);

    std::sort(ssc_.begin(), ssc_.end());
    for (int i = ssc_.size() - 1; i >= 0; --i) {
      auto txn_it = txn_info_.find(ssc_[i]);
      CHECK(txn_it != txn_info_.end()) << "SSC contains unknown txn: " << ssc_[i];
      auto& txn = txn_it->second;
      CHECK(txn.is_complete()) << "SSC contains incomplete txn: " << ssc_[i];

      // Don't add edge if this is the last element of in the component list
      bool new_edge_added = static_cast<size_t>(i) == ssc_.size() - 1;
      for (size_t j = 0; j < txn.waited_by.size(); j++) {
        if (std::binary_search(ssc_.begin(), ssc_.end(), txn.waited_by[j])) {
          auto waiting_txn = txn_info_.find(txn.waited_by[j]);
          CHECK(waiting_txn != txn_info_.end());
          if (!new_edge_added) {
            txn.waited_by[j] = ssc_[i + 1];
            // Since i goes in reverse order, ssc_[i + 1] is already checked for
            // existence and stuff at this point
            ++(txn_info_.find(ssc_[i + 1])->second.num_waiting_for);
            new_edge_added = true;
          } else {
            // Setting to kSentinelTxnId effectively removes this edge
            txn.waited_by[j] = kSentinelTxnId;
          }
          --waiting_txn->second.num_waiting_for;
        }
      }
      // There is at least one waited-by txn for each txn in an ssc so there
      // must be one empty slot for the new edge
      CHECK(new_edge_added) << "Cannot find slot to add new edge";
    }
  }

  DDRLockManager& lm_;
  zmq::socket_t signal_;
  Channel signal_chan_;
  milliseconds check_interval_;

  unordered_map<TxnId, TxnInfo> txn_info_;
  unordered_map<TxnId, Node> aux_graph_;
  vector<TxnId> topo_order_;
  vector<TxnId> ssc_;
};

optional<TxnId> LockQueueTail::AcquireReadLock(TxnId txn_id) {
  read_lock_requesters_.push_back(txn_id);
  return write_lock_requester_;
}

vector<TxnId> LockQueueTail::AcquireWriteLock(TxnId txn_id) {
  vector<TxnId> deps;
  if (read_lock_requesters_.empty()) {
    if (write_lock_requester_.has_value()) {
      deps.push_back(write_lock_requester_.value());
    }
  } else {
    deps.insert(deps.end(), read_lock_requesters_.begin(), read_lock_requesters_.end());
    read_lock_requesters_.clear();
  }
  write_lock_requester_ = txn_id;
  return deps;
}

void DDRLockManager::StartDeadlockResolver(zmq::context_t& context, Channel signal_chan, milliseconds check_interval,
                                           bool init_only) {
  dl_resolver_ = MakeRunnerFor<DeadlockResolver>(*this, context, signal_chan, check_interval);
  if (!init_only) {
    dl_resolver_->StartInNewThread();
  }
}

bool DDRLockManager::ResolveDeadlock() {
  if (!dl_resolver_ || dl_resolver_->is_running()) {
    return false;
  }
  dl_resolver_->StartOnce();
  return true;
}

vector<TxnId> DDRLockManager::GetReadyTxns() {
  lock_guard<mutex> guard(mut_ready_txns_);
  auto ret = ready_txns_;
  ready_txns_.clear();
  return ret;
}

AcquireLocksResult DDRLockManager::AcquireLocks(const Transaction& txn) {
  auto txn_id = txn.internal().id();
  auto home = txn.internal().home();
  auto is_remaster = txn.procedure_case() == Transaction::kRemaster;
  int num_relevant_locks = 0;

  vector<TxnId> blocking_txns;
  for (const auto& kv : txn.keys()) {
    if (!is_remaster && static_cast<int>(kv.second.metadata().master()) != home) {
      continue;
    }
    ++num_relevant_locks;

    auto key_replica = MakeKeyReplica(kv.first, home);
    auto& lock_queue_tail = lock_table_[key_replica];

    switch (kv.second.type()) {
      case KeyType::READ: {
        auto b_txn = lock_queue_tail.AcquireReadLock(txn_id);
        if (b_txn.has_value()) {
          blocking_txns.push_back(b_txn.value());
        }
        break;
      }
      case KeyType::WRITE: {
        auto b_txns = lock_queue_tail.AcquireWriteLock(txn_id);
        blocking_txns.insert(blocking_txns.begin(), b_txns.begin(), b_txns.end());
        break;
      }
      default:
        LOG(FATAL) << "Invalid lock mode";
    }
  }

  // Deduplicate the blocking txns list. We throw away this list eventually
  // so there is no need to erase the extra values at the tail
  std::sort(blocking_txns.begin(), blocking_txns.end());
  auto last = std::unique(blocking_txns.begin(), blocking_txns.end());

  {
    lock_guard<mutex> guard(mut_txn_info_);
    // A remaster txn only has one key K but it acquires locks on (K, RO) and (K, RN)
    // where RO and RN are the old and new region respectively.
    auto ins = txn_info_.try_emplace(txn_id, txn_id, is_remaster ? 2 : txn.keys_size());
    auto& txn_info = ins.first->second;
    txn_info.unarrived_lock_requests -= num_relevant_locks;
    // Add current txn to the waited_by list of each blocking txn
    for (auto b_txn = blocking_txns.begin(); b_txn != last; b_txn++) {
      if (*b_txn == txn_id) {
        continue;
      }
      // The txns returned from the lock table might already leave
      // the lock manager so we need to check for their existence here
      auto b_txn_info = txn_info_.find(*b_txn);
      if (b_txn_info == txn_info_.end()) {
        continue;
      }
      // Let A be a blocking txn of a multi-home txn B. It is possible that
      // two lock-only txns of B both sees A and A is double counted here.
      // However, B is also added twice in the waited_by list of A. Therefore,
      // on releasing A, num_waiting_for of B is correctly subtracted.
      txn_info.num_waiting_for++;
      b_txn_info->second.waited_by.emplace_back(txn_id);
    }
    if (txn_info.is_ready()) {
      return AcquireLocksResult::ACQUIRED;
    }
    return AcquireLocksResult::WAITING;
  }
}

vector<TxnId> DDRLockManager::ReleaseLocks(const Transaction& txn) {
  auto txn_id = txn.internal().id();
  {
    lock_guard<mutex> guard(mut_txn_info_);

    auto txn_info_it = txn_info_.find(txn_id);
    if (txn_info_it == txn_info_.end()) {
      return {};
    }
    auto& txn_info = txn_info_it->second;
    if (!txn_info.is_ready()) {
      LOG(FATAL) << "Releasing unready txn is forbidden";
    }
    vector<TxnId> result;
    for (auto blocked_txn_id : txn_info.waited_by) {
      if (blocked_txn_id == kSentinelTxnId) {
        continue;
      }
      auto it = txn_info_.find(blocked_txn_id);
      if (it == txn_info_.end()) {
        LOG(ERROR) << "Blocked txn " << blocked_txn_id << " does not exist";
        continue;
      }
      auto& blocked_txn = it->second;
      blocked_txn.num_waiting_for--;
      if (blocked_txn.is_ready()) {
        // While the waited_by list might contain duplicates, the blocked
        // txn only becomes ready when its last entry in the waited_by list
        // is accounted for.
        result.push_back(blocked_txn_id);
      }
    }
    txn_info_.erase(txn_id);
    return result;
  }
}

void DDRLockManager::GetStats(rapidjson::Document& stats, uint32_t level) const {
  using rapidjson::StringRef;

  auto& alloc = stats.GetAllocator();
  {
    lock_guard<mutex> guard(mut_txn_info_);
    stats.AddMember(StringRef(NUM_TXNS_WAITING_FOR_LOCK), txn_info_.size(), alloc);
    if (level >= 1) {
      // Collect number of locks waited per txn
      // TODO: Give this another name. For this lock manager, this is
      // number of txn waited, not the number of locks.
      stats.AddMember(StringRef(NUM_LOCKS_WAITED_PER_TXN),
                      ToJsonArrayOfKeyValue(
                          txn_info_, [](const auto& info) { return info.num_waiting_for; }, alloc),
                      alloc);
    }
  }

  stats.AddMember(StringRef(NUM_LOCKED_KEYS), 0, alloc);
  if (level >= 2) {
    // Collect data from lock tables
    rapidjson::Value lock_table(rapidjson::kArrayType);
    for (const auto& pair : lock_table_) {
      auto& key = pair.first;
      auto& lock_state = pair.second;
      rapidjson::Value entry(rapidjson::kArrayType);
      rapidjson::Value key_json(key.c_str(), alloc);
      entry.PushBack(key_json, alloc)
          .PushBack(lock_state.write_lock_requester().value_or(0), alloc)
          .PushBack(ToJsonArray(lock_state.read_lock_requesters(), alloc), alloc);
      lock_table.PushBack(move(entry), alloc);
    }
    stats.AddMember(StringRef(LOCK_TABLE), move(lock_table), alloc);
  }
}

}  // namespace slog