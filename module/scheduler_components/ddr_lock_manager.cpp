#include "module/scheduler_components/ddr_lock_manager.h"

#include <glog/logging.h>

#include <algorithm>
#include <queue>
#include <stack>

#include "connection/zmq_utils.h"

using std::lock_guard;
using std::make_pair;
using std::make_unique;
using std::move;
using std::optional;
using std::queue;
using std::shared_ptr;
using std::unique_ptr;
using std::vector;
using std::chrono::milliseconds;

namespace slog {

/**
 * Periodically, the deadlock resolver wakes up, takes a snapshot of the dependency graph,
 * broadcasts the local graph to other partitions, deterministically resolves the deadlocks using the combination
 * of graphs from all partitions, and applies any changes to the original graph.
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
class DeadlockResolver : public NetworkedModule {
 public:
  DeadlockResolver(DDRLockManager& lock_manager, const shared_ptr<Broker>& broker,
                   const MetricsRepositoryManagerPtr& metrics_manager, Channel signal_chan,
                   optional<milliseconds> poll_timeout)
      : NetworkedModule(broker, kDeadlockResolverChannel, metrics_manager, poll_timeout),
        lm_(lock_manager),
        config_(broker->config()),
        signal_chan_(signal_chan) {}

  void OnInternalRequestReceived(EnvelopePtr&& env) final {
    for (const auto& e : env->request().graph_log().entries()) {
      UpdateGraph(e);
    }
  }

  void Initialize() final {
    if (config_->ddr_interval() > milliseconds(0)) {
      ScheduleNextRun();
    }
  }

  void Run() {
    auto start_time = std::chrono::steady_clock::now();

    UpdateGraphAndBroadcastChanges();

    FindSCCOrder();

    CheckAndResolveDeadlocks();

    if (per_thread_metrics_repo != nullptr) {
      auto runtime = (std::chrono::steady_clock::now() - start_time).count();
      per_thread_metrics_repo->RecordDeadlockResolverRun(runtime, unstable_graph_sz_, stable_graph_sz_,
                                                         deadlocks_resolved_, graph_update_time_);
    }
  }

  std::string name() const override { return "DeadlockResolver"; }

 private:
  DDRLockManager& lm_;
  ConfigurationPtr config_;
  Channel signal_chan_;

  struct TxnInfoUpdate {
    TxnInfoUpdate() : num_waiting_for(0) { waited_by.push_back(kSentinelTxnId); }
    int num_waiting_for = 0;
    vector<TxnId> waited_by;
  };
  // This map replicate a snapshot of the txn info map in the lock manager. Any updates
  // made by the resolver is performed on this region so that it does not conflict with
  // the lock manager while it is doing so.
  unordered_map<TxnId, TxnInfoUpdate> txn_info_updates_;

  // Metrics
  size_t unstable_graph_sz_;
  size_t stable_graph_sz_;
  size_t deadlocks_resolved_;
  uint64_t graph_update_time_;

  struct Node {
    explicit Node(TxnId id, int num_partitions)
        : id(id), num_partitions(num_partitions), num_complete(0), is_stable(false), is_visited(false) {}

    const TxnId id;
    int num_partitions;
    int num_complete;
    bool is_stable;
    bool is_visited;
    vector<TxnId> outgoing;
    vector<TxnId> incoming;
  };

  unordered_map<TxnId, Node> graph_;
  vector<TxnId> dfs_order_;
  vector<TxnId> scc_;
  vector<TxnId> to_be_updated_;

  void ScheduleNextRun() {
    NewTimedCallback(config_->ddr_interval(), [this] {
      Run();
      ScheduleNextRun();
    });
  }

  void UpdateGraphAndBroadcastChanges() {
    internal::Envelope graph_log_env;
    int log_index = 0;
    // Make the other log active so that we can read from current log without conflict
    {
      std::lock_guard<SpinLatch> guard(lm_.log_latch_);
      log_index = lm_.log_index_;
      lm_.log_index_ = 1 - lm_.log_index_;
    }
    // For each log entry, reconstruct txn_info, update the graph and add to remote message
    auto& log = lm_.log_[log_index];
    for (const auto& entry : log) {
      ReconstructLocalTxnInfo(entry);

      UpdateGraph(entry);

      auto new_entry = graph_log_env.mutable_request()->mutable_graph_log()->add_entries();
      new_entry->set_txn_id(entry.txn_id());
      new_entry->set_num_partitions(entry.num_partitions());
      new_entry->set_is_complete(entry.is_complete());
      new_entry->mutable_incoming_edges()->Add(entry.incoming_edges().begin(), entry.incoming_edges().end());
    }
    log.clear();

    vector<MachineId> other_partitions;
    other_partitions.reserve(config_->num_partitions());
    for (int p = 0; p < config_->num_partitions(); p++) {
      if (static_cast<PartitionId>(p) != config_->local_partition()) {
        other_partitions.push_back(MakeMachineId(config_->local_region(), config_->local_replica(), p));
      }
    }
    Send(move(graph_log_env), other_partitions, kDeadlockResolverChannel);

    // Collect all unstable nodes
    queue<TxnId> unstables;
    for (auto& [id, n] : graph_) {
      // Incomplete nodes are always unstable
      if (n.num_complete < n.num_partitions) {
        unstables.push(id);
        n.is_stable = false;
      } else {
        // Initially assume that all complete nodes are stable
        n.is_stable = true;
      }
      n.is_visited = false;
    }

    unstable_graph_sz_ = stable_graph_sz_ = graph_.size();
    // Nodes that can be reached from an unstable node are unstable
    while (!unstables.empty()) {
      auto it = graph_.find(unstables.front());
      CHECK(it != graph_.end());
      unstables.pop();
      stable_graph_sz_--;
      for (auto next : it->second.outgoing) {
        auto next_it = graph_.find(next);
        CHECK(next_it != graph_.end()) << "Dangling edge";
        if (next_it->second.is_stable) {
          next_it->second.is_stable = false;
          unstables.push(next);
        }
      }
    }
  }

  void ReconstructLocalTxnInfo(const DDRLockManager::LogEntry& entry) {
    txn_info_updates_.try_emplace(entry.txn_id());
    for (auto v : entry.incoming_edges()) {
      auto it = txn_info_updates_.find(v);
      // The txn at the starting end of the edge might have been removed because
      // we already determine whether it is in a deadlock or not
      if (it != txn_info_updates_.end()) {
        it->second.waited_by.push_back(entry.txn_id());
      }
      // num_waiting_for field is used to store the number of other txns that a txn is waiting
      // for. However, we don't update this field here because it will be used to record the amount that
      // the original num_waiting_for would increase/decrease after resolving deadlocks. At the end,
      // of the deadlock resolving process, this value will be added to the current num_waiting_for
      // in the lock manager
    }
  }

  template <typename LogEntry>
  void UpdateGraph(const LogEntry& entry) {
    // Create a new node if not exists
    auto ins = graph_.try_emplace(entry.txn_id(), entry.txn_id(), entry.num_partitions());
    auto& node = ins.first->second;
    // If this entry is marked complete, increase the number of complete partitions
    node.num_complete += entry.is_complete();
    // Update edges coming from other nodes to the current node
    node.incoming.insert(node.incoming.end(), entry.incoming_edges().begin(), entry.incoming_edges().end());
    // Update edges coming from current node to other nodes
    for (auto v : entry.incoming_edges()) {
      auto it = graph_.find(v);
      if (it != graph_.end()) {
        it->second.outgoing.push_back(entry.txn_id());
      }
    }
  }

  void FindSCCOrder() {
    dfs_order_.clear();
    // Do DFS iteratively to avoid stack overflow when the graph is too deep
    for (auto& [first_id, first_node] : graph_) {
      if (first_node.is_visited || !first_node.is_stable) {
        continue;
      }
      // Pair of (txn_id, whether we are done with the vertex)
      std::stack<std::pair<TxnId, bool>> st;
      st.emplace(first_id, false);
      while (!st.empty()) {
        auto [cur, done] = st.top();
        st.pop();
        if (!done) {
          auto it = graph_.find(cur);
          CHECK(it != graph_.end());
          if (!it->second.is_visited) {
            st.emplace(cur, true);
            it->second.is_visited = true;
            for (auto next : it->second.outgoing) {
              // Ignore unstable and visited nodes
              if (auto next_it = graph_.find(next);
                  next_it != graph_.end() && next_it->second.is_stable && !next_it->second.is_visited) {
                st.emplace(next, false);
              }
            }
          }
        } else {
          dfs_order_.push_back(cur);
        }
      }
    }
    std::reverse(dfs_order_.begin(), dfs_order_.end());
  }

  void CheckAndResolveDeadlocks() {
    for (auto& n : graph_) {
      n.second.is_visited = false;
    }

    to_be_updated_.clear();
    deadlocks_resolved_ = 0;
    // Form the strongly connected components. This time, We traverse on the tranpose graph.
    // For each component with more than 1 member, perform deterministic deadlock resolving
    for (auto vertex : dfs_order_) {
      auto it = graph_.find(vertex);
      CHECK(it != graph_.end()) << "SCC order contains unknown vertex: " << vertex;
      if (!it->second.is_visited) {
        FormStronglyConnectedComponent(it->second);
        if (scc_.size() > 1) {
          // If this component is stable and has more than 1 element, resolve the deadlock
          ResolveDeadlock();
          deadlocks_resolved_++;
        }
      }
      graph_.erase(it);
    }

    vector<TxnId> ready_txns;

    auto start_time = std::chrono::steady_clock::now();
    // Update the txn info table in the lock manager with deadlock-free dependencies
    if (!to_be_updated_.empty()) {
      lock_guard<SpinLatch> guard(lm_.txn_info_latch_);

      for (auto txn_id : to_be_updated_) {
        auto update_it = txn_info_updates_.find(txn_id);
        CHECK(update_it != txn_info_updates_.end());
        auto& update = update_it->second;

        auto txn_it = lm_.txn_info_.find(txn_id);
        CHECK(txn_it != lm_.txn_info_.end());
        auto& txn = txn_it->second;

        // Mark that this txn was in a deadlock
        txn.deadlocked = true;
        // Replace the prefix of the waited-by list by the deadlock-resolved waited-by list
        std::copy(update.waited_by.begin(), update.waited_by.end(), txn.waited_by.begin());
        // The resolver stores the amount that num_waiting_for increases/decreases so it is
        // added here instead of assigning
        txn.num_waiting_for += update.num_waiting_for;
        // Check if any txn becomes ready after deadlock resolving. This must be performed
        // in this critical region. Otherwise, we might run into a race condition where both
        // the resolver and lock manager see num_waiting_for as larger than 0, while it is not
        // because they work on two different snapshots of the txn_info table
        if (txn.is_ready()) {
          ready_txns.push_back(txn_id);
        }
      }
    }
    graph_update_time_ = (std::chrono::steady_clock::now() - start_time).count();

    if (!ready_txns.empty()) {
      // Update the ready txns list in the lock manager
      {
        lock_guard<SpinLatch> guard(lm_.ready_txns_latch_);
        lm_.ready_txns_.insert(lm_.ready_txns_.end(), ready_txns.begin(), ready_txns.end());
      }

      // Send signal that there are new ready txns
      auto env = NewEnvelope();
      env->mutable_request()->mutable_signal();
      Send(move(env), signal_chan_);
    }

    // Clean up txns that we already know whether it got into a deadlock or not
    for (auto txn_id : dfs_order_) {
      txn_info_updates_.erase(txn_id);
    }

    if (deadlocks_resolved_) {
      VLOG(3) << "Deadlock group(s) found and resolved: " << deadlocks_resolved_;
      if (ready_txns.empty()) {
        VLOG(3) << "No txn becomes ready after resolving deadlock";
      } else {
        VLOG(3) << "New ready txns after resolving deadlocks: " << ready_txns.size();
      }
    } else {
      VLOG_EVERY_N(4, 100) << "No stable deadlock found";
    }
  }

  void FormStronglyConnectedComponent(Node& node) {
    scc_.clear();
    std::queue<TxnId> q;
    q.push(node.id);
    node.is_visited = true;
    while (!q.empty()) {
      auto it = graph_.find(q.front());
      CHECK(it != graph_.end());
      q.pop();
      auto& node = it->second;
      scc_.push_back(node.id);
      for (auto next : node.incoming) {
        auto next_it = graph_.find(next);
        if (next_it != graph_.end()) {
          auto& next_node = next_it->second;
          CHECK(next_node.is_stable) << "All nodes in a component must be stable";
          if (!next_node.is_visited) {
            q.push(next);
            next_node.is_visited = true;
          }
        }
      }
    }
  }

  void ResolveDeadlock() {
    CHECK_GE(scc_.size(), 2);

    // Sort the SCC to ensure determinism
    std::sort(scc_.begin(), scc_.end());

    // Create edges between consecutive nodes in the scc and remove all other edges.
    // For example, if the SCC is:
    //    1 --> 4
    //    ^   ^ |
    //    |  /  |
    //    | /   v
    //    7 <-- 3
    //
    // It becomes:
    //    1 --> 3 --> 4 --> 7
    //
    // Note that we only remove edges between nodes in the same SCC.
    //

    // Find the first node that is in the local partition
    int prev_local = scc_.size() - 1;
    while (prev_local >= 0 && txn_info_updates_.find(scc_[prev_local]) == txn_info_updates_.end()) {
      --prev_local;
    }
    if (prev_local < 0) {
      return;
    }

    std::vector<std::pair<uint64_t, uint64_t>> removed, added;
    bool record_edges = config_->metric_options().deadlock_resolver_deadlock_details();
    for (int i = prev_local; i >= 0; --i) {
      auto this_txn = scc_[i];
      auto it = txn_info_updates_.find(this_txn);
      if (it == txn_info_updates_.end()) {
        continue;
      }
      auto& this_update = it->second;

      if (per_thread_metrics_repo != nullptr) {
        per_thread_metrics_repo->RecordTxnEvent(this_txn, TransactionEvent::DEADLOCK_DETECTED);
      }
      to_be_updated_.push_back(this_txn);

      // Remove old edges
      for (size_t j = 0; j < this_update.waited_by.size(); j++) {
        auto other_txn = this_update.waited_by[j];
        // Only remove edges connecting the current node to another node in the same SCC
        if (std::binary_search(scc_.begin(), scc_.end(), other_txn)) {
          auto other_update = txn_info_updates_.find(other_txn);
          CHECK(other_update != txn_info_updates_.end());

          // Setting to kSentinelTxnId effectively removes this edge
          this_update.waited_by[j] = kSentinelTxnId;

          // Decrement the incoming edge counter
          --other_update->second.num_waiting_for;

          if (record_edges) {
            removed.emplace_back(this_txn, other_txn);
          }
        }
      }

      if (i != prev_local) {
        auto other_txn = scc_[prev_local];
        // Add the new edge from this_txn to other_txn
        auto new_edge_added = false;
        for (size_t j = 0; j < this_update.waited_by.size(); j++) {
          // Add to the first empty slot
          if (this_update.waited_by[j] == kSentinelTxnId) {
            // Add new edge to the edge list of this_txn
            this_update.waited_by[j] = other_txn;
            // Update the counter of other_txn
            ++(txn_info_updates_.find(other_txn)->second.num_waiting_for);
            new_edge_added = true;

            if (record_edges) {
              added.emplace_back(this_txn, other_txn);
            }
            break;
          }
        }
        // An empty slot is always added when the txn info is initialized so
        // this should always work
        CHECK(new_edge_added) << "Cannot find slot to add new edge";
      }

      prev_local = i;
    }

    if (per_thread_metrics_repo != nullptr) {
      per_thread_metrics_repo->RecordDeadlockResolverDeadlock(scc_.size(), removed, added);
    }

    ++lm_.num_deadlocks_resolved_;
  }
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

DDRLockManager::DDRLockManager() {
  lock_table_.reserve(25000000);
  txn_info_.reserve(1000000);
}

void DDRLockManager::InitializeDeadlockResolver(const shared_ptr<Broker>& broker,
                                                const MetricsRepositoryManagerPtr& metrics_manager, Channel signal_chan,
                                                optional<milliseconds> poll_timeout) {
  dl_resolver_ = MakeRunnerFor<DeadlockResolver>(*this, broker, metrics_manager, signal_chan, poll_timeout);
}

void DDRLockManager::StartDeadlockResolver() {
  if (dl_resolver_) {
    dl_resolver_->StartInNewThread();
  }
}

// For testing only
bool DDRLockManager::ResolveDeadlock(bool dont_recv_remote_msg) {
  if (dl_resolver_ && !dl_resolver_->is_running()) {
    if (!dont_recv_remote_msg) {
      dl_resolver_->StartOnce();
    }
    std::dynamic_pointer_cast<DeadlockResolver>(dl_resolver_->module())->Run();
    return true;
  }
  return false;
}

vector<TxnId> DDRLockManager::GetReadyTxns() {
  lock_guard<SpinLatch> guard(ready_txns_latch_);
  auto ret = ready_txns_;
  ready_txns_.clear();
  return ret;
}

AcquireLocksResult DDRLockManager::AcquireLocks(const Transaction& txn) {
  auto txn_id = txn.internal().id();
  auto home = txn.internal().home();
  auto is_remaster = txn.program_case() == Transaction::kRemaster;
  // The txn may contain keys that are homed in a remote region. This variable
  // counts the keys homed in the current region.
  int num_relevant_locks = 0;

  // Collect a list of txns that are blocking the current txn
  vector<TxnId> blocking_txns;
  for (const auto& kv : txn.keys()) {
    if (!is_remaster && static_cast<int>(kv.value_entry().metadata().master()) != home) {
      continue;
    }
    ++num_relevant_locks;

    auto key_region = MakeKeyRegion(kv.key(), home);
    auto& lock_queue_tail = lock_table_[key_region];

    switch (kv.value_entry().type()) {
      case KeyType::READ: {
        auto b_txn = lock_queue_tail.AcquireReadLock(txn_id);
        if (b_txn.has_value()) {
          blocking_txns.push_back(b_txn.value());
        }
        break;
      }
      case KeyType::WRITE: {
        auto b_txns = lock_queue_tail.AcquireWriteLock(txn_id);
        blocking_txns.insert(blocking_txns.end(), b_txns.begin(), b_txns.end());
        break;
      }
      default:
        LOG(FATAL) << "Invalid lock mode";
    }
  }

  // Deduplicate the blocking txns list.
  std::sort(blocking_txns.begin(), blocking_txns.end());
  blocking_txns.erase(std::unique(blocking_txns.begin(), blocking_txns.end()), blocking_txns.end());

  AcquireLocksResult result;
  bool is_complete = false;
  {
    lock_guard<SpinLatch> guard(txn_info_latch_);
    // A remaster txn has only one key K but it acquires locks on (K, RO) and (K, RN)
    // where RO and RN are the old and new region respectively.
    auto ins = txn_info_.try_emplace(txn_id, txn_id, is_remaster ? 2 : txn.keys_size());
    auto& txn_info = ins.first->second;
    txn_info.unarrived_lock_requests -= num_relevant_locks;
    is_complete = txn_info.unarrived_lock_requests == 0;
    // Add current txn to the waited_by list of each blocking txn
    for (auto b_txn : blocking_txns) {
      // This should never happen but just to be safe
      if (b_txn == txn_id) {
        continue;
      }
      // The txns returned from the lock table might already leave
      // the lock manager so we need to check for their existence here
      auto b_txn_info = txn_info_.find(b_txn);
      if (b_txn_info == txn_info_.end()) {
        continue;
      }
      // Let A be a blocking txn of a multi-home txn B. It is possible that
      // two lock-only txns of B both are blocked by A and A is double counted here.
      // However, B is also added twice in the waited_by list of A. Therefore,
      // on releasing A, num_waiting_for of B is correctly subtracted.
      txn_info.num_waiting_for++;
      b_txn_info->second.waited_by.emplace_back(txn_id);
    }
    result = txn_info.is_ready() ? AcquireLocksResult::ACQUIRED : AcquireLocksResult::WAITING;
  }
  if (dl_resolver_) {
    lock_guard<SpinLatch> guard(log_latch_);
    log_[log_index_].emplace_back(txn_id, txn.internal().involved_partitions_size(), is_complete, blocking_txns);
  }
  return result;
}

vector<pair<TxnId, bool>> DDRLockManager::ReleaseLocks(TxnId txn_id) {
  lock_guard<SpinLatch> guard(txn_info_latch_);

  auto txn_info_it = txn_info_.find(txn_id);
  if (txn_info_it == txn_info_.end()) {
    return {};
  }
  auto& txn_info = txn_info_it->second;
  CHECK(txn_info.is_ready()) << "Releasing unready txn " << txn_id
                             << " is forbidden. Unarrived lock requests: " << txn_info.unarrived_lock_requests
                             << ". Number of blocking txns: " << txn_info.num_waiting_for
                             << ". Deadlocked: " << txn_info.deadlocked;
  vector<pair<TxnId, bool>> result;
  for (auto blocked_txn_id : txn_info.waited_by) {
    if (blocked_txn_id == kSentinelTxnId) {
      continue;
    }
    auto it = txn_info_.find(blocked_txn_id);
    if (it == txn_info_.end()) {
      LOG(ERROR) << "Blocked txn " << TXN_ID_STR(blocked_txn_id) << " does not exist";
      continue;
    }
    auto& blocked_txn = it->second;
    blocked_txn.num_waiting_for--;
    if (blocked_txn.is_ready()) {
      // While the waited_by list might contain duplicates, the blocked
      // txn only becomes ready when its last entry in the waited_by list
      // is accounted for.
      result.emplace_back(blocked_txn_id, blocked_txn.deadlocked);
    }
  }
  txn_info_.erase(txn_id);
  return result;
}

/**
 * {
 *    lock_manager_type: 1,
 *    num_txns_waiting_for_lock: <int>,
 *    waited_by_graph (lvl >= 1): [
 *      [<txn id>, [<waited by txn id>, ...]],
 *      ...
 *    ],
 *    lock_table (lvl >= 2): [
 *      [
 *        <key>,
 *        <write lock requester>,
 *        [<read lock requester>, ...],
 *      ],
 *      ...
 *    ],
 * }
 */
void DDRLockManager::GetStats(rapidjson::Document& stats, uint32_t level) const {
  using rapidjson::StringRef;

  auto& alloc = stats.GetAllocator();

  stats.AddMember(StringRef(LOCK_MANAGER_TYPE), 1, alloc);
  stats.AddMember(StringRef(NUM_DEADLOCKS_RESOLVED), num_deadlocks_resolved_.load(), alloc);
  {
    lock_guard<SpinLatch> guard(txn_info_latch_);
    stats.AddMember(StringRef(NUM_TXNS_WAITING_FOR_LOCK), txn_info_.size(), alloc);
    if (level >= 1) {
      rapidjson::Value waited_by_graph(rapidjson::kArrayType);
      for (const auto& [txn_id, info] : txn_info_) {
        rapidjson::Value entry(rapidjson::kArrayType);
        entry.PushBack(txn_id, alloc).PushBack(ToJsonArray(info.waited_by, alloc), alloc);
        waited_by_graph.PushBack(entry, alloc);
      }
      stats.AddMember(StringRef(WAITED_BY_GRAPH), move(waited_by_graph), alloc);
    }
  }

  if (level >= 2) {
    // Collect data from lock tables
    rapidjson::Value lock_table(rapidjson::kArrayType);
    for (const auto& [key, lock_state] : lock_table_) {
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