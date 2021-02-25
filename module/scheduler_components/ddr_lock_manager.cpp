#include "module/scheduler_components/ddr_lock_manager.h"

#include <glog/logging.h>

#include <algorithm>
#include <queue>

#include "connection/zmq_utils.h"

using std::lock_guard;
using std::make_pair;
using std::make_unique;
using std::move;
using std::mutex;
using std::optional;
using std::queue;
using std::shared_ptr;
using std::unique_ptr;

namespace slog {

/**
 * Periodically, the deadlock resolver wakes up, takes a snapshot of the dependency graph,
 * broadcast the local graph to other partitions, deterministically resolve the deadlocks using the combination
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
  DeadlockResolver(DDRLockManager& lock_manager, const ConfigurationPtr& config, const shared_ptr<Broker>& broker,
                   Channel signal_chan, optional<milliseconds> poll_timeout)
      : NetworkedModule("DeadlockResolver", broker, kDeadlockResolverChannel, poll_timeout),
        lm_(lock_manager),
        config_(config),
        signal_chan_(signal_chan) {
    partitioned_graph_.resize(config->num_partitions());
  }

  void OnInternalRequestReceived(EnvelopePtr&& env) final {
    auto partition = config_->UnpackMachineId(env->from()).second;
    auto& remote_graph = partitioned_graph_[partition];

    // Acquire remote graph
    remote_graph.clear();
    auto nodes = env->request().graph().nodes();
    for (const auto& node : nodes) {
      auto ins = remote_graph.try_emplace(node.vertex(), node.vertex(), node.num_partitions(), node.deadlocked(),
                                          true /* is_stable */);
      auto& edges = ins.first->second.edges;
      edges.insert(edges.end(), node.edges().begin(), node.edges().end());
    }
  }

  void Initialize() final { ScheduleNextRun(); }

  void Run() {
    BuildLocalGraph();

    BroadcastLocalGraph();

    BuildTotalGraph();

    topo_order_.clear();
    for (auto& n : total_graph_) {
      if (!n.second.is_visited) {
        FindTopoOrderAndBuildTransposeGraph(n.second);
      }
    }
    std::reverse(topo_order_.begin(), topo_order_.end());

    CheckAndResolveDeadlocks();
  }

 private:
  DDRLockManager& lm_;
  ConfigurationPtr config_;
  Channel signal_chan_;

  unordered_map<TxnId, DDRLockManager::TxnInfo> txn_info_;

  struct Node {
    explicit Node(TxnId vertex, int num_partitions, bool deadlocked, bool is_stable)
        : vertex(vertex),
          num_partitions(num_partitions),
          deadlocked(deadlocked),
          is_stable(is_stable),
          is_visited(false) {}

    const TxnId vertex;
    const int num_partitions;

    bool deadlocked;  // Indicate if the vertex was in a deadlock but resolved previously
    bool is_stable;
    bool is_visited;
    vector<TxnId> edges;
    vector<TxnId> redges;
  };

  using Graph = unordered_map<TxnId, Node>;
  vector<Graph> partitioned_graph_;
  Graph total_graph_;
  vector<TxnId> topo_order_;
  vector<TxnId> scc_;

  void ScheduleNextRun() {
    if (config_->ddr_interval() > 0ms) {
      NewTimedCallback(config_->ddr_interval(), [this] {
        Run();
        ScheduleNextRun();
      });
    }
  }

  void BuildLocalGraph() {
    // Take a snapshot of the txn info in the lock manager
    {
      lock_guard<mutex> guard(lm_.mut_txn_info_);
      txn_info_ = lm_.txn_info_;
    }

    queue<TxnId> unstables;

    // Construct the local graph
    auto& local_graph = partitioned_graph_[config_->local_partition()];
    local_graph.clear();
    for (const auto& [vertex, info] : txn_info_) {
      auto ins = local_graph.try_emplace(vertex, vertex, info.num_partitions, info.deadlocked, info.is_stable());
      auto& edges = ins.first->second.edges;
      edges.reserve(info.waited_by.size());
      for (auto v : info.waited_by) {
        if (v != kSentinelTxnId) {
          edges.push_back(v);
        }
      }
      if (!info.is_stable()) {
        unstables.push(vertex);
      }
    }

    // Propagate unstability to reachable vertices
    while (!unstables.empty()) {
      auto it = local_graph.find(unstables.front());
      DCHECK(it != local_graph.end());
      unstables.pop();

      for (auto next : it->second.edges) {
        auto next_it = local_graph.find(next);
        DCHECK(next_it != local_graph.end()) << "Dangling edge";
        if (next_it->second.is_stable) {
          next_it->second.is_stable = false;
          unstables.push(next);
        }
      }
    }

    // Remove unstable vertices. Dangling edges may exist after this
    for (auto it = local_graph.begin(); it != local_graph.end();) {
      if (!it->second.is_stable) {
        it = local_graph.erase(it);
      } else {
        ++it;
      }
    }
  }

  void BroadcastLocalGraph() {
    auto& local_graph = partitioned_graph_[config_->local_partition()];
    if (local_graph.empty()) {
      return;
    }
    auto env = NewEnvelope();
    auto adjs = env->mutable_request()->mutable_graph()->mutable_nodes();
    adjs->Reserve(local_graph.size());
    for (const auto& [vertex, node] : local_graph) {
      auto adj = adjs->Add();
      adj->set_vertex(vertex);
      adj->set_num_partitions(node.num_partitions);
      adj->set_deadlocked(node.deadlocked);
      adj->mutable_edges()->Add(node.edges.begin(), node.edges.end());
    }
    vector<MachineId> destinations;
    for (uint32_t p = 0; p < config_->num_partitions(); p++) {
      if (p != config_->local_partition()) {
        destinations.push_back(config_->MakeMachineId(config_->local_replica(), p));
      }
    }
    Send(move(env), destinations, kDeadlockResolverChannel);

    VLOG_EVERY_N(3, 20) << "Local graph is being broadcasted";
  }

  struct MergedVertexInfo {
    MergedVertexInfo(int expected_partitions)
        : expected_partitions(expected_partitions), actual_partitions(0), deadlocked(false) {}
    int expected_partitions;
    int actual_partitions;
    bool deadlocked;
  };

  void BuildTotalGraph() {
    unordered_map<TxnId, MergedVertexInfo> vertices;
    for (auto& g : partitioned_graph_) {
      for (const auto& [vertex, node] : g) {
        auto ins = vertices.try_emplace(vertex, node.num_partitions);
        ins.first->second.actual_partitions++;
        ins.first->second.deadlocked |= node.deadlocked;
      }
    }

    queue<TxnId> unstables;

    // Build total graph from graphs across the partitions
    total_graph_.clear();
    for (const auto& [vertex, merged_vertex] : vertices) {
      bool is_stable = merged_vertex.expected_partitions == merged_vertex.actual_partitions;
      auto ins = total_graph_.try_emplace(vertex, vertex, merged_vertex.expected_partitions, merged_vertex.deadlocked,
                                          is_stable);
      auto& edges = ins.first->second.edges;
      // We allow duplicate edges, which do not affect correctnect
      for (auto& g : partitioned_graph_) {
        if (auto it = g.find(vertex); it != g.end()) {
          edges.insert(edges.end(), it->second.edges.begin(), it->second.edges.end());
        }
      }
      if (!is_stable) {
        unstables.push(vertex);
      }
    }

    // Propagate unstability to reachable vertices
    while (!unstables.empty()) {
      auto it = total_graph_.find(unstables.front());
      DCHECK(it != total_graph_.end());
      unstables.pop();

      for (auto next : it->second.edges) {
        auto next_it = total_graph_.find(next);
        // Dangling edges are a possibility so need to check for existence of `next`
        if (next_it != total_graph_.end() && next_it->second.is_stable) {
          next_it->second.is_stable = false;
          unstables.push(next);
        }
      }
    }

    // Remove unstable vertices
    for (auto it = total_graph_.begin(); it != total_graph_.end();) {
      if (!it->second.is_stable) {
        it = total_graph_.erase(it);
      } else {
        ++it;
      }
    }
  }

  void FindTopoOrderAndBuildTransposeGraph(Node& node) {
    node.is_visited = true;
    for (auto next : node.edges) {
      auto next_it = total_graph_.find(next);
      // Dangling edges are a possibility so need to check for existence of `next`
      if (next_it != total_graph_.end()) {
        auto& next_node = next_it->second;
        next_node.redges.push_back(node.vertex);
        if (!next_node.is_visited) {
          FindTopoOrderAndBuildTransposeGraph(next_node);
        }
      }
    }
    topo_order_.push_back(node.vertex);
  }

  void CheckAndResolveDeadlocks() {
    // num_waiting_for field is used to store the number of other txns that a txn is waiting
    // for. To avoid race condition (specifically, lost writes) because the deadlock resolver is
    // running in parallel to the lock manager, here, num_waiting_for field is used to record the
    // amount that num_waiting_for would increase/decrease after resolving deadlocks. At the end,
    // of the deadlock resolving process, this value will be added to the current num_waiting_for
    // in the lock manager
    for (auto& p : txn_info_) {
      p.second.num_waiting_for = 0;
    }

    for (auto& p : total_graph_) {
      p.second.is_visited = false;
    }

    int num_sccs = 0;
    // Form the strongly connected components. This time, We traverse on the tranpose graph.
    // For each component with more than 1 member, perform deterministic deadlock resolving
    for (auto vertex : topo_order_) {
      auto it = total_graph_.find(vertex);
      CHECK(it != total_graph_.end()) << "Topo order contains unknown vertex: " << vertex;
      if (!it->second.is_visited) {
        scc_.clear();
        FormStronglyConnectedComponent(it->second);
        if (scc_.size() > 1) {
          // If this component is stable and has more than 1 element, resolve the deadlock
          ResolveDeadlock();
          num_sccs++;
        }
      }
    }

    // Collect the txns that was for the first time detected to involve in a deadlock
    vector<TxnId> to_be_updated;
    for (auto& [txn_id, txn_info] : txn_info_) {
      if (!txn_info.deadlocked) {
        const auto& node_it = total_graph_.find(txn_id);
        if (node_it != total_graph_.end() && node_it->second.deadlocked) {
          txn_info.deadlocked = true;
          to_be_updated.push_back(txn_id);
        }
      }
    }

    vector<TxnId> ready_txns;
    // Update the txn info table in the lock manager with deadlock-free dependencies if needed
    if (!to_be_updated.empty()) {
      lock_guard<mutex> guard(lm_.mut_txn_info_);

      for (auto txn_id : to_be_updated) {
        auto new_txn_it = txn_info_.find(txn_id);
        if (new_txn_it == txn_info_.end()) {
          continue;
        }
        auto& new_txn = new_txn_it->second;

        auto txn_it = lm_.txn_info_.find(txn_id);
        CHECK(txn_it != lm_.txn_info_.end());
        auto& txn = txn_it->second;

        txn.deadlocked = new_txn.deadlocked;

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
      Send(move(env), signal_chan_);
    }

    if (num_sccs) {
      VLOG(3) << "Deadlock group(s) found and resolved: " << num_sccs;
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
    node.is_visited = true;
    scc_.push_back(node.vertex);
    for (auto next : node.redges) {
      if (auto it = total_graph_.find(next); it != total_graph_.end() && !it->second.is_visited) {
        FormStronglyConnectedComponent(it->second);
      }
    }
    if (scc_.size() > 1) {
      node.deadlocked = true;
    }
  }

  void ResolveDeadlock() {
    DCHECK_GE(scc_.size(), 2);

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
    int prev_local = scc_.size() - 1;
    while (prev_local >= 0 && txn_info_.find(scc_[prev_local]) == txn_info_.end()) {
      --prev_local;
    }
    if (prev_local <= 0) {
      return;
    }

    for (int i = prev_local; i >= 0; --i) {
      auto it = txn_info_.find(scc_[i]);
      if (it == txn_info_.end()) {
        continue;
      }
      auto& info = it->second;
      CHECK(info.is_stable()) << "scc contains unstable txn: " << scc_[i];

      // Remove old edges
      for (size_t j = 0; j < info.waited_by.size(); j++) {
        // Only remove edges connecting the current node to another node in the same SCC
        if (std::binary_search(scc_.begin(), scc_.end(), info.waited_by[j])) {
          auto waiting_txn = txn_info_.find(info.waited_by[j]);
          CHECK(waiting_txn != txn_info_.end());

          // Setting to kSentinelTxnId effectively removes this edge
          info.waited_by[j] = kSentinelTxnId;

          // Decrement the incoming edge counter
          --waiting_txn->second.num_waiting_for;
        }
      }

      if (i != prev_local) {
        // Add the new edge from scc_[i] to scc_[prev_local]
        auto new_edge_added = false;
        for (size_t j = 0; j < info.waited_by.size(); j++) {
          // Add to the first empty slot
          if (info.waited_by[j] == kSentinelTxnId) {
            // Add new edge to the edge list of scc_[i]
            info.waited_by[j] = scc_[prev_local];
            // Update the counter of scc_[prev_local]
            ++(txn_info_.find(scc_[prev_local])->second.num_waiting_for);
            new_edge_added = true;
            break;
          }
        }
        // An empty slot is always added when the txn info is initialized so
        // this should always work
        CHECK(new_edge_added) << "Cannot find slot to add new edge";
      }

      prev_local = i;
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

void DDRLockManager::InitializeDeadlockResolver(const ConfigurationPtr& config, const shared_ptr<Broker>& broker,
                                                Channel signal_chan, optional<milliseconds> poll_timeout) {
  dl_resolver_ = MakeRunnerFor<DeadlockResolver>(*this, config, broker, signal_chan, poll_timeout);
}

void DDRLockManager::StartDeadlockResolver() {
  if (dl_resolver_) {
    dl_resolver_->StartInNewThread();
  }
}

// For testing only
bool DDRLockManager::ResolveDeadlock(bool recv_remote_message, bool resolve_deadlock) {
  if (dl_resolver_ && !dl_resolver_->is_running()) {
    if (recv_remote_message) {
      dl_resolver_->StartOnce();
    }
    if (resolve_deadlock) {
      std::dynamic_pointer_cast<DeadlockResolver>(dl_resolver_->module())->Run();
    }
    return true;
  }
  return false;
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
  for (const auto& [key, value] : txn.keys()) {
    if (!is_remaster && static_cast<int>(value.metadata().master()) != home) {
      continue;
    }
    ++num_relevant_locks;

    auto key_replica = MakeKeyReplica(key, home);
    auto& lock_queue_tail = lock_table_[key_replica];

    switch (value.type()) {
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
    auto ins = txn_info_.try_emplace(txn_id, txn_id, txn.internal().involved_partitions_size(),
                                     is_remaster ? 2 : txn.keys_size());
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

vector<pair<TxnId, bool>> DDRLockManager::ReleaseLocks(TxnId txn_id) {
  lock_guard<mutex> guard(mut_txn_info_);

  auto txn_info_it = txn_info_.find(txn_id);
  if (txn_info_it == txn_info_.end()) {
    return {};
  }
  auto& txn_info = txn_info_it->second;
  if (!txn_info.is_ready()) {
    LOG(FATAL) << "Releasing unready txn is forbidden";
  }
  vector<pair<TxnId, bool>> result;
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
      result.emplace_back(blocked_txn_id, blocked_txn.deadlocked);
    }
  }
  txn_info_.erase(txn_id);
  return result;
}

/**
 * {
 *    lock_manager_type: 1,
 *    num_txns_waiting_for_lock: <number of txns waiting for lock>,
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
    lock_guard<mutex> guard(mut_txn_info_);
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