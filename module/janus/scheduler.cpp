#include "module/janus/scheduler.h"

#include <algorithm>
#include <iostream>
#include <sstream>
#include <unordered_map>
#include <unordered_set>

#include "common/json_utils.h"
#include "common/metrics.h"
#include "common/proto_utils.h"
#include "common/types.h"
#include "proto/internal.pb.h"

namespace janus {

using std::make_shared;
using std::move;
using std::shared_ptr;
using std::unordered_set;
using std::vector;
using std::chrono::milliseconds;

using slog::kMachineIdBits;
using slog::kPartitionIdBits;
using slog::kRegionIdBits;
using slog::kReplicaIdBits;
using slog::kSchedulerChannel;
using slog::MakeMachineId;
using slog::MakeRunnerFor;
using slog::per_thread_metrics_repo;
using slog::internal::Request;
using slog::internal::Response;

bool PendingIndex::Add(const JanusDependency& ancestor, TxnId descendant) {
  auto res = index_.try_emplace(ancestor.txn_id());
  res.first->second.insert(descendant);
  return res.second;
}

std::optional<std::unordered_set<TxnId>> PendingIndex::Remove(TxnId ancestor) {
  auto it = index_.find(ancestor);
  if (it == index_.end()) {
    return std::nullopt;
  }
  std::unordered_set<TxnId> descendants(move(it->second));
  index_.erase(it);
  return descendants;
}

std::string PendingIndex::to_string() const {
  std::ostringstream oss;
  for (auto& [txn_id, desc] : index_) {
    oss << txn_id << ": ";
    bool first = true;
    for (auto d : desc) {
      if (!first) oss << ", ";
      oss << d;
      first = false;
    }
    oss << "\n";
  }
  return oss.str();
}

Scheduler::Scheduler(const shared_ptr<Broker>& broker, const shared_ptr<Storage>& storage,
                     const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, {kSchedulerChannel, false /* is_raw */}, metrics_manager, poll_timeout),
      sccs_finder_(graph_, execution_horizon_),
      current_worker_(0) {
  for (int i = 0; i < config()->num_workers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(i, broker, storage, metrics_manager, poll_timeout));
  }
}

void Scheduler::Initialize() {
  auto cpus = config()->cpu_pinnings(slog::ModuleId::WORKER);
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
    case Request::kJanusCommit:
      ProcessTransaction(move(env));
      break;
    case Request::kJanusInquire:
      ProcessInquiry(move(env));
      break;
    case Request::kStats:
      PrintStats(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
      break;
  }
}

void Scheduler::ProcessTransaction(EnvelopePtr&& env) {
  auto commit = env->mutable_request()->mutable_janus_commit();
  auto txn = commit->release_txn();
  auto txn_id = txn->internal().id();

  txns_.emplace(txn_id, txn);

  vector<JanusDependency> deps(commit->deps().begin(), commit->deps().end());
  auto [vertex_it, inserted] = graph_.insert({txn_id, Vertex{txn_id, true, move(deps)}});
  CHECK(inserted);

  VLOG(2) << "New transaction: " << txn_id;

  CheckPendingInquiry(txn_id);

  vector<TxnId> ready_txns;
  FindAndResolveSCCs(vertex_it->second, ready_txns);
  CheckPendingTxns(move(ready_txns));
}

bool Scheduler::ProcessInquiry(EnvelopePtr&& env) {
  auto inquired_txn_ids = env->request().janus_inquire().txn_id();

  auto resp_env = NewEnvelope();
  auto resp_inquiry = resp_env->mutable_response()->mutable_janus_inquire();
  bool all_processed = true;

  for (auto txn_id : inquired_txn_ids) {
    if (auto vertex_it = graph_.find(txn_id); vertex_it == graph_.end()) {
      if (execution_horizon_.contains(txn_id)) {
        auto result = resp_inquiry->add_results();
        result->set_txn_id(txn_id);
        result->set_executed(true);
      } else {
        auto pending_env = NewEnvelope();
        pending_env->mutable_request()->mutable_janus_inquire()->add_txn_id(txn_id);
        pending_env->set_from(env->from());
        pending_inquiries_[txn_id].push_back(move(pending_env));
        all_processed = false;
      }
    } else {
      auto result = resp_inquiry->add_results();
      result->set_txn_id(txn_id);
      result->set_executed(false);
      for (auto& dep : vertex_it->second.deps) {
        result->add_deps()->CopyFrom(dep);
      }
    }
  }

  if (!resp_inquiry->results().empty()) {
    Send(*resp_env, env->from(), kSchedulerChannel);
  }

  return all_processed;
}

void Scheduler::OnInternalResponseReceived(EnvelopePtr&& env) {
  if (env->response().type_case() != Response::kJanusInquire) {
    LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), Response) << "\"";
  }
  auto& inquiry_result = env->response().janus_inquire();

  VLOG(2) << "Inquiry result: " << inquiry_result.DebugString();

  for (const auto& res : inquiry_result.results()) {
    auto txn_id = res.txn_id();
    if (res.executed()) {
      execution_horizon_.Add(txn_id);
      CheckPendingTxns({txn_id});
    } else {
      vector<JanusDependency> deps(res.deps().begin(), res.deps().end());
      auto res = graph_.insert({txn_id, Vertex{txn_id, false, move(deps)}});
      vector<TxnId> ready_txns;
      FindAndResolveSCCs(res.first->second, ready_txns);
      CheckPendingTxns(move(ready_txns));
    }
  }
}

void Scheduler::DispatchSCCs(const vector<SCC>& sccs) {
  for (const SCC& scc : sccs) {
    VLOG(2) << "Dispatched SCC: " << scc;
    for (auto [txn_id, is_local] : scc) {
      if (!is_local) {
        graph_.erase(txn_id);
        execution_horizon_.Add(txn_id);
        continue;
      }
      auto txn_it = txns_.find(txn_id);
      CHECK(txn_it != txns_.end()) << "Could not find transaction " << txn_id;

      zmq::message_t msg(sizeof(Transaction*));
      *msg.data<Transaction*>() = txn_it->second;
      txns_.erase(txn_it);

      int worker = current_worker_;
      current_worker_ = (current_worker_ + 1) % workers_.size();
      GetCustomSocket(worker).send(msg, zmq::send_flags::none);
    }

    if (per_thread_metrics_repo != nullptr && scc.size() > 1) {
      per_thread_metrics_repo->RecordDeadlockResolverDeadlock(scc.size(), {}, {});
    }
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
        execution_horizon_.Add(txn_id);
        graph_.erase(txn_id);
      }
    }
  };

  return has_msg;
}

void Scheduler::CheckPendingInquiry(TxnId txn_id) {
  if (auto it = pending_inquiries_.find(txn_id); it != pending_inquiries_.end()) {
    for (auto& env : it->second) {
      CHECK(ProcessInquiry(move(env)));
    }
    pending_inquiries_.erase(it);
  }
}

void Scheduler::CheckPendingTxns(vector<TxnId>&& ready_txns) {
  while (!ready_txns.empty()) {
    auto txn_id = ready_txns.back();
    ready_txns.pop_back();

    auto pending = pending_txns_.Remove(txn_id);
    if (!pending.has_value()) {
      continue;
    }

    VLOG(2) << "Checking pending txns: " << txn_id << " <- " << pending.value();

    unordered_set<TxnId> unready;
    for (auto pending_txn_id : pending.value()) {
      auto pending_txn_it = graph_.find(pending_txn_id);
      if (pending_txn_it == graph_.end() || pending_txn_it->second.disc != 0) {
        continue;
      }

      pending_txn_it->second.missing_deps--;

      if (unready.find(pending_txn_id) != unready.end() || pending_txn_it->second.missing_deps > 0) {
        continue;
      }

      auto new_unready = FindAndResolveSCCs(pending_txn_it->second, ready_txns);
      unready.insert(new_unready.begin(), new_unready.end());
    }
  }
}

std::unordered_set<TxnId> Scheduler::FindAndResolveSCCs(Vertex& v, vector<TxnId>& ready_txns) {
  sccs_finder_.FindSCCs(v);
  auto result = sccs_finder_.Finalize();
  VLOG(3) << "FindSCCs for " << v.txn_id << " - unready vertices: " << result.unready;

  v.missing_deps = result.missing_deps.size();

  // Create inquiry requests for the missing dependencies
  auto local_region = config()->local_region();
  auto local_replica = config()->local_replica();
  auto local_partition = config()->local_partition();
  vector<EnvelopePtr> envs(config()->num_partitions());
  for (const auto& dep : result.missing_deps) {
    VLOG(3) << "Pending: " << dep.txn_id() << " <- " << v.txn_id;
    if (pending_txns_.Add(dep, v.txn_id)) {
      // Send an inquiry if current partition is not a participant of ancestor
      if ((dep.participants_bitmap() & (1 << local_partition)) == 0) {
        auto part = dep.target_partition();
        if (envs[part] == nullptr) envs[part] = NewEnvelope();
        envs[part]->mutable_request()->mutable_janus_inquire()->add_txn_id(dep.txn_id());
      }
    }
  }
  for (size_t i = 0; i < envs.size(); i++) {
    if (envs[i] != nullptr) {
      VLOG(3) << "Inquire: " << envs[i]->DebugString();
      Send(*envs[i], MakeMachineId(local_region, local_replica, i), kSchedulerChannel);
    }
  }

  DispatchSCCs(result.sccs);

  for (auto& scc : result.sccs) {
    for (auto [new_txn_id, _] : scc) {
      ready_txns.push_back(new_txn_id);
    }
  }

  if (per_thread_metrics_repo != nullptr) {
    per_thread_metrics_repo->RecordDeadlockResolverRun(v.txn_id, result.unready.size(), ready_txns.size(),
                                                       result.sccs.size(), result.missing_deps.size());
  }

  return result.unready;
}

using std::cout;

void Scheduler::PrintStats(EnvelopePtr&& env) {
  auto& stat_request = env->request().stats();

  cout << "\n";
  switch (stat_request.level()) {
    case 0:
      cout << "Graph:\n";
      for (auto& [txn_id, vertex] : graph_) {
        cout << txn_id << ": ";
        bool first = true;
        for (auto& dep : vertex.deps) {
          if (!first) cout << ", ";
          cout << dep.txn_id();
          first = false;
        }
        cout << "\n";
      }
      break;
    case 1:
      cout << "Pending txns:\n" << pending_txns_.to_string();
      break;
    case 2:
      cout << "Undispatched txns:\n";
      for (auto& [txn_id, _] : txns_) {
        cout << txn_id << "\n";
      }
      break;
    default: {
      auto txn_id = stat_request.level();
      cout << "Txn id: " << txn_id << "\n";
      cout << "In execution horizon: " << execution_horizon_.contains(txn_id) << "\n";
      auto txns_it = txns_.find(txn_id);
      if (txns_it == txns_.end()) {
        cout << "Txn data does not exist\n";
      } else {
        cout << "Data:\n" << txns_it->second->DebugString();
      }
      auto graph_it = graph_.find(txn_id);
      if (graph_it == graph_.end()) {
        cout << "No longer inside graph";
      } else {
        sccs_finder_.FindSCCs(graph_it->second);
        auto result = sccs_finder_.Finalize();
        cout << "Found SCCs:\n";
        for (const auto& scc : result.sccs) {
          cout << scc << "\n";
        }
        cout << "Missing dependencies:\n";
        for (const auto& dep : result.missing_deps) {
          cout << dep.DebugString() << "\n";
        }
        cout << "Unready: " << result.unready;
      }
    }
  }
  cout << std::flush;
}

}  // namespace janus