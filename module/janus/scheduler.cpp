#include "module/janus/scheduler.h"

#include <algorithm>
#include <unordered_set>

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

JanusScheduler::JanusScheduler(const shared_ptr<Broker>& broker, const shared_ptr<Storage>& storage,
                               const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(broker, {kSchedulerChannel, false /* is_raw */}, metrics_manager, poll_timeout),
      current_worker_(0) {
  for (int i = 0; i < config()->num_workers(); i++) {
    workers_.push_back(MakeRunnerFor<Worker>(i, broker, storage, metrics_manager, poll_timeout));
  }
}

void JanusScheduler::Initialize() {
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

void JanusScheduler::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kJanusCommit:
      ProcessTransaction(move(env));
      break;
    case Request::kJanusInquire:
      ProcessInquiry(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
      break;
  }
}

void JanusScheduler::OnInternalResponseReceived(EnvelopePtr&& env) {
  switch (env->response().type_case()) {
    case Response::kJanusInquire: {

      break;
    }
    default:
      LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), Response) << "\"";
      break;
  }
}

// Handle responses from the workers
bool JanusScheduler::OnCustomSocket() {
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
        txns_.erase(txn_id);
      }
    }
  };

  return has_msg;
}

void JanusScheduler::ProcessTransaction(EnvelopePtr&& env) {
  auto commit = env->mutable_request()->mutable_janus_commit();
  auto txn = commit->release_txn();
  auto txn_id = txn->internal().id();
  
  txns_.emplace(txn_id, txn);

  auto [vertex_it, inserted] = graph_.emplace(txn_id, txn_id);
  CHECK(inserted);

  std::copy(commit->dep().begin(), commit->dep().end(),
      std::back_inserter(vertex_it->second.dep));
  
  auto result = sccs_finder_.FindSCCs(graph_, vertex_it->second, execution_horizon_);
  switch (result) {
    case TarjanResult::FOUND:
      DispatchSCCs();
      break;
    case TarjanResult::MISSING_DEPENDENCIES_AND_MAYBE_FOUND:
      InquireMissingDependencies();
      DispatchSCCs();
      break;
    case TarjanResult::NOT_FOUND:
      break;
  }
}

void JanusScheduler::DispatchSCCs() {
  auto sccs = sccs_finder_.TakeSCCs();
  for (SCC& scc : sccs) {
    for (auto txn_id : scc) {
      auto txn_it = txns_.find(txn_id);
      CHECK(txn_it != txns_.end());

      zmq::message_t msg(sizeof(Transaction*));
      *msg.data<Transaction*>() = txn_it->second;

      int worker = current_worker_;
      current_worker_ = (current_worker_ + 1) % workers_.size();
      GetCustomSocket(worker).send(msg, zmq::send_flags::none);

      VLOG(3) << "Dispatched txn " << TXN_ID_STR(txn_id);
    }
  }
}

void JanusScheduler::InquireMissingDependencies() {
  auto missing_deps = sccs_finder_.TakeMissingVertices();
  
}

void JanusScheduler::ProcessInquiry(EnvelopePtr&& env) {

}


}  // namespace slog