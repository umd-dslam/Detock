#include "module/janus/coordinator.h"

#include <glog/logging.h>
#include "common/clock.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"

namespace slog {

using internal::Envelope;
using internal::Request;
using internal::Response;

class Quorum {
 public:
  Quorum(int num_replicas) : num_replicas_(num_replicas), count_(0) {}

  void Inc() { count_++; }

  bool is_done() {  return count_ >= (num_replicas_ + 1) / 2; }

 private:
  const int num_replicas_;
  int count_;
};

class QuorumDeps {
 public:
  QuorumDeps(int num_replicas)
    : num_replicas_(num_replicas), is_fast_quorum_(true), count_(0) {
  }
  
  void Add(const Dependencies& deps) {
    if (is_done()) {
      return;
    }

    if (!is_fast_quorum_ || (count_ > 0 && deps != this->deps)) {
      is_fast_quorum_ = false;
      this->deps.insert(deps.begin(), deps.end());
    }

    count_++;
  }

  bool is_done() {
    if (is_fast_quorum_)
      return count_ == num_replicas_;
  
    return count_ >= (num_replicas_ + 1) / 2;
  }

  bool is_fast_quorum() {
    return is_fast_quorum_;
  }

  Dependencies deps;

 private:
  const int num_replicas_;
  bool is_fast_quorum_;
  int count_;
};

/*
 * This module does not have anything to do with Forwarder. It just uses the Forwarder's stuff for convenience.
 */
JanusCoordinator::JanusCoordinator(const std::shared_ptr<zmq::context_t>& context,
                                   const ConfigurationPtr& config,
                                   const MetricsRepositoryManagerPtr& metrics_manager,
                                   std::chrono::milliseconds poll_timeout)
    : NetworkedModule(context, config, config->forwarder_port(), kForwarderChannel, metrics_manager, poll_timeout,
                      true /* is_long_sender */),
      sharder_(Sharder::MakeSharder(config)) {
}

void JanusCoordinator::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kForwardTxn:
      StartNewTxn(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
  }
}

void JanusCoordinator::OnInternalResponseReceived(EnvelopePtr&& env) {
  switch (env->response().type_case()) {
    case Response::kJanusPreAccept:
      PreAcceptTxn(move(env));
      break;
    case Response::kJanusAccept:
      AcceptTxn(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), Response) << "\"";
  }
}

void JanusCoordinator::StartNewTxn(EnvelopePtr&& env) {
  auto txn = env->mutable_request()->mutable_forward_txn()->release_txn();
  auto txn_id = txn->internal().id();

  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_FORWARDER);

  // Figure out participating partitions
  try {
    PopulateInvolvedPartitions(sharder_, *txn);
  } catch (std::invalid_argument& e) {
    LOG(ERROR) << "Only numeric keys are allowed while running in Simple Partitioning mode";
    return;
  }

  // Remember the txn state
  auto [info_it, inserted] = txns_.insert(
    {txn_id, CoordinatorTxnInfo(txn_id, config()->num_partitions())});
  
  CHECK(inserted);

  auto pre_accept_env = NewEnvelope();
  auto pre_accept = pre_accept_env->mutable_request()->mutable_janus_pre_accept();
  pre_accept->set_allocated_txn(txn);
  pre_accept->set_ballot(0);

  auto num_involved_partitions = txn->internal().involved_partitions_size();

  // Collect participants
  for (int i = 0; i < num_involved_partitions; ++i) {
    auto p = txn->internal().involved_partitions(i);
    for (int reg = 0; reg < config()->num_regions(); reg++) {
      info_it->second.destinations.push_back(MakeMachineId(reg, 0, p));
    }
    info_it->second.participants.push_back(p);
  }

  Send(*pre_accept_env, info_it->second.destinations, kSequencerChannel);
}

void JanusCoordinator::PreAcceptTxn(EnvelopePtr&& env) {
  auto& pre_accept = env->response().janus_pre_accept();
  auto txn_id = pre_accept.txn_id();

  // Crash recovery is not implemented
  CHECK(pre_accept.ok());

  auto info_it = txns_.find(txn_id);
  CHECK(info_it != txns_.end());
  CHECK(info_it->second.phase == Phase::PRE_ACCEPT);

  // Get the quorum deps object of the partition in the response
  auto& sharded_deps = info_it->second.sharded_deps;
  auto from_partition = std::get<2>(UnpackMachineId(env->from()));
  CHECK_LT(from_partition, sharded_deps.size());
  auto& quorum_deps = sharded_deps[from_partition];
  if (!quorum_deps.has_value()) {
    quorum_deps.emplace(config()->num_regions());
  }

  // Add the dependency to the quorum
  Dependencies deps;
  for (auto& dep : pre_accept.deps()) {
    deps.emplace(dep.txn_id(), dep.participants_bitmap());
  }
  quorum_deps.value().Add(deps);

  // See if we get enough responses for each participant
  bool done = true;
  for (int p : info_it->second.participants) {
    CHECK_LT(p, sharded_deps.size());
    if (!sharded_deps[p].has_value() || !sharded_deps[p].value().is_done()) {
      done = false;
      break;
    }
  }

  // Nothing else to do if we're not done
  if (!done) {
    return;
  }

  // Fast path
  if (quorum_deps.value().is_fast_quorum()) {
    CommitTxn(info_it->second);
  }
  // Slow path 
  else {
    // Go to accept phase
    auto accept_env = NewEnvelope();
    auto accept = accept_env->mutable_request()->mutable_janus_accept();
    accept->set_txn_id(txn_id);
    accept->set_ballot(0);
    for (auto p = 0U; p < sharded_deps.size(); p++) {
      if (!sharded_deps[p].has_value()) 
        continue;
      for (auto& dep : sharded_deps[p].value().deps) {
        auto janus_dep = accept->add_deps();
        janus_dep->set_txn_id(dep.first);
        janus_dep->set_participants_bitmap(dep.second);
        janus_dep->set_target_partition(p);
      }
    }
    Send(*accept_env, info_it->second.destinations, kSequencerChannel);

    info_it->second.phase = Phase::ACCEPT;
  }
}

void JanusCoordinator::AcceptTxn(EnvelopePtr&& env) {
  auto& accept = env->response().janus_accept();
  auto txn_id = accept.txn_id();

  // Crash recovery is not implemented
  CHECK(accept.ok());

  auto info_it = txns_.find(txn_id);
  CHECK(info_it != txns_.end());
  CHECK(info_it->second.phase == Phase::ACCEPT);

  // Get the quorum deps object of the partition in the response
  auto& quorums = info_it->second.quorums;
  auto from_partition = std::get<2>(UnpackMachineId(env->from()));
  CHECK_LT(from_partition, quorums.size());
  auto& quorum = quorums[from_partition];
  if (!quorum.has_value()) {
    quorum.emplace(config()->num_regions());
  }

  // Increase the number of responses in the quorum
  quorum.value().Inc();

  // See if we get enough responses for each participant
  bool done = true;
  for (int p : info_it->second.participants) {
    CHECK_LT(p, quorums.size());
    if (!quorums[p].has_value() || !quorums[p].value().is_done()) {
      done = false;
      break;
    }
  }

  // Nothing else to do if we're not done
  if (!done) {
    return;
  }

  CommitTxn(info_it->second);
}

void JanusCoordinator::CommitTxn(CoordinatorTxnInfo& txn_info) {
  auto& sharded_deps = txn_info.sharded_deps;
  auto commit_env = NewEnvelope();
  auto commit = commit_env->mutable_request()->mutable_janus_commit();
  commit->set_txn_id(txn_info.txn_id);
  for (auto p = 0U; p < sharded_deps.size(); p++) {
    if (!sharded_deps[p].has_value()) 
      continue;
    for (auto& dep : sharded_deps[p].value().deps) {
      auto janus_dep = commit->add_deps();
      janus_dep->set_txn_id(dep.first);
      janus_dep->set_participants_bitmap(dep.second);
      janus_dep->set_target_partition(p);
    }
  }
  Send(*commit_env, txn_info.destinations, kSequencerChannel);

  // No need to keep this around anymore
  txns_.erase(txn_info.txn_id);
}

}  // namespace slog