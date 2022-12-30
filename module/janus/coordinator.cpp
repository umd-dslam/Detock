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

QuorumDeps::QuorumDeps(const Dependency& deps, int num_replicas)
    : deps_(deps), is_fast_quorum_(true), count_(1), num_replicas_(num_replicas) {
}

void QuorumDeps::Add(const Dependency& deps) {
  if (deps != deps_) {
    is_fast_quorum_ = false;
  }
  count_++;
}

bool QuorumDeps::is_done() {
  if (is_fast_quorum_)
    return count_ == num_replicas_;
  
  return count_ >= (num_replicas_ + 1) / 2;
}

bool QuorumDeps::is_fast_quorum() {
  return is_fast_quorum_;
}

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
      break;
    default:
      LOG(ERROR) << "Unexpected response type received: \"" << CASE_NAME(env->response().type_case(), Response) << "\"";
  }
}

void JanusCoordinator::StartNewTxn(EnvelopePtr&& env) {
  auto local_region = config()->local_region();
  auto local_partition = config()->local_partition();
  auto txn = env->mutable_request()->mutable_forward_txn()->release_txn();

  RECORD(txn->mutable_internal(), TransactionEvent::ENTER_FORWARDER);

  try {
    PopulateInvolvedPartitions(sharder_, *txn);
  } catch (std::invalid_argument& e) {
    LOG(ERROR) << "Only numeric keys are allowed while running in Simple Partitioning mode";
    return;
  }

  txns_.insert({txn->internal().id(), TransactionState()});

  auto req_env = NewEnvelope();
  auto pre_accept = req_env->mutable_request()->mutable_janus_pre_accept();
  pre_accept->set_allocated_txn(txn);

  auto num_involved_partitions = txn->internal().involved_partitions_size();
  std::vector<MachineId> destinations;

  for (int reg = 0; reg < config()->num_regions(); reg++) {
    for (int i = 0; i < num_involved_partitions; ++i) {
      auto p = txn->internal().involved_partitions(i);
      if (reg == local_region && p == local_partition) {
        continue;
      }
      destinations.push_back(MakeMachineId(reg, 0, p));
    }
  }

  Send(*req_env, destinations, kSequencerChannel);
}


}  // namespace slog