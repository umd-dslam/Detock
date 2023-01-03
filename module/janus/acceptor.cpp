#include "module/janus/acceptor.h"

#include <glog/logging.h>

#include <unordered_map>

#include "common/clock.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"

namespace janus {

using slog::kForwarderChannel;
using slog::kSchedulerChannel;
using slog::internal::Envelope;
using slog::internal::Request;
using slog::internal::Response;

/*
 * This module does not have anything to do with Sequencer. It just uses the Sequencer's stuff for convenience.
 */
Acceptor::Acceptor(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                   const MetricsRepositoryManagerPtr& metrics_manager, std::chrono::milliseconds poll_timeout)
    : NetworkedModule(context, config, config->sequencer_port(), slog::kSequencerChannel, metrics_manager, poll_timeout,
                      true /* is_long_sender */),
      sharder_(slog::Sharder::MakeSharder(config)) {}

void Acceptor::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kJanusPreAccept:
      ProcessPreAccept(move(env));
      break;
    case Request::kJanusAccept:
      ProcessAccept(move(env));
      break;
    case Request::kJanusCommit:
      ProcessCommit(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
  }
}

void Acceptor::ProcessPreAccept(EnvelopePtr&& env) {
  auto local_partition = config()->local_partition();
  auto num_partitions = config()->num_partitions();
  auto txn = env->mutable_request()->mutable_janus_pre_accept()->release_txn();
  auto txn_id = txn->internal().id();
  auto ballot = env->request().janus_pre_accept().ballot();

  auto txn_info_it = txns_.find(txn_id);
  if (txn_info_it != txns_.end()) {
    // If highest ballot is higher, return with not ok
    if (txn_info_it->second.highest_ballot > ballot) {
      auto pre_accept_env = NewEnvelope();
      auto pre_accept = pre_accept_env->mutable_response()->mutable_janus_pre_accept();
      pre_accept->set_txn_id(txn_id);
      pre_accept->set_ok(false);
      Send(*pre_accept_env, env->from(), kForwarderChannel);
      return;
    }
  } else {
    txn_info_it = txns_.insert({txn_id, AcceptorTxnInfo(txn)}).first;
  }

  // Update the ballot
  txn_info_it->second.highest_ballot = ballot;

  // Extract participants of the transaction
  uint64_t participants_bitmap = 0;
  for (auto p : txn->internal().involved_partitions()) {
    CHECK_LT(p, num_partitions);
    participants_bitmap |= 1 << p;
  }

  // Determine dependencies
  std::unordered_map<TxnId, uint64_t> deps;
  for (auto it = txn->mutable_keys()->begin(); it != txn->mutable_keys()->end(); it++) {
    if (sharder_->compute_partition(it->key()) == local_partition) {
      auto key_it = latest_writing_txns_.find(it->key());
      if (key_it != latest_writing_txns_.end()) {
        deps.insert(key_it->second);
      }
      if (it->value_entry().type() == slog::KeyType::WRITE) {
        latest_writing_txns_[it->key()] = {txn_id, participants_bitmap};
      }
    }
  }

  // Construct pre-accept response
  auto pre_accept_env = NewEnvelope();
  auto pre_accept = pre_accept_env->mutable_response()->mutable_janus_pre_accept();
  pre_accept->set_txn_id(txn_id);
  pre_accept->set_ok(true);
  for (auto it = deps.begin(); it != deps.end(); it++) {
    auto janus_dep = pre_accept->add_deps();
    janus_dep->set_txn_id(it->first);
    janus_dep->set_participants_bitmap(it->second);
    janus_dep->set_target_partition(local_partition);
  }

  Send(*pre_accept_env, env->from(), kForwarderChannel);
}

void Acceptor::ProcessAccept(EnvelopePtr&& env) {
  auto txn_id = env->request().janus_accept().txn_id();
  auto ballot = env->request().janus_accept().ballot();

  auto txn_info_it = txns_.find(txn_id);
  if (txn_info_it == txns_.end() || txn_info_it->second.highest_ballot > ballot) {
    auto accept_env = NewEnvelope();
    auto accept = accept_env->mutable_response()->mutable_janus_accept();
    accept->set_txn_id(txn_id);
    accept->set_ok(false);
    Send(*accept_env, env->from(), kForwarderChannel);
    return;
  }

  auto accept_env = NewEnvelope();
  auto accept = accept_env->mutable_response()->mutable_janus_accept();
  accept->set_txn_id(txn_id);
  accept->set_ok(true);

  txn_info_it->second.phase = Phase::ACCEPT;

  Send(*accept_env, env->from(), kForwarderChannel);
}

void Acceptor::ProcessCommit(EnvelopePtr&& env) {
  auto txn_id = env->request().janus_commit().txn_id();

  auto txn_info_it = txns_.find(txn_id);
  if (txn_info_it == txns_.end()) {
    LOG(ERROR) << "Cannot find transaction " << txn_id << " to commit";
    return;
  }

  env->mutable_request()->mutable_janus_commit()->set_allocated_txn(txn_info_it->second.txn);

  Send(move(env), kSchedulerChannel);

  txns_.erase(txn_id);
}

}  // namespace janus