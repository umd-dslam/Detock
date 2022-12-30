#include "module/janus/acceptor.h"

#include <glog/logging.h>
#include <unordered_set>

#include "common/clock.h"
#include "common/constants.h"
#include "common/json_utils.h"
#include "common/proto_utils.h"

namespace slog {

using internal::Envelope;
using internal::Request;
using internal::Response;

/*
 * This module does not have anything to do with Sequencer. It just uses the Sequencer's stuff for convenience.
 */
JanusAcceptor::JanusAcceptor(const std::shared_ptr<zmq::context_t>& context,
                             const ConfigurationPtr& config,
                             const MetricsRepositoryManagerPtr& metrics_manager,
                             std::chrono::milliseconds poll_timeout)
    : NetworkedModule(context, config, config->sequencer_port(), kSequencerChannel, metrics_manager, poll_timeout,
                      true /* is_long_sender */),
      sharder_(Sharder::MakeSharder(config)) {
}

void JanusAcceptor::OnInternalRequestReceived(EnvelopePtr&& env) {
  switch (env->request().type_case()) {
    case Request::kJanusPreAccept:
      ProcessPreAccept(move(env));
      break;
    default:
      LOG(ERROR) << "Unexpected request type received: \"" << CASE_NAME(env->request().type_case(), Request) << "\"";
  }
}

void JanusAcceptor::ProcessPreAccept(EnvelopePtr&& env) {
  auto local_partition = config()->local_partition();
  auto txn = env->mutable_request()->mutable_janus_pre_accept()->release_txn();
  auto txn_id = txn->internal().id();

  auto resp_env = NewEnvelope();
  auto pre_accept = resp_env->mutable_response()->mutable_janus_pre_accept();
  pre_accept->set_txn_id(txn_id);

  std::unordered_set<TxnId> dep;
  for (auto it = txn->mutable_keys()->begin(); it != txn->mutable_keys()->end(); it++) {
    if (sharder_->compute_partition(it->key()) == local_partition) {
      auto key_it = latest_writing_txns_.find(it->key());
      if (key_it != latest_writing_txns_.end()) {
        dep.insert(key_it->second);
      }
      if (it->value_entry().type() == KeyType::WRITE) {
        latest_writing_txns_[it->key()] = txn_id;
      }
    }
  }

  for (auto it = dep.begin(); it != dep.end(); it++) {
    pre_accept->add_dep(*it);
  }

  Send(*resp_env, env->from(), kForwarderChannel);
}


}  // namespace slog