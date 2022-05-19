#include "module/base/networked_module.h"

#include <glog/logging.h>

#include <sstream>

#include "common/constants.h"
#include "connection/broker.h"
#include "connection/sender.h"

using std::make_unique;
using std::move;
using std::optional;
using std::unique_ptr;
using std::vector;

namespace slog {

using internal::Envelope;

NetworkedModule::NetworkedModule(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                                 Channel channel, const MetricsRepositoryManagerPtr& metrics_manager,
                                 std::optional<std::chrono::milliseconds> poll_timeout, bool is_long_sender)
    : context_(context),
      config_(config),
      channel_(channel),
      port_(std::nullopt),
      metrics_manager_(metrics_manager),
      sender_(config, context, is_long_sender),
      poller_(poll_timeout),
      recv_retries_(0) {
  std::ostringstream os;
  os << "machine_id = " << MACHINE_ID_STR(config->local_machine_id());
  debug_info_ = os.str();
}

NetworkedModule::NetworkedModule(const std::shared_ptr<Broker>& broker, Broker::ChannelOption chopt,
                                 const MetricsRepositoryManagerPtr& metrics_manager,
                                 optional<std::chrono::milliseconds> poll_timeout, bool is_long_sender)
    : NetworkedModule(broker->context(), broker->config(), chopt.channel, metrics_manager, poll_timeout,
                      is_long_sender) {
  broker->AddChannel(chopt);
}

NetworkedModule::NetworkedModule(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config,
                                 uint32_t port, Channel channel, const MetricsRepositoryManagerPtr& metrics_manager,
                                 std::optional<std::chrono::milliseconds> poll_timeout, bool is_long_sender)
    : NetworkedModule(context, config, channel, metrics_manager, poll_timeout, is_long_sender) {
  port_ = port;
}

void NetworkedModule::AddCustomSocket(zmq::socket_t&& new_socket) {
  auto& sock = custom_sockets_.emplace_back(move(new_socket));
  poller_.PushSocket(sock);
}

zmq::socket_t& NetworkedModule::GetCustomSocket(size_t i) { return custom_sockets_.at(i); }

void NetworkedModule::SetUp() {
  VLOG(1) << "Thread info (" << name() << "): " << debug_info_;

  inproc_socket_ = zmq::socket_t(*context_, ZMQ_PULL);
  inproc_socket_.bind(MakeInProcChannelAddress(channel_));
  inproc_socket_.set(zmq::sockopt::rcvhwm, 0);
  poller_.PushSocket(inproc_socket_);

  if (port_.has_value()) {
    outproc_socket_ = zmq::socket_t(*context_, ZMQ_PULL);
    auto addr = MakeRemoteAddress(config_->protocol(), config_->local_address(), port_.value(), true /* binding */);
    outproc_socket_.set(zmq::sockopt::rcvhwm, 0);
    outproc_socket_.set(zmq::sockopt::rcvbuf, config_->broker_rcvbuf());
    outproc_socket_.bind(addr);
    poller_.PushSocket(outproc_socket_);

    LOG(INFO) << "Bound " << name() << " to \"" << addr << "\"";
  }

  if (metrics_manager_ != nullptr) {
    metrics_manager_->RegisterCurrentThread();
  }

  Initialize();
}

bool NetworkedModule::Loop() {
  if (!poller_.NextEvent(recv_retries_ > 0 /* dont_wait */)) {
    return false;
  }

  if (OnEnvelopeReceived(RecvEnvelope(inproc_socket_, true /* dont_wait */))) {
    recv_retries_ = kRecvRetries;
  }

  if (outproc_socket_.handle() != ZMQ_NULLPTR) {
    if (zmq::message_t msg; outproc_socket_.recv(msg, zmq::recv_flags::dontwait)) {
      auto env = DeserializeEnvelope(msg);
      if (OnEnvelopeReceived(move(env))) {
        recv_retries_ = kRecvRetries;
      }
    }
  }

  if (OnCustomSocket()) {
    recv_retries_ = kRecvRetries;
  }

  if (recv_retries_ > 0) {
    recv_retries_--;
  }

  return false;
}

bool NetworkedModule::OnEnvelopeReceived(EnvelopePtr&& wrapped_env) {
  if (wrapped_env == nullptr) {
    return false;
  }
  EnvelopePtr env;
  if (wrapped_env->type_case() == Envelope::TypeCase::kRaw) {
    env.reset(new Envelope());
    if (DeserializeProto(*env, wrapped_env->raw().data(), wrapped_env->raw().size())) {
      env->set_from(wrapped_env->from());
    }
  } else {
    env = move(wrapped_env);
  }

  if (env->has_request()) {
    OnInternalRequestReceived(move(env));
  } else if (env->has_response()) {
    OnInternalResponseReceived(move(env));
  }

  return true;
}

void NetworkedModule::Send(const Envelope& env, MachineId to_machine_id, Channel to_channel) {
  sender_.Send(env, to_machine_id, to_channel);
}

void NetworkedModule::Send(EnvelopePtr&& env, MachineId to_machine_id, Channel to_channel) {
  sender_.Send(move(env), to_machine_id, to_channel);
}

void NetworkedModule::Send(EnvelopePtr&& env, Channel to_channel) { sender_.Send(move(env), to_channel); }

void NetworkedModule::Send(const Envelope& env, const std::vector<MachineId>& to_machine_ids, Channel to_channel) {
  sender_.Send(env, to_machine_ids, to_channel);
}

void NetworkedModule::Send(EnvelopePtr&& env, const std::vector<MachineId>& to_machine_ids, Channel to_channel) {
  sender_.Send(move(env), to_machine_ids, to_channel);
}

Poller::Handle NetworkedModule::NewTimedCallback(std::chrono::microseconds timeout, std::function<void()>&& cb) {
  return poller_.AddTimedCallback(timeout, std::move(cb));
}

void NetworkedModule::RemoveTimedCallback(const Poller::Handle& id) { poller_.RemoveTimedCallback(id); }

}  // namespace slog