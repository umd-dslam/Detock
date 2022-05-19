#pragma once

#include <vector>
#include <zmq.hpp>

#include "common/constants.h"
#include "common/metrics.h"
#include "common/types.h"
#include "connection/broker.h"
#include "connection/poller.h"
#include "connection/sender.h"
#include "connection/zmq_utils.h"
#include "module/base/module.h"
#include "proto/internal.pb.h"

namespace slog {

/**
 * Base class for modules that can send and receive in internal messages.
 */
class NetworkedModule : public Module {
 public:
  /**
   * Don't receive external messages.
   */
  NetworkedModule(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config, Channel channel,
                  const MetricsRepositoryManagerPtr& metrics_manager,
                  std::optional<std::chrono::milliseconds> poll_timeout, bool is_long_sender);

  /**
   * Use broker to receive external messages.
   */
  NetworkedModule(const std::shared_ptr<Broker>& broker, Broker::ChannelOption chopt,
                  const MetricsRepositoryManagerPtr& metrics_manager,
                  std::optional<std::chrono::milliseconds> poll_timeout, bool is_long_sender = false);

  /**
   * Use owned socket to receive external messages.
   */
  NetworkedModule(const std::shared_ptr<zmq::context_t>& context, const ConfigurationPtr& config, uint32_t port,
                  Channel channel, const MetricsRepositoryManagerPtr& metrics_manager,
                  std::optional<std::chrono::milliseconds> poll_timeout, bool is_long_sender = false);

  MetricsRepositoryManager& metrics_manager() { return *metrics_manager_; }

 protected:
  /**
   * Called once when the module starts
   */
  virtual void Initialize(){};

  virtual void OnInternalRequestReceived(EnvelopePtr&& env) = 0;

  virtual void OnInternalResponseReceived(EnvelopePtr&& /* env */) {}

  // Returns true if useful work was done
  virtual bool OnCustomSocket() { return false; }

  void AddCustomSocket(zmq::socket_t&& new_socket);
  zmq::socket_t& GetCustomSocket(size_t i);

  inline static EnvelopePtr NewEnvelope() { return std::make_unique<internal::Envelope>(); }
  void Send(const internal::Envelope& env, MachineId to_machine_id, Channel to_channel);
  void Send(EnvelopePtr&& env, MachineId to_machine_id, Channel to_channel);
  void Send(EnvelopePtr&& env, Channel to_channel);
  void Send(const internal::Envelope& env, const std::vector<MachineId>& to_machine_ids, Channel to_channel);
  void Send(EnvelopePtr&& env, const std::vector<MachineId>& to_machine_ids, Channel to_channel);

  // Returns the callback's id
  Poller::Handle NewTimedCallback(std::chrono::microseconds timeout, std::function<void()>&& cb);
  void RemoveTimedCallback(const Poller::Handle& handle);

  const std::shared_ptr<zmq::context_t>& context() const { return context_; }
  const ConfigurationPtr& config() const { return config_; }

  Channel channel() const { return channel_; }

 private:
  void SetUp() final;
  bool Loop() final;

  bool OnEnvelopeReceived(EnvelopePtr&& wrapped_env);

  std::shared_ptr<zmq::context_t> context_;
  ConfigurationPtr config_;
  Channel channel_;
  std::optional<uint32_t> port_;
  MetricsRepositoryManagerPtr metrics_manager_;
  zmq::socket_t inproc_socket_;
  zmq::socket_t outproc_socket_;
  std::vector<zmq::socket_t> custom_sockets_;
  Sender sender_;
  Poller poller_;
  int recv_retries_;

  std::string debug_info_;

  std::atomic<uint64_t> work_ = 0;
};

}  // namespace slog