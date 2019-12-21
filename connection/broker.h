#pragma once

#include <thread>
#include <unordered_map>

#include <zmq.hpp>

#include "common/configuration.h"
#include "connection/channel.h"

using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::vector;

namespace slog {

/**
 * A Broker distributes messages in and out of a machine.
 * It runs its own thread with the components depicted below
 * 
 *                   -----------------------
 *                   |                     |
 *  Module A <---> Channel A             Router  <----- Incoming Message
 *                   |         B           |
 *                   |          R          |
 *  Module B <---> Channel B     O         |
 *                   |            K      Dealer  -----> Outgoing Message to XXX.XXX.XXX.XXX
 *                   |             E     Dealer  -----> Outgoing Message to YYY.YYY.YYY.YYY
 *  Module C <---> Channel C        R    Dealer  -----> Outgoing Message to ZZZ.ZZZ.ZZZ.ZZZ
 *                   |                   ...
 *                   -----------------------
 * 
 * To receive messages from other machines, it uses a ZMQ_ROUTER socket, which automatically
 * prepends a connection identity to an arriving zmq message. Using this identity, it can
 * tell where the message comes from.
 * 
 * The messages going into the system via the router will be broker to the channel
 * specified in each message. On the other end of each channel is a module which also runs
 * in its own thread. The module can also send messages back to the broker and
 * subsequently to other machines.
 * 
 * To send messages to other machines, a broker uses a ZMQ_DEALER socket for each machines.
 * 
 */
class Broker {
public:
  static const std::string SERVER_CHANNEL;
  static const std::string SEQUENCER_CHANNEL;
  static const std::string SCHEDULER_CHANNEL;

  Broker(
      shared_ptr<Configuration> config, 
      shared_ptr<zmq::context_t> context);
  ~Broker();

  void Start();

  ChannelListener* AddChannel(const string& name);

private:
  string MakeEndpoint(const string& addr = "") const;

  /**
   * A broker only starts working after every other broker is up and send a READY
   * message to everyone. There is one caveat: if after the synchronization happens, 
   * a machine goes down, and restarts, that machine cannot join anymore since the
   * READY messages are only sent once in the beginning. 
   * In production system, HEARTBEAT messages should be periodically sent out instead
   * to mitigate this problem.
   */
  bool InitializeConnection();
  
  void Run();

  shared_ptr<Configuration> config_;
  shared_ptr<zmq::context_t> context_;
  zmq::socket_t router_;
  std::atomic<bool> running_;

  std::thread thread_;

  // Map from channel name to the channel
  unordered_map<string, unique_ptr<Channel>> channels_;
  // Map from ip addresses to sockets
  unordered_map<string, unique_ptr<zmq::socket_t>> address_to_socket_;

  // Map from connection ids (zmq identities) to serialized-to-string SlogIdentifiers
  // Used to translate the identities of incoming messages
  unordered_map<string, string> connection_id_to_slog_id_;
  // Map from serialized-to-string SlogIdentifiers to IP addresses
  // Used to translate the identities of outgoing messages
  unordered_map<string, string> slog_id_to_address_;
};

} // namespace slog