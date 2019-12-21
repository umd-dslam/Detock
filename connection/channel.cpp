#include "connection/channel.h"

#include <glog/logging.h>

namespace slog {

namespace {

std::string MakeEndpoint(const std::string& name) {
  return "inproc://" + name + ".ipc";
}

} // namespace 

Channel::Channel(
    std::shared_ptr<zmq::context_t> context, 
    const std::string& name) 
  : context_(context),
    name_(name),
    socket_(*context, ZMQ_PAIR),
    listener_created_(false) {
  auto endpoint = MakeEndpoint(name);
  socket_.connect(endpoint);
}

const std::string& Channel::GetName() const {
  return name_;
}

zmq::pollitem_t Channel::GetPollItem() {
  return { 
    static_cast<void*>(socket_),
    0, /* fd */
    ZMQ_POLLIN,
    0 /* revent */
  };
}

void Channel::SendToListener(const MMessage& msg) {
  msg.Send(socket_);
}

void Channel::ReceiveFromListener(MMessage& msg) {
  msg.Receive(socket_);
}

ChannelListener* Channel::GetListener() {
  CHECK(!listener_created_) << "Listener of channel \"" << name_ 
                            << "\" has already been created";
  listener_created_ = true;
  return new ChannelListener(context_, name_);
}


ChannelListener::ChannelListener(
    std::shared_ptr<zmq::context_t> context,
    const std::string& name)
  : socket_(*context, ZMQ_PAIR) {
  auto endpoint = MakeEndpoint(name);
  socket_.bind(endpoint);
}

bool ChannelListener::PollMessage(MMessage& msg, long timeout_ms) {
  zmq::pollitem_t item { 
      static_cast<void*>(socket_), 0, ZMQ_POLLIN, 0 };
  zmq::poll(&item, 1, timeout_ms);
  if (item.revents & ZMQ_POLLIN) {
    msg.Receive(socket_);
    return true;
  }
  return false;
}

void ChannelListener::SendMessage(const MMessage& msg) {
  msg.Send(socket_);
}

} // namespace slog;