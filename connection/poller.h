#pragma once

#include <functional>
#include <list>
#include <map>
#include <optional>
#include <vector>
#include <zmq.hpp>

namespace slog {

class Poller {
 public:
  using Handle = std::pair<std::chrono::steady_clock::time_point, int>;

  Poller(std::optional<std::chrono::microseconds> timeout);

  // Returns true if it is possible that there is a message in one of the sockets
  // If dont_wait is set to true, this always return true
  bool NextEvent(bool dont_wait = false);

  void PushSocket(zmq::socket_t& socket);

  bool is_socket_ready(size_t i) const;

  Handle AddTimedCallback(std::chrono::microseconds timeout, std::function<void()>&& cb);
  void RemoveTimedCallback(const Handle& id);

 private:
  std::optional<std::chrono::microseconds> poll_timeout_;
  std::vector<zmq::pollitem_t> poll_items_;
  std::map<Handle, std::function<void()>> timed_callbacks_;
  int timed_callback_counter_;
};

}  // namespace slog