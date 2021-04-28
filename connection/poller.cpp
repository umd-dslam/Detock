#include "connection/poller.h"

using namespace std::chrono;

using std::optional;
using std::vector;

namespace slog {

Poller::Poller(optional<microseconds> timeout) : poll_timeout_(timeout) {}

void Poller::PushSocket(zmq::socket_t& socket) {
  poll_items_.push_back({
      socket.handle(), 0, /* fd */
      ZMQ_POLLIN, 0       /* revent */
  });
}

bool Poller::NextEvent(bool dont_wait) {
  auto may_has_msg = true;
  if (!dont_wait) {
    // Compute the time that we need to wait until the next event
    auto shortest_timeout = poll_timeout_;
    auto now = std::chrono::steady_clock::now();
    if (!timed_callbacks_.empty()) {
      const auto& next_ev_time = timed_callbacks_.top().when;
      if (next_ev_time <= now) {
        shortest_timeout = 0us;
      } else if (!shortest_timeout.has_value() || next_ev_time - now < shortest_timeout.value()) {
        shortest_timeout = duration_cast<microseconds>(next_ev_time - now);
      }
    }

    int rc = 0;
    // Wait until the next time event or some timeout.
    if (shortest_timeout.has_value()) {
      // By casting the timeout from microseconds to milliseconds, if it is below 1ms,
      // the casting result will be 0 and thus poll becomes non-blocking. This is intended
      // so that we spin wait instead of sleeping, making waiting more accurate.
      rc = zmq::poll(poll_items_, duration_cast<milliseconds>(shortest_timeout.value()));
    } else {
      // No timed event to wait, wait until there is a new message
      rc = zmq::poll(poll_items_, -1);
    }
    may_has_msg = rc > 0;
  }

  // Process and clean up triggered callbacks
  if (!timed_callbacks_.empty()) {
    auto now = std::chrono::steady_clock::now();
    while (!timed_callbacks_.empty() && timed_callbacks_.top().when <= now) {
      timed_callbacks_.top().callback();
      timed_callbacks_.pop();
    }
  }

  return may_has_msg;
}

bool Poller::is_socket_ready(size_t i) const { return poll_items_[i].revents & ZMQ_POLLIN; }

void Poller::AddTimedCallback(microseconds timeout, std::function<void()>&& cb) {
  timed_callbacks_.push({.when = std::chrono::steady_clock::now() + timeout, .callback = move(cb)});
}

void Poller::ClearTimedCallbacks() { timed_callbacks_ = {}; }

}  // namespace slog