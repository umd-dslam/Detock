#include "connection/poller.h"

using namespace std::chrono;

using std::optional;
using std::vector;

namespace slog {

Poller::Poller(optional<microseconds> timeout) : poll_timeout_(timeout), timed_callback_counter_(0) {}

void Poller::PushSocket(zmq::socket_t& socket) {
  poll_items_.push_back({
      socket.handle(), 0, /* fd */
      ZMQ_POLLIN, 0       /* revent */
  });
}

bool Poller::NextEvent(bool dont_wait) {
  auto may_have_msg = true;
  if (!dont_wait) {
    // Compute the time that we need to wait until the next event
    auto shortest_timeout = poll_timeout_;
    auto now = steady_clock::now();
    if (!timed_callbacks_.empty()) {
      const auto& next_ev_time = timed_callbacks_.begin()->first.first;
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
    may_have_msg = rc > 0;
  }

  // Process and clean up triggered callbacks
  if (!timed_callbacks_.empty()) {
    auto now = steady_clock::now();
    auto it = timed_callbacks_.begin();
    for (; it != timed_callbacks_.end(); it++) {
      if (it->first.first > now) {
        break;
      }
      it->second();
    }
    timed_callbacks_.erase(timed_callbacks_.begin(), it);
  }

  return may_have_msg;
}

bool Poller::is_socket_ready(size_t i) const { return poll_items_[i].revents & ZMQ_POLLIN; }

Poller::Handle Poller::AddTimedCallback(microseconds timeout, std::function<void()>&& cb) {
  auto id = Handle(steady_clock::now() + timeout, timed_callback_counter_);
  timed_callbacks_.emplace(id, move(cb));
  timed_callback_counter_++;
  return id;
}

void Poller::RemoveTimedCallback(const Poller::Handle& id) { timed_callbacks_.erase(id); }

}  // namespace slog