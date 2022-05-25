#pragma once

#include <chrono>
#include <mutex>

namespace slog {

using std::chrono::duration_cast;

// Limiting request rates to a fixed number of requests per second using
// the token bucket algorithm
class RateLimiter {
  using Clock = std::chrono::steady_clock;
  using Resolution = std::chrono::nanoseconds;

 public:
  RateLimiter(int req_per_sec) : time_per_req_(0), time_(0), last_check_(Clock::now()) {
    if (req_per_sec > 0) {
      time_per_req_ = Resolution::period::den / req_per_sec;
    }
  }

  bool RequestWithMutex() {
    std::lock_guard<std::mutex> _guard(mut_);
    return Request();
  }

  bool Request() {
    auto now = Clock::now();
    time_ += std::chrono::duration_cast<Resolution>(now - last_check_).count();
    last_check_ = now;

    if (time_ > Resolution::period::den / 2) time_ = Resolution::period::den / 2;

    if (time_ >= time_per_req_) {
      time_ -= time_per_req_;
      return true;
    }

    return false;
  }

 private:
  uint64_t time_per_req_;
  uint64_t time_;
  std::chrono::time_point<Clock> last_check_;

  std::mutex mut_;
};

}  // namespace slog