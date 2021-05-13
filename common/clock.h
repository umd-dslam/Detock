#pragma once

#include <atomic>
#include <chrono>

namespace slog {

struct slog_clock {
  using time_point = std::chrono::system_clock::time_point;
  static time_point now();

 private:
  friend class ClockSynchronizer;
  static std::atomic<int64_t> offset_;
};

}  // namespace slog