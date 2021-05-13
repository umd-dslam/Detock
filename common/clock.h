#pragma once

#include <atomic>
#include <chrono>

namespace slog {

struct slog_clock {
  static std::chrono::system_clock::time_point now();

 private:
  friend class ClockSynchronizer;

  static std::atomic<int64_t> offset_;
};

}  // namespace slog