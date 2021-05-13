#include "common/clock.h"

namespace slog {

std::atomic<int64_t> slog_clock::offset_ = 0;

slog_clock::time_point slog_clock::now() {
  return std::chrono::system_clock::now() + std::chrono::nanoseconds(offset_);
}

}  // namespace slog