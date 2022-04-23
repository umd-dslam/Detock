#pragma once

#include <cstddef>
#include <queue>

namespace slog {

template <typename T>
class RollingWindow {
 public:
  RollingWindow(size_t sz) : max_size_(sz), sum_(0) {}

  void Add(T val) {
    buffer_.push(val);
    sum_ += val;
    if (buffer_.size() > max_size_) {
      sum_ -= buffer_.front();
      buffer_.pop();
    }
  }

  T sum() const { return sum_; }
  float avg() const { return static_cast<float>(sum_) / buffer_.size(); }

 private:
  size_t max_size_;
  std::queue<T> buffer_;
  T sum_;
};

}  // namespace slog