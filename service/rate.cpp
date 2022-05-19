#include <iostream>
#include "common/rate_limiter.h"

using namespace slog;

using Clock = std::chrono::steady_clock;

int main() {
  int requests = 200000;
  int done = 0;
  RateLimiter limiter(5000);

  bool mute_started = false;
  auto mute = Clock::now();
  auto last_report = Clock::now();
  auto last_request = Clock::now();

  int last_report_count = 0;
  
  for (;;) {
    auto now = Clock::now();

    bool make_request = false;
    if (now - last_request >= std::chrono::microseconds(10)) {
      if (mute_started) {
        if (now - mute >= std::chrono::seconds(2)) {
          mute_started = false;
          make_request = true;
        }
      } else {
        make_request = true;
      }
    }

    if (make_request) {
      last_request = now;
      if (limiter.Request()) {
        requests--;
        done++;
        last_report_count++;

        if (done == 10000 || done == 50000) {
          mute_started = true;
          mute = now;
        }

        if (requests == 0)
          break;
      }
    }

    if (now - last_report >= std::chrono::seconds(1)) {
      last_report = now;
      std::cout << "Request rate: " << last_report_count << " rps" << std::endl;
      last_report_count = 0;
    }
  }
  std::cout << "Request rate: " << last_report_count << " rps" << std::endl;
}