#include <iostream>
#include "common/rate_limiter.h"

using namespace slog;

using Clock = std::chrono::steady_clock;

int main() {
  int requests = 200000;
  RateLimiter limiter(29000);

  auto last_report = Clock::now();
  auto last_request = Clock::now();

  int last_report_count = 0;
  
  for (;;) {
    auto now = Clock::now();

    if (now - last_request >= std::chrono::microseconds(10)) {
      last_request = now;
      if (limiter.Request()) {
        requests--;
        last_report_count++;
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