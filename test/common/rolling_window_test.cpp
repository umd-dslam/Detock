#include "common/rolling_window.h"

#include <gtest/gtest.h>

using namespace std;
using namespace slog;

TEST(RollingWindowTest, Basic) {
  RollingWindow<int64_t> window(4);

  window.Add(1);
  ASSERT_EQ(window.sum(), 1);
  ASSERT_EQ(window.avg(), 1);

  window.Add(5);
  ASSERT_EQ(window.sum(), 6);
  ASSERT_EQ(window.avg(), 3);

  window.Add(9);
  ASSERT_EQ(window.sum(), 15);
  ASSERT_EQ(window.avg(), 5);

  window.Add(5);
  ASSERT_EQ(window.sum(), 20);
  ASSERT_EQ(window.avg(), 5);

  window.Add(3);
  ASSERT_EQ(window.sum(), 22);
  ASSERT_EQ(window.avg(), 5.5);

  window.Add(6);
  ASSERT_EQ(window.sum(), 23);
  ASSERT_EQ(window.avg(), 5.75);
}
