#pragma once

#include <sstream>
#include <string>
#include <vector>

namespace slog {

size_t NextToken(std::string& token, const std::string& str, const std::string& delims, size_t pos = 0);

size_t NextNTokens(std::vector<std::string>& tokens, const std::string& str, const std::string& delims, size_t n = 1,
                   size_t pos = 0);

std::string Trim(std::string str);

std::vector<std::string> Split(const std::string& str, const std::string& delims);

template <typename T, typename R>
std::string Join(const std::vector<std::pair<T, R>> parts, char delim = ';') {
  std::ostringstream ss;
  bool first = true;
  for (const auto& p : parts) {
    if (!first) {
      ss << delim;
    } else {
      first = false;
    }
    ss << p.first << ":" << p.second;
  }
  return ss.str();
}

template <typename T>
std::string Join(const std::vector<T> parts, char delim = ';') {
  std::ostringstream ss;
  bool first = true;
  for (const auto& p : parts) {
    if (!first) {
      ss << delim;
    } else {
      first = false;
    }
    ss << p;
  }
  return ss.str();
}

}  // namespace slog