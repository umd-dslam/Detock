#pragma once

#include "common/types.h"

namespace slog {

class Storage {
 public:
  virtual ~Storage() = default;
  virtual bool Read(const Key& key, Record& result) const = 0;
  virtual void Write(const Key& key, const Record& record) = 0;
  virtual void Write(const Key& key, Record&& record) { Write(key, record); };
  virtual bool Delete(const Key& key) = 0;
};

}  // namespace slog