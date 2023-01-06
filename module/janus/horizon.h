#pragma once

#include "common/types.h"

namespace janus {

using slog::kMachineIdBits;
using slog::TxnId;

class TxnHorizon {
 public:
  void Add(TxnId id) {
    auto machine_id = TXN_ID_GET_MACHINE_ID(id);
    auto counter = TXN_ID_GET_COUNTER(id);
    horizons_[machine_id].Add(counter);
  }

  bool contains(TxnId id) {
    auto machine_id = TXN_ID_GET_MACHINE_ID(id);
    auto counter = TXN_ID_GET_COUNTER(id);
    return horizons_[machine_id].contains(counter);
  }

 private:
  class Horizon {
    // All transactions <= this are executed
    TxnId horizon = 0;
    std::unordered_set<TxnId> buffered;

   public:
    void Add(TxnId counter) {
      if (counter == horizon + 1) {
        horizon++;
        for (;;) {
          auto it = buffered.find(horizon + 1);
          if (it == buffered.end()) {
            break;
          }
          buffered.erase(it);
          horizon++;
        }
      } else {
        buffered.insert(counter);
      }
    }

    bool contains(TxnId counter) const {
      if (counter <= horizon) {
        return true;
      }
      return buffered.find(counter) != buffered.end();
    }
  };

  std::unordered_map<slog::MachineId, Horizon> horizons_;
};

}  // namespace janus