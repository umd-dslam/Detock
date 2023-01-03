#include "module/janus/tarjan.h"

#include <glog/logging.h>

using std::vector;
using std::unordered_map;
using std::unordered_set;

namespace slog {

TarjanSCCsFinder::TarjanSCCsFinder(Graph& graph) : graph_(graph), id_counter_(0) {}

void TarjanSCCsFinder::FindSCCs(Vertex& v, TxnHorizon& execution_horizon) {
  id_counter_++;
  v.disc = id_counter_;
  v.low = id_counter_;
  v.on_stack = true;
  stack_.push_back(v.txn_id);

  for (auto next : v.deps) {
    auto next_id = next.txn_id();
    if (next_id == v.txn_id || execution_horizon.contains(next_id)) {
      continue;
    }
    if (auto next_v_it = graph_.find(next_id); next_v_it == graph_.end()) {
      missing_deps_.insert({next_id, next});
    } else {
      auto& next_v = next_v_it->second;
      if (next_v.txn_id == 0) {
        FindSCCs(next_v, execution_horizon);
        
        if (!missing_deps_.empty()) {
          return;
        }

        v.low = std::min(v.low, next_v.low);
      } else {
        if (next_v.on_stack) {
          v.low = std::min(v.low, next_v.disc);
        }
      }
    }
  }

  if (!missing_deps_.empty()) {
    return;
  }

  if (v.disc == v.low) {
    auto& scc = sccs_.emplace_back();
    while (stack_.back() != v.txn_id) {
      auto member_it = graph_.find(stack_.back());
      CHECK(member_it != graph_.end());

      member_it->second.on_stack = false;
      scc.push_back(member_it->second.txn_id);
      stack_.pop_back();
    }
    scc.push_back(v.txn_id);
    std::sort(scc.begin(), scc.end());
  }
}

TarjanResult TarjanSCCsFinder::Finalize() {
  TarjanResult result;
  sccs_.swap(result.sccs);

  for (auto& [_, dep] : missing_deps_) {
    result.missing_deps.emplace_back(std::move(dep));
  }
  missing_deps_.clear();

  while (!stack_.empty()) {
    auto top = stack_.back();
    
    auto it = graph_.find(top);
    CHECK(it != graph_.end());

    // Reset disc of vertices that are visited but not in any scc yet
    it->second.disc = 0;

    result.visited.insert(top);

    stack_.pop_back();
  }

  // Reset id counter
  id_counter_ = 0;

  return result;
}

}  // namespace slog