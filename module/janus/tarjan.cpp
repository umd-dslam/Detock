#include "module/janus/tarjan.h"

#include <glog/logging.h>

using std::vector;
using std::unordered_set;

namespace slog {

TarjanSCCsFinder::TarjanSCCsFinder() : id_counter_(0) {}

TarjanResult TarjanSCCsFinder::FindSCCs(Graph& graph, Vertex& v, TxnHorizon& execution_horizon) {
  id_counter_++;
  v.disc = id_counter_;
  v.low = id_counter_;
  v.on_stack = true;
  stack_.push_back(v.txn_id);

  for (auto next : v.dep) {
    if (next == v.txn_id) {
      continue;
    }
    if (auto next_v_it = graph.find(next); next_v_it == graph.end()) {
      missing_vertices_.insert(next);
    } else {
      auto& next_v = next_v_it->second;
      if (next_v.txn_id == 0) {
        auto result = FindSCCs(graph, next_v, execution_horizon);
        
        if (result == TarjanResult::MISSING_DEPENDENCIES_AND_MAYBE_FOUND) {
          return result;
        }

        v.low = std::min(v.low, next_v.low);
      } else {
        if (next_v.on_stack) {
          v.low = std::min(v.low, next_v.disc);
        }
      }
    }
  }

  if (missing_vertices_.empty() && v.disc == v.low) {
    auto& scc = sccs_.emplace_back();
    while (stack_.back() != v.txn_id) {
      auto member_it = graph.find(stack_.back());
      CHECK(member_it != graph.end());

      member_it->second.on_stack = false;
      scc.push_back(member_it->second.txn_id);
      stack_.pop_back();
    }
    scc.push_back(v.txn_id);
    std::sort(scc.begin(), scc.end());
    return TarjanResult::FOUND;
  }

  return TarjanResult::NOT_FOUND;
}

vector<SCC> TarjanSCCsFinder::TakeSCCs() {
  vector<SCC> sccs;
  sccs_.swap(sccs);
  return sccs;
}

unordered_set<TxnId> TarjanSCCsFinder::TakeMissingVertices() {
  unordered_set<TxnId> vertices;
  missing_vertices_.swap(vertices);
  return vertices;
}

}  // namespace slog