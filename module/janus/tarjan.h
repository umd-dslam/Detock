#pragma once

#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "module/janus/horizon.h"
#include "proto/internal.pb.h"

namespace slog {

struct Vertex {
  explicit Vertex(TxnId txn_id) : txn_id(txn_id), disc(0), low(0), on_stack(false) {}
  const TxnId txn_id;
  std::vector<internal::JanusDependency> deps;
  int disc;
  int low;
  bool on_stack;
};

using Graph = std::unordered_map<TxnId, Vertex>;
using SCC = std::vector<TxnId>;

enum class TarjanResult {
  MISSING_DEPENDENCIES_AND_MAYBE_FOUND,
  FOUND,
  NOT_FOUND,
};

class TarjanSCCsFinder {
 public:
  TarjanSCCsFinder();
  TarjanResult FindSCCs(Graph& graph, Vertex& v, TxnHorizon& execution_horizon);
  std::vector<SCC> TakeSCCs();
  std::unordered_set<TxnId> TakeMissingVertices();

 private:
  std::vector<TxnId> stack_;
  std::vector<SCC> sccs_;
  std::unordered_set<TxnId> missing_vertices_;
  int id_counter_;
};

}  // namespace slog