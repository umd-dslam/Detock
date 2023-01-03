#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "module/janus/horizon.h"
#include "proto/internal.pb.h"

namespace janus {

using slog::TxnId;
using slog::internal::JanusDependency;

struct Vertex {
  explicit Vertex(TxnId txn_id, const std::vector<JanusDependency>& deps)
      : txn_id(txn_id), deps(deps), disc(0), low(0), on_stack(false) {}

  const TxnId txn_id;
  const std::vector<JanusDependency> deps;

  int disc;
  int low;
  bool on_stack;
};

using Graph = std::unordered_map<TxnId, Vertex>;
using SCC = std::vector<TxnId>;

struct TarjanResult {
  std::vector<SCC> sccs;
  std::vector<JanusDependency> missing_deps;
  std::unordered_set<TxnId> visited;
};

class TarjanSCCsFinder {
 public:
  TarjanSCCsFinder(Graph& graph);
  void FindSCCs(Vertex& v, TxnHorizon& execution_horizon);
  TarjanResult Finalize();

 private:
  Graph& graph_;
  std::vector<TxnId> stack_;
  std::vector<SCC> sccs_;
  std::unordered_map<TxnId, JanusDependency> missing_deps_;
  int id_counter_;
};

}  // namespace janus