#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "module/janus/horizon.h"
#include "proto/internal.pb.h"

namespace janus {

inline std::ostream& operator<<(std::ostream& os, const std::pair<TxnId, bool>& txn) {
  return os << "(" << txn.first << ", " << txn.second << ")";
}

#define OSTREAM_IMPL(Container)                                               \
  inline std::ostream& operator<<(std::ostream& os, const Container& elems) { \
    bool first = true;                                                        \
    os << "[";                                                                \
    for (const auto& elem : elems) {                                          \
      if (!first) os << ", ";                                                 \
      os << elem;                                                             \
      first = false;                                                          \
    }                                                                         \
    os << "]";                                                                \
    return os;                                                                \
  }

#define COMMA ,

OSTREAM_IMPL(std::unordered_set<TxnId>);
OSTREAM_IMPL(std::vector<std::pair<TxnId COMMA bool>>);

using slog::TxnId;
using slog::internal::JanusDependency;

struct Vertex {
  explicit Vertex(TxnId txn_id, bool is_local, const std::vector<JanusDependency>& deps)
      : txn_id(txn_id), is_local(is_local), deps(deps), disc(0), low(0), missing_deps(0), on_stack(false) {}

  const TxnId txn_id;
  const bool is_local;
  const std::vector<JanusDependency> deps;

  int disc;
  int low;
  int missing_deps;
  bool on_stack;
};

using Graph = std::unordered_map<TxnId, Vertex>;
using SCC = std::vector<std::pair<TxnId, bool>>;  // the second element is whether the txn is local or not

struct TarjanResult {
  std::vector<SCC> sccs;
  std::vector<JanusDependency> missing_deps;
  std::unordered_set<TxnId> unready;
};

class TarjanSCCsFinder {
 public:
  TarjanSCCsFinder(Graph& graph, TxnHorizon& execution_horizon);
  void FindSCCs(Vertex& v);
  TarjanResult Finalize();

 private:
  Graph& graph_;
  TxnHorizon& execution_horizon_;
  std::vector<TxnId> stack_;
  std::vector<SCC> sccs_;
  std::unordered_map<TxnId, JanusDependency> missing_deps_;
  int id_counter_;
};

}  // namespace janus