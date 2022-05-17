#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/types.h"
#include "proto/internal.pb.h"

using std::string;
using std::unique_ptr;
using std::unordered_map;
using std::unordered_set;
using std::vector;

namespace slog {

using EnvelopePtr = unique_ptr<internal::Envelope>;

class SimulatedMultiPaxos;

struct Members {
  Members(const std::vector<MachineId>& acceptors, const std::vector<MachineId>& learners)
      : acceptors(acceptors), learners(learners) {}

  const std::vector<MachineId> acceptors;
  const std::vector<MachineId> learners;
};

struct PaxosInstance {
  PaxosInstance(uint32_t ballot, uint64_t value) : ballot(ballot), value(value), num_accepts(0), num_commits(0) {}

  uint32_t ballot;
  uint64_t value;
  int num_accepts;
  int num_commits;
};

class Leader {
 public:
  /**
   * @param paxos     The enclosing Paxos class
   * @param members   Machine Ids of acceptors and learners
   * @param me        Machine Id of the current machine
   */
  Leader(SimulatedMultiPaxos& paxos, Members members, MachineId me);

  void HandleRequest(const internal::Envelope& req);
  void HandleResponse(const internal::Envelope& res);

 private:
  void ProcessCommitRequest(const internal::PaxosCommitRequest& commit);
  void StartNewInstance(uint64_t value);

  SimulatedMultiPaxos& paxos_;

  Members members_;
  const MachineId me_;
  bool is_elected_;
  MachineId elected_leader_;

  SlotId next_empty_slot_;
  uint32_t ballot_;
  unordered_map<SlotId, PaxosInstance> instances_;
};
}  // namespace slog