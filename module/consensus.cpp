#include "module/consensus.h"

#include "common/proto_utils.h"
#include "module/log_manager.h"

namespace slog {

using internal::Request;

namespace {

const ReplicaId kLeaderReplica = 0;
const PartitionId kLeaderPartition = 0;

Members GetMembers(const ConfigurationPtr& config) {
  auto local_reg = config->local_region();
  auto num_replicas = config->num_replicas(local_reg);
  auto num_partitions = config->num_partitions();
  vector<MachineId> acceptors;
  vector<MachineId> learners;

  // All machines in the leader's replica are always the learners
  for (int part = 0; part < num_partitions; part++) {
    learners.push_back(MakeMachineId(local_reg, kLeaderReplica, part));
  }

  // All leaders partition in other replicas are always the learners
  for (int rep = 0; rep < num_replicas; rep++) {
    if (rep != kLeaderReplica) learners.push_back(MakeMachineId(local_reg, rep, kLeaderPartition));
  }

  // If local synchronous replication is enabled, the leader
  // partitons of local replicas are acceptors
  if (config->local_sync_replication()) {
    int cutoff = (num_replicas - 1) / 2 * 2 + 1;
    acceptors.reserve(cutoff);
    for (int rep = 0; rep < cutoff; rep++) {
      acceptors.push_back(MakeMachineId(local_reg, rep, kLeaderPartition));
    }
  }
  // Otherwise, the partitions in the leader replica are acceptors
  else {
    int cutoff = (num_partitions - 1) / 2 * 2 + 1;
    acceptors.reserve(cutoff);
    for (int part = 0; part < cutoff; part++) {
      acceptors.push_back(MakeMachineId(local_reg, kLeaderReplica, part));
    }
  }

  return {acceptors, learners};
}

}  // namespace

GlobalPaxos::GlobalPaxos(const shared_ptr<Broker>& broker, std::chrono::milliseconds poll_timeout)
    : SimulatedMultiPaxos(kGlobalPaxos, broker, GetMembers(broker->config()), broker->config()->local_machine_id(),
                          poll_timeout),
      local_machine_id_(broker->config()->local_machine_id()) {
  auto& config = broker->config();
  auto part = config->leader_partition_for_multi_home_ordering();
  for (int reg = 0; reg < config->num_regions(); reg++) {
    multihome_orderers_.push_back(MakeMachineId(reg, 0, part));
  }
}

void GlobalPaxos::OnCommit(uint32_t slot, int64_t value, MachineId leader) {
  if (local_machine_id_ != leader) {
    return;
  }
  auto env = NewEnvelope();
  auto order = env->mutable_request()->mutable_forward_batch_order()->mutable_remote_batch_order();
  order->set_slot(slot);
  order->set_batch_id(value);
  Send(std::move(env), multihome_orderers_, kMultiHomeOrdererChannel);
}

LocalPaxos::LocalPaxos(const shared_ptr<Broker>& broker, std::chrono::milliseconds poll_timeout)
    : SimulatedMultiPaxos(kLocalPaxos, broker, GetMembers(broker->config()), broker->config()->local_machine_id(),
                          poll_timeout),
      local_log_channel_(kLogManagerChannel + broker->config()->local_region() % broker->config()->num_log_managers()) {
}

void LocalPaxos::OnCommit(uint32_t slot, int64_t value, MachineId leader) {
  auto env = NewEnvelope();
  auto order = env->mutable_request()->mutable_forward_batch_order()->mutable_local_batch_order();
  order->set_generator(value);
  order->set_slot(slot);
  order->set_leader(leader);
  Send(std::move(env), local_log_channel_);
}

}  // namespace slog