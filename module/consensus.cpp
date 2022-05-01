#include "module/consensus.h"

#include "common/proto_utils.h"
#include "module/log_manager.h"

namespace slog {

using internal::Request;

namespace {

vector<MachineId> GetMembers(const ConfigurationPtr& config) {
  auto local_reg = config->local_region();
  auto num_replicas = config->num_replicas(local_reg);
  vector<MachineId> members;
  if (num_replicas == 1) {
    // For the experiment, we make the partitions in the current
    // replica voters in Paxos
    members.reserve(config->num_partitions());
    for (int part = 0; part < config->num_partitions(); part++) {
      members.push_back(MakeMachineId(local_reg, 0, part));
    }
  } else {
    // If there are more than 1 replica, create one voter in
    // each replica
    members.reserve(num_replicas);
    for (int rep = 0; rep < num_replicas; rep++) {
      members.push_back(MakeMachineId(local_reg, rep, 0));
    }
  }

  return members;
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

void GlobalPaxos::OnCommit(uint32_t slot, uint32_t value, MachineId leader) {
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

void LocalPaxos::OnCommit(uint32_t slot, uint32_t value, MachineId leader) {
  auto env = NewEnvelope();
  auto order = env->mutable_request()->mutable_forward_batch_order()->mutable_local_batch_order();
  order->set_queue_id(value);
  order->set_slot(slot);
  order->set_leader(leader);
  Send(std::move(env), local_log_channel_);
}

}  // namespace slog