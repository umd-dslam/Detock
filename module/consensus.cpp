#include "module/consensus.h"

#include "common/proto_utils.h"
#include "module/log_manager.h"

namespace slog {

using internal::Request;

namespace {

vector<MachineId> GetMembers(const ConfigurationPtr& config) {
  auto local_rep = config->local_region();
  vector<MachineId> members;
  members.reserve(config->num_partitions());
  // Enlist all machines in the same region as members
  for (uint32_t part = 0; part < config->num_partitions(); part++) {
    members.push_back(config->MakeMachineId(local_rep, part));
  }
  return members;
}

}  // namespace

GlobalPaxos::GlobalPaxos(const shared_ptr<Broker>& broker, std::chrono::milliseconds poll_timeout)
    : SimulatedMultiPaxos(kGlobalPaxos, broker, GetMembers(broker->config()), broker->config()->local_machine_id(),
                          poll_timeout),
      local_machine_id_(broker->config()->local_machine_id()) {
  auto& config = broker->config();
  for (uint32_t rep = 0; rep < config->num_regions(); rep++) {
    multihome_orderers_.push_back(config->MakeMachineId(rep, config->leader_partition_for_multi_home_ordering()));
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
      local_log_channel_(kLogManagerChannel +
                         broker->config()->local_region() % broker->config()->num_log_managers()) {}

void LocalPaxos::OnCommit(uint32_t slot, uint32_t value, MachineId leader) {
  auto env = NewEnvelope();
  auto order = env->mutable_request()->mutable_forward_batch_order()->mutable_local_batch_order();
  order->set_queue_id(value);
  order->set_slot(slot);
  order->set_leader(leader);
  Send(std::move(env), local_log_channel_);
}

}  // namespace slog