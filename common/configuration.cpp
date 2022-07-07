#include "common/configuration.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>

#include <fstream>

#include "common/constants.h"
#include "common/proto_utils.h"
#include "common/string_utils.h"

namespace slog {

using google::protobuf::io::FileInputStream;
using google::protobuf::io::ZeroCopyInputStream;
using std::chrono::milliseconds;
using std::chrono::operator""ms;
using std::string;
using std::vector;

ConfigurationPtr Configuration::FromFile(const string& file_path, const string& local_address) {
  int fd = open(file_path.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(FATAL) << "Configuration file error: " << strerror(errno);
  }
  ZeroCopyInputStream* input = new FileInputStream(fd);
  internal::Configuration config;

  google::protobuf::TextFormat::Parse(input, &config);

  delete input;
  close(fd);

  return std::make_shared<Configuration>(config, local_address);
}

Configuration::Configuration(const internal::Configuration& config, const string& local_address)
    : config_(config), local_address_(local_address), local_region_(0), local_partition_(0) {
  CHECK_LE(config_.replication_factor(), config_.regions_size())
      << "Replication factor must not exceed number of regions";
  CHECK_LT(config_.broker_ports_size(), kMaxNumBrokers) << "Too many brokers";
  CHECK_GT(config_.broker_ports_size(), 0) << "There must be at least one broker";
  CHECK_NE(config_.server_port(), 0) << "Server port must be set";
  CHECK_NE(config_.forwarder_port(), 0) << "Forwarder port must be set";
  CHECK_NE(config_.sequencer_port(), 0) << "Sequencer port must be set";
  CHECK_LT(config_.num_workers(), kMaxNumWorkers) << "Too many workers";
  CHECK_LT(config_.num_log_managers(), kMaxNumLogManagers) << "Too many log managers";
  CHECK_LT(config_.regions_size(), kMaxNumLogs) << "Too many reigons";
  CHECK_LE(config_.num_log_managers(), config_.regions_size())
      << "Number of log managers cannot exceed number of regions";

  if (config_.force_bypass_mh_orderer()) {
    config_.set_bypass_mh_orderer(true);
  } else {
#ifdef LOCK_MANAGER_DDR
    CHECK(!config_.bypass_mh_orderer() || config_.ddr_interval() > 0)
        << "Deadlock resolver must be enabled in orderer-bypassing mode";
#else
    CHECK(!config_.bypass_mh_orderer() || config_.synchronized_batching())
        << "Batching by timestamp must be enabled in orderer-bypassing mode";
#endif
  }

  bool local_address_is_valid = local_address_.empty();
  for (int reg = 0; reg < config_.regions_size(); reg++) {
    auto& region = config_.regions(reg);
    auto num_rep = region.num_replicas();
    auto num_part = config_.num_partitions();

    if (num_rep <= 0) {
      LOG(WARNING) << "No num_replica set for region " << reg << ". Assume there is one replica.";
      num_rep = 1;
    }

    CHECK_EQ((uint32_t)region.addresses_size(), num_part * num_rep)
        << "Number of addresses in each region must match num_partitions * num_replicas";

    for (size_t rep = 0; rep < num_rep; rep++) {
      for (size_t p = 0; p < num_part; p++) {
        auto& addr = region.addresses(rep * num_part + p);
        MachineId id = MakeMachineId(reg, rep, p);
        all_addresses_.emplace(id, addr);
        all_machine_ids_.push_back(id);
        if (addr == local_address) {
          local_address_is_valid = true;
          local_region_ = reg;
          local_replica_ = rep;
          local_partition_ = p;
          local_machine_id_ = id;
        }
      }
    }
  }

  CHECK(local_address_is_valid) << "The configuration does not contain the provided local machine ID: \""
                                << local_address_ << "\"";

  if (config_.execution_type() == internal::ExecutionType::TPC_C) {
    CHECK(config_.has_tpcc_partitioning()) << "TPC-C execution type can only be paired with TPC-C partitioning";
  }

  if (config_.has_tpcc_partitioning()) {
    CHECK(config_.execution_type() == internal::ExecutionType::TPC_C)
        << "TPC-C partitioning can only be paired with TPC-C execution type";
  }

  if (config_.replication_order_size() > local_region_) {
    auto order_str = Split(config_.replication_order(local_region_), ",");
    for (auto rstr : order_str) {
      auto r = std::stol(rstr);
      CHECK_LT(r, config_.regions_size()) << "Invalid region specified in the replication order";
      if (r != local_region_) {
        replication_order_.push_back(r);
      }
    }
  }
}

const internal::Configuration& Configuration::proto_config() const { return config_; }

const string& Configuration::protocol() const { return config_.protocol(); }

const string& Configuration::address(RegionId region, ReplicaId replica, PartitionId partition) const {
  auto idx = replica * config_.num_partitions() + partition;
  return config_.regions(region).addresses(idx);
}

const string& Configuration::address(MachineId machine_id) const {
  auto it = all_addresses_.find(machine_id);
  CHECK(it != all_addresses_.end()) << "Cannot find address for machine id: " << machine_id;
  return it->second;
}

int Configuration::num_regions() const { return config_.regions_size(); }

int Configuration::num_replicas(RegionId reg) const { return std::max(config_.regions(reg).num_replicas(), 1U); }

int Configuration::num_partitions() const { return config_.num_partitions(); }

int Configuration::num_workers() const { return std::max(config_.num_workers(), 1U); }

int Configuration::num_log_managers() const { return std::max(config_.num_log_managers(), 1U); }

uint32_t Configuration::broker_ports(int i) const { return config_.broker_ports(i); }
uint32_t Configuration::broker_ports_size() const { return config_.broker_ports_size(); }

uint32_t Configuration::server_port() const { return config_.server_port(); }

uint32_t Configuration::forwarder_port() const { return config_.forwarder_port(); }

uint32_t Configuration::sequencer_port() const { return config_.sequencer_port(); }

uint32_t Configuration::clock_synchronizer_port() const { return config_.clock_synchronizer_port(); }

std::chrono::milliseconds Configuration::mh_orderer_batch_duration() const {
  return milliseconds(config_.mh_orderer_batch_duration());
}

milliseconds Configuration::forwarder_batch_duration() const {
  return milliseconds(config_.forwarder_batch_duration());
}

milliseconds Configuration::sequencer_batch_duration() const {
  if (config_.sequencer_batch_duration() == 0) {
    return 1ms;
  }
  return milliseconds(config_.sequencer_batch_duration());
}

int Configuration::sequencer_batch_size() const { return config_.sequencer_batch_size(); }

bool Configuration::sequencer_rrr() const { return config_.sequencer_rrr(); }

uint32_t Configuration::replication_factor() const { return std::max(config_.replication_factor(), 1U); }

bool Configuration::local_sync_replication() const { return config_.regions(local_region_).sync_replication(); }

vector<MachineId> Configuration::all_machine_ids() const { return all_machine_ids_; }

const string& Configuration::local_address() const { return local_address_; }

RegionId Configuration::local_region() const { return local_region_; }

ReplicaId Configuration::local_replica() const { return local_replica_; }

PartitionId Configuration::local_partition() const { return local_partition_; }

MachineId Configuration::local_machine_id() const { return local_machine_id_; }

RegionId Configuration::leader_region_for_multi_home_ordering() const { return 0; }

PartitionId Configuration::leader_partition_for_multi_home_ordering() const { return 0; }

uint32_t Configuration::replication_delay_pct() const { return config_.replication_delay().delay_pct(); }

uint32_t Configuration::replication_delay_amount_ms() const { return config_.replication_delay().delay_amount_ms(); }

vector<TransactionEvent> Configuration::enabled_events() const {
  vector<TransactionEvent> res;
  res.reserve(config_.enabled_events_size());
  for (auto e : config_.enabled_events()) {
    res.push_back(TransactionEvent(e));
  }
  return res;
};

bool Configuration::bypass_mh_orderer() const { return config_.bypass_mh_orderer(); }

milliseconds Configuration::ddr_interval() const { return milliseconds(config_.ddr_interval()); };

vector<int> Configuration::cpu_pinnings(ModuleId module) const {
  vector<int> cpus;
  for (auto& entry : config_.cpu_pinnings()) {
    if (entry.module() == module) {
      cpus.push_back(entry.cpu());
    }
  }
  return cpus;
}

internal::ExecutionType Configuration::execution_type() const { return config_.execution_type(); }

const vector<uint32_t>& Configuration::replication_order() const { return replication_order_; }

bool Configuration::synchronized_batching() const { return config_.synchronized_batching(); }

const internal::MetricOptions& Configuration::metric_options() const { return config_.metric_options(); }

milliseconds Configuration::fs_latency_interval() const { return milliseconds(config_.fs_latency_interval()); }

milliseconds Configuration::clock_sync_interval() const { return milliseconds(config_.clock_sync_interval()); }

int64_t Configuration::timestamp_buffer_us() const { return config_.timestamp_buffer_us(); }

uint32_t Configuration::avg_latency_window_size() const { return std::max(config_.avg_latency_window_size(), 1U); }

bool Configuration::shrink_mh_orderer() const { return config_.regions(local_region_).shrink_mh_orderer(); };

std::vector<int> Configuration::distance_ranking_from(RegionId region_id) const {
  auto ranking_str = Split(config_.regions(region_id).distance_ranking(), ",");
  std::vector<int> ranking;
  for (auto s : ranking_str) {
    ranking.push_back(std::stoi(s));
  }
  return ranking;
}

int Configuration::broker_rcvbuf() const { return config_.broker_rcvbuf() <= 0 ? -1 : config_.broker_rcvbuf(); }

int Configuration::long_sender_sndbuf() const {
  return config_.long_sender_sndbuf() <= 0 ? -1 : config_.long_sender_sndbuf();
}

int Configuration::tps_limit() const { return config_.tps_limit(); }

}  // namespace slog