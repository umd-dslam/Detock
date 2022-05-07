#include "workload/basic.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <algorithm>
#include <iomanip>
#include <random>
#include <sstream>
#include <unordered_set>

#include "common/offline_data_reader.h"
#include "common/proto_utils.h"
#include "proto/offline_data.pb.h"

using std::bernoulli_distribution;
using std::iota;
using std::sample;
using std::unordered_set;

namespace slog {
namespace {

// Percentage of multi-home transactions
constexpr char MH_PCT[] = "mh";
// Max number of regions selected as homes in a multi-home transaction
constexpr char MH_HOMES[] = "mh_homes";
// Zipf coefficient for selecting regions to access in a txn. Must be non-negative.
// The lower this is, the more uniform the regions are selected
constexpr char MH_ZIPF[] = "mh_zipf";
// Percentage of multi-partition transactions
constexpr char MP_PCT[] = "mp";
// Max number of partitions selected as parts of a multi-partition transaction
constexpr char MP_PARTS[] = "mp_parts";
// Number of hot keys per partition. The actual number of
// hot keys won't match exactly the specified number but will be close.
// Precisely, it will be:
//        floor(hot / num_regions) * num_regions
constexpr char HOT[] = "hot";
// Number of records in a transaction
constexpr char RECORDS[] = "records";
// Number of hot records in a transaction
constexpr char HOT_RECORDS[] = "hot_records";
// Number of write records in a transaction
constexpr char WRITES[] = "writes";
// Size of a written value in bytes
constexpr char VALUE_SIZE[] = "value_size";
// If set to 1, a SH txn will always be sent to the nearest
// region, a MH txn will always have a part that touches the nearest region
constexpr char NEAREST[] = "nearest";
// Partition that is used in a single-partition transaction.
// Use a negative number to select a random partition for
// each transaction
constexpr char SP_PARTITION[] = "sp_partition";
// Home that is used in a single-home transaction.
// The NEAREST parameter is ignored if this is positive
constexpr char SH_HOME[] = "sh_home";

const RawParamMap DEFAULT_PARAMS = {{MH_PCT, "0"},   {MH_HOMES, "2"},     {MH_ZIPF, "0"},  {MP_PCT, "0"},
                                    {MP_PARTS, "2"}, {HOT, "0"},          {RECORDS, "10"}, {HOT_RECORDS, "0"},
                                    {WRITES, "10"},  {VALUE_SIZE, "100"}, {NEAREST, "1"},  {SP_PARTITION, "-1"},
                                    {SH_HOME, "-1"}};

// For the Calvin experiment, there is a single region, so replace the regions by the replicas so that
// we generate the same workload as other experiments
int GetNumRegions(const ConfigurationPtr& config) {
  return config->num_regions() == 1 ? config->num_replicas(config->local_region()) : config->num_regions();
}

}  // namespace

BasicWorkload::BasicWorkload(const ConfigurationPtr& config, RegionId region, ReplicaId replica, const string& data_dir,
                             const string& params_str, const uint32_t seed, const RawParamMap& extra_default_params)
    : Workload(MergeParams(extra_default_params, DEFAULT_PARAMS), params_str),
      config_(config),
      local_region_(region),
      local_replica_(replica),
      distance_ranking_(config->distance_ranking_from(region)),
      zipf_coef_(params_.GetInt32(MH_ZIPF)),
      partition_to_key_lists_(config->num_partitions()),
      rg_(seed),
      rnd_str_(seed),
      client_txn_id_counter_(0) {
  name_ = "basic";
  auto num_regions = GetNumRegions(config);
  auto num_partitions = config->num_partitions();
  auto hot_keys_per_list = std::max(1U, params_.GetUInt32(HOT) / num_regions);
  const auto& proto_config = config->proto_config();
  for (int part = 0; part < num_partitions; part++) {
    for (int reg = 0; reg < num_regions; reg++) {
      // Initialize hot keys limit for each key list. When keys are added to a list,
      // the first keys are considered hot keys until this limit is reached and any new
      // keys from there are cold keys.
      switch (proto_config.partitioning_case()) {
        case internal::Configuration::kSimplePartitioning: {
          partition_to_key_lists_[part].emplace_back(config, part, reg, hot_keys_per_list);
          break;
        }
        case internal::Configuration::kHashPartitioning: {
          partition_to_key_lists_[part].emplace_back(hot_keys_per_list);
          break;
        }
        default:
          LOG(FATAL) << "Invalid partioning mode: "
                     << CASE_NAME(proto_config.partitioning_case(), internal::Configuration);
      }
    }
  }

  if (distance_ranking_.empty()) {
    for (size_t i = 0; i < static_cast<size_t>(num_regions); i++) {
      if (i != static_cast<size_t>(local_region())) {
        distance_ranking_.push_back(i);
      }
    }
    if (zipf_coef_ > 0) {
      LOG(WARNING) << "Distance ranking is not provided. MH_ZIPF is reset to 0.";
      zipf_coef_ = 0;
    }
  } else if (config_->num_regions() == 1) {
    // This case is for the Calvin experiment where there is only a single region.
    // The num_regions variable is equal to num_replicas at this point
    CHECK_EQ(distance_ranking_.size(), num_regions * (num_regions - 1));
    size_t from = local_region() * (num_regions - 1);
    std::copy_n(distance_ranking_.begin() + from, num_regions, distance_ranking_.begin());
    distance_ranking_.resize(num_regions - 1);
  }

  CHECK_EQ(distance_ranking_.size(), num_regions - 1) << "Distance ranking size must match the number of regions";

  if (!params_.GetInt32(NEAREST)) {
    distance_ranking_.insert(distance_ranking_.begin(), local_region());
  }

  if (proto_config.partitioning_case() == internal::Configuration::kHashPartitioning) {
    // Load and index the initial data from file if simple partitioning is not used
    for (int partition = 0; partition < num_partitions; partition++) {
      auto data_file = data_dir + "/" + std::to_string(partition) + ".dat";
      auto fd = open(data_file.c_str(), O_RDONLY);
      if (fd < 0) {
        LOG(FATAL) << "Error while loading \"" << data_file << "\": " << strerror(errno);
      }

      OfflineDataReader reader(fd);
      LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums from " << data_file;
      while (reader.HasNextDatum()) {
        auto datum = reader.GetNextDatum();
        CHECK_LT(datum.master(), num_regions) << "Master number exceeds number of regions";

        partition_to_key_lists_[partition][datum.master()].AddKey(datum.key());
      }
      close(fd);
    }
  }
}

std::pair<Transaction*, TransactionProfile> BasicWorkload::NextTransaction() {
  TransactionProfile pro;

  pro.client_txn_id = client_txn_id_counter_;

  // Decide if this is a multi-partition txn or not
  auto num_partitions = config_->num_partitions();
  auto multi_partition_pct = params_.GetDouble(MP_PCT);
  bernoulli_distribution is_mp(multi_partition_pct / 100);
  pro.is_multi_partition = is_mp(rg_);

  // Select a number of partitions to choose from for each record
  vector<uint32_t> selected_partitions;
  if (pro.is_multi_partition) {
    CHECK_GE(num_partitions, 2) << "There must be at least 2 partitions for MP txns";

    // Generate a permuation of 0..num_partitions-1
    selected_partitions.resize(num_partitions);
    iota(selected_partitions.begin(), selected_partitions.end(), 0);
    shuffle(selected_partitions.begin(), selected_partitions.end(), rg_);

    // Compute number of needed partitions
    auto max_num_partitions = std::min(num_partitions, params_.GetInt32(MP_PARTS));
    CHECK_GE(max_num_partitions, 2) << "At least 2 partitions must be selected for MP txns";

    // Select the first max_num_partitions
    std::uniform_int_distribution num_partitions(2, max_num_partitions);
    selected_partitions.resize(num_partitions(rg_));
  } else {
    auto sp_partition = params_.GetInt32(SP_PARTITION);
    if (sp_partition < 0) {
      // Pick a random partition if no specific partition is given
      std::uniform_int_distribution<uint32_t> dis(0, num_partitions - 1);
      selected_partitions.push_back(dis(rg_));
    } else {
      // Use the given partition
      CHECK_LT(static_cast<uint32_t>(sp_partition), num_partitions)
          << "Selected single-partition partition does not exist";
      selected_partitions.push_back(sp_partition);
    }
  }

  // Decide if this is a multi-home txn or not
  auto num_regions = GetNumRegions(config_);
  auto multi_home_pct = params_.GetDouble(MH_PCT);
  bernoulli_distribution is_mh(multi_home_pct / 100);
  pro.is_multi_home = is_mh(rg_);

  // Select a number of homes to choose from for each record
  vector<uint32_t> selected_homes;
  if (pro.is_multi_home) {
    CHECK_GE(num_regions, 2) << "There must be at least 2 regions for MH txns";
    auto max_num_homes = std::min(params_.GetInt32(MH_HOMES), num_regions);

    CHECK_GE(max_num_homes, 2) << "At least 2 regions must be selected for MH txns";
    auto num_homes = std::uniform_int_distribution{2, max_num_homes}(rg_);
    selected_homes.reserve(num_homes);

    if (params_.GetInt32(NEAREST)) {
      selected_homes.push_back(local_region());
      num_homes--;
    }
    auto sampled_homes = zipf_sample(rg_, zipf_coef_, distance_ranking_, num_homes);
    selected_homes.insert(selected_homes.end(), sampled_homes.begin(), sampled_homes.end());
  } else {
    if (params_.GetInt32(SH_HOME) >= 0) {
      selected_homes.push_back(params_.GetInt32(SH_HOME));
    } else {
      if (params_.GetInt32(NEAREST)) {
        selected_homes.push_back(local_region());
      } else {
        std::uniform_int_distribution<uint32_t> dis(0, num_regions - 1);
        selected_homes.push_back(dis(rg_));
      }
    }
  }

  vector<KeyMetadata> keys;
  vector<vector<string>> code;

  auto writes = params_.GetUInt32(WRITES);
  auto hot_records = params_.GetUInt32(HOT_RECORDS);
  auto records = params_.GetUInt32(RECORDS);
  auto value_size = params_.GetUInt32(VALUE_SIZE);

  CHECK_LE(writes, records) << "Number of writes cannot exceed number of records in a transaction!";
  CHECK_LE(hot_records, records) << "Number of hot records cannot exceed number of records in a transaction!";

  // Randomly decide which records are hot or not
  std::vector<bool> is_hot(hot_records, true);
  is_hot.resize(records);
  std::shuffle(is_hot.begin(), is_hot.end(), rg_);

  for (size_t i = 0; i < records; i++) {
    // Round-robin the partitions
    auto partition = selected_partitions[i % selected_partitions.size()];
    // Evenly divide the records to the selected homes
    auto home = selected_homes[i / ((records + 1) / selected_homes.size())];
    for (;;) {
      Key key;
      if (is_hot[i]) {
        key = partition_to_key_lists_[partition][home].GetRandomHotKey(rg_);
      } else {
        key = partition_to_key_lists_[partition][home].GetRandomColdKey(rg_);
      }

      auto ins = pro.records.try_emplace(key, TransactionProfile::Record());
      if (ins.second) {
        auto& record = ins.first->second;
        record.is_hot = is_hot[i];
        // Decide whether this is a read or a write record
        if (i < writes) {
          code.push_back({"SET", key, rnd_str_(value_size)});
          keys.emplace_back(key, KeyType::WRITE);
          record.is_write = true;
        } else {
          code.push_back({"GET", key});
          keys.emplace_back(key, KeyType::READ);
          record.is_write = false;
        }
        record.home = home;
        record.partition = partition;
        break;
      }
    }
  }

  // Construct a new transaction
  auto txn = MakeTransaction(keys, code);
  txn->mutable_internal()->set_id(client_txn_id_counter_);

  client_txn_id_counter_++;

  return {txn, pro};
}

}  // namespace slog