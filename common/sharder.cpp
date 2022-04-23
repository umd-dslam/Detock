#include "common/sharder.h"

namespace slog {

namespace {

template <class It>
uint32_t FNVHash(It begin, It end) {
  uint64_t hash = 0x811c9dc5;
  for (auto it = begin; it != end; it++) {
    hash = (hash * 0x01000193) % (1LL << 32);
    hash ^= *it;
  }
  return hash;
}

}  // namespace

std::shared_ptr<Sharder> Sharder::MakeSharder(const ConfigurationPtr& config) {
  if (config->proto_config().has_simple_partitioning()) {
    return std::make_shared<SimpleSharder>(config);
  } else if (config->proto_config().has_simple_partitioning2()) {
    return std::make_shared<SimpleSharder2>(config);
  } else if (config->proto_config().has_tpcc_partitioning()) {
    return std::make_shared<TPCCSharder>(config);
  }
  return std::make_shared<HashSharder>(config);
}

Sharder::Sharder(const ConfigurationPtr& config)
    : local_partition_(config->local_partition()), num_partitions_(config->num_partitions()) {}

bool Sharder::is_local_key(const Key& key) const { return compute_partition(key) == local_partition_; }

uint32_t Sharder::num_partitions() const { return num_partitions_; }

uint32_t Sharder::local_partition() const { return local_partition_; }

/**
 * Hash Sharder
 */
HashSharder::HashSharder(const ConfigurationPtr& config)
    : Sharder(config), partition_key_num_bytes_(config->proto_config().hash_partitioning().partition_key_num_bytes()) {}
uint32_t HashSharder::compute_partition(const Key& key) const {
  auto end = partition_key_num_bytes_ >= key.length() ? key.end() : key.begin() + partition_key_num_bytes_;
  return FNVHash(key.begin(), end) % num_partitions_;
}

/**
 * Simple Sharder
 *
 * This sharder assumes the following home/partition assignment
 *
 *        home | 0  1  2  3  0  1  2  3  0  ...
 * ------------|-------------------------------
 * partition 0 | 0  3  6  9  12 15 18 21 24 ...
 * partition 1 | 1  4  7  10 13 16 19 22 25 ...
 * partition 2 | 2  5  8  11 14 17 20 23 26 ...
 * ------------|-------------------------------
 *             |            keys
 *
 * Taking the modulo of the key by the number of partitions gives the partition of the key
 */
SimpleSharder::SimpleSharder(const ConfigurationPtr& config) : Sharder(config) {}
uint32_t SimpleSharder::compute_partition(const Key& key) const { return std::stoll(key) % num_partitions_; }

/**
 * Simple Sharder 2
 *
 * This sharder assumes the following home/partition assignment
 *
 *   partition | 0  1  2  3  0  1  2  3  0  ...
 * ------------|-------------------------------
 *      home 0 | 0  3  6  9  12 15 18 21 24 ...
 *      home 1 | 1  4  7  10 13 16 19 22 25 ...
 *      home 2 | 2  5  8  11 14 17 20 23 26 ...
 * ------------|-------------------------------
 *             |            keys
 *
 * We divide the key by the number of regions to get the "column number" of the key.
 * Then, taking the modulo of the column number by the number of partitions gives the partition
 * of the key.
 */
SimpleSharder2::SimpleSharder2(const ConfigurationPtr& config) : Sharder(config), num_regions_(config->num_regions()) {}
uint32_t SimpleSharder2::compute_partition(const Key& key) const {
  return (std::stoll(key) / num_regions_) % num_partitions_;
}

/**
 * TPC-C Sharder
 */
TPCCSharder::TPCCSharder(const ConfigurationPtr& config) : Sharder(config) {}
uint32_t TPCCSharder::compute_partition(const Key& key) const {
  int w_id = *reinterpret_cast<const int*>(key.data());
  return (w_id - 1) % num_partitions_;
}

}  // namespace slog