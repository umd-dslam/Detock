#include "storage/metadata_initializer.h"

namespace slog {

SimpleMetadataInitializer::SimpleMetadataInitializer(uint32_t num_regions, uint32_t num_partitions)
    : num_regions_(num_regions), num_partitions_(num_partitions) {}

/**
 * This initializer assumes the following home/partition assignment
 *
 *        home | 0  1  2  3  0  1  2  3  0  ...
 * ------------|-------------------------------
 * partition 0 | 0  3  6  9  12 15 18 21 24 ...
 * partition 1 | 1  4  7  10 13 16 19 22 25 ...
 * partition 2 | 2  5  8  11 14 17 20 23 26 ...
 * ------------|-------------------------------
 *             |            keys
 *
 * We divide the key by the number of parititons to get the "column number" of the key.
 * Then, taking the modulo of the column number by the number of regions gives the home
 * of the key.
 */
Metadata SimpleMetadataInitializer::Compute(const Key& key) {
  return Metadata((std::stoull(key) / num_partitions_) % num_regions_);
}

SimpleMetadataInitializer2::SimpleMetadataInitializer2(uint32_t num_regions, uint32_t num_partitions)
    : num_regions_(num_regions), num_partitions_(num_partitions) {}

/**
 * This initializer assumes the following home/partition assignment
 *
 *   partition | 0  1  2  3  0  1  2  3  0  ...
 * ------------|-------------------------------
 *      home 0 | 0  3  6  9  12 15 18 21 24 ...
 *      home 1 | 1  4  7  10 13 16 19 22 25 ...
 *      home 2 | 2  5  8  11 14 17 20 23 26 ...
 * ------------|-------------------------------
 *             |            keys
 *
 * Taking the modulo of the key by the number of regions gives the home of the key
 */
Metadata SimpleMetadataInitializer2::Compute(const Key& key) { return Metadata(std::stoull(key) % num_regions_); }

ConstantMetadataInitializer::ConstantMetadataInitializer(uint32_t home) : home_(home) {}

Metadata ConstantMetadataInitializer::Compute(const Key&) { return Metadata(home_); }

}  // namespace slog