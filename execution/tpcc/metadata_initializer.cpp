#include "execution/tpcc/metadata_initializer.h"

#include <glog/logging.h>

namespace slog {
namespace tpcc {

TPCCMetadataInitializer::TPCCMetadataInitializer(uint32_t num_regions, uint32_t num_partitions)
    : num_regions_(num_regions), num_partitions_(num_partitions) {}

Metadata TPCCMetadataInitializer::Compute(const Key& key) {
  CHECK_GE(key.size(), 4) << "Invalid key";
  uint32_t warehouse_id = *reinterpret_cast<const uint32_t*>(key.data());
  return Metadata(((warehouse_id - 1) / num_partitions_) % num_regions_);
}

}  // namespace tpcc
}  // namespace slog