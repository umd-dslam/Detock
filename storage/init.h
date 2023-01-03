#pragma once

#include "common/configuration.h"
#include "execution/tpcc/metadata_initializer.h"
#include "storage/mem_only_storage.h"

namespace slog {

std::pair<std::shared_ptr<MemOnlyStorage>, std::shared_ptr<MetadataInitializer>> MakeStorage(
    const ConfigurationPtr& config, const std::string& data_dir);

}  // namespace slog