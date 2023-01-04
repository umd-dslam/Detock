#include "storage/init.h"

#include <fcntl.h>
#include <glog/logging.h>

#include <condition_variable>
#include <thread>
#include <vector>

#include "common/offline_data_reader.h"
#include "common/sharder.h"
#include "execution/tpcc/load_tables.h"
#include "proto/offline_data.pb.h"

namespace slog {

using std::make_shared;
using std::shared_ptr;
using std::string;

const int kDataGenThreads = 3;

static void GenerateSimpleData(shared_ptr<Storage> storage, const shared_ptr<MetadataInitializer>& metadata_initializer,
                               const ConfigurationPtr& config);
static void GenerateSimpleData2(shared_ptr<Storage> storage,
                                const shared_ptr<MetadataInitializer>& metadata_initializer,
                                const ConfigurationPtr& config);
static void GenerateTPCCData(shared_ptr<Storage> storage, const shared_ptr<MetadataInitializer>& metadata_initializer,
                             const ConfigurationPtr& config);
static void LoadData(Storage& storage, const ConfigurationPtr& config, const string& data_dir);

std::pair<shared_ptr<MemOnlyStorage>, shared_ptr<MetadataInitializer>> MakeStorage(const ConfigurationPtr& config,
                                                                                   const string& data_dir) {
  auto storage = std::make_shared<MemOnlyStorage>();
  shared_ptr<MetadataInitializer> metadata_initializer;
  switch (config->proto_config().partitioning_case()) {
    case internal::Configuration::kSimplePartitioning:
      metadata_initializer = make_shared<SimpleMetadataInitializer>(config->num_regions(), config->num_partitions());
      GenerateSimpleData(storage, metadata_initializer, config);
      break;
    case internal::Configuration::kSimplePartitioning2:
      metadata_initializer = make_shared<SimpleMetadataInitializer2>(config->num_regions(), config->num_partitions());
      GenerateSimpleData2(storage, metadata_initializer, config);
      break;
    case internal::Configuration::kTpccPartitioning:
      metadata_initializer =
          make_shared<tpcc::TPCCMetadataInitializer>(config->num_regions(), config->num_partitions());
      GenerateTPCCData(storage, metadata_initializer, config);
      break;
    default:
      metadata_initializer = make_shared<ConstantMetadataInitializer>(0);
      LoadData(*storage, config, data_dir);
      break;
  }
  return {storage, metadata_initializer};
}

void LoadData(Storage& storage, const ConfigurationPtr& config, const string& data_dir) {
  if (data_dir.empty()) {
    LOG(INFO) << "No initial data directory specified. Starting with an empty storage.";
    return;
  }

  auto data_file = data_dir + "/" + std::to_string(config->local_partition()) + ".dat";

  auto fd = open(data_file.c_str(), O_RDONLY);
  if (fd < 0) {
    LOG(ERROR) << "Error while loading \"" << data_file << "\": " << strerror(errno)
               << ". Starting with an empty storage.";
    return;
  }

  OfflineDataReader reader(fd);
  LOG(INFO) << "Loading " << reader.GetNumDatums() << " datums...";

  auto sharder = Sharder::MakeSharder(config);

  VLOG(1) << "First 10 datums are: ";
  int c = 10;
  while (reader.HasNextDatum()) {
    auto datum = reader.GetNextDatum();
    if (c > 0) {
      VLOG(1) << datum.key() << " " << datum.record() << " " << datum.master();
      c--;
    }

    CHECK(sharder->is_local_key(datum.key()))
        << "Key " << datum.key() << " does not belong to partition " << config->local_partition();

    CHECK_LT(datum.master(), config->num_regions()) << "Master number exceeds number of regions";

    // Write to storage
    Record record(datum.record(), datum.master());
    storage.Write(datum.key(), record);
  }
  close(fd);
}

void GenerateSimpleData(shared_ptr<Storage> storage, const shared_ptr<MetadataInitializer>& metadata_initializer,
                        const ConfigurationPtr& config) {
  auto simple_partitioning = config->proto_config().simple_partitioning();
  auto num_records = simple_partitioning.num_records();
  auto num_partitions = config->num_partitions();
  auto partition = config->local_partition();

  // Create a value of specified size by repeating the character 'a'
  string value(simple_partitioning.record_size_bytes(), 'a');

  LOG(INFO) << "Generating ~" << num_records / num_partitions << " records using " << kDataGenThreads << " threads. "
            << "Record size = " << simple_partitioning.record_size_bytes() << " bytes";

  std::atomic<uint64_t> counter = 0;
  std::atomic<size_t> num_done = 0;
  auto GenerateFn = [&](uint64_t from_key, uint64_t to_key) {
    for (uint64_t key = from_key; key < to_key; key += num_partitions) {
      Record record(value);
      record.SetMetadata(metadata_initializer->Compute(std::to_string(key)));
      storage->Write(std::to_string(key), record);
      counter++;
    }
    num_done++;
  };
  std::vector<std::thread> threads;
  uint64_t range = num_records / kDataGenThreads + 1;
  for (uint32_t i = 0; i < kDataGenThreads; i++) {
    uint64_t range_start = i * range;
    uint64_t partition_of_range_start = range_start % num_partitions;
    uint64_t distance_to_next_in_partition_key =
        (partition - partition_of_range_start + num_partitions) % num_partitions;
    uint64_t from_key = range_start + distance_to_next_in_partition_key;
    uint64_t to_key = std::min((i + 1) * range, num_records);
    threads.emplace_back(GenerateFn, from_key, to_key);
  }
  while (num_done < kDataGenThreads) {
    int attempts = 10;
    while (num_done < kDataGenThreads && attempts > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(200));
      attempts--;
    }
    int pct = 100.0 * counter.load() / num_records;
    LOG(INFO) << "Generated " << counter.load() << " records (" << pct << "%)";
  }
  for (auto& t : threads) {
    t.join();
  }
}

void GenerateSimpleData2(shared_ptr<Storage> storage, const shared_ptr<MetadataInitializer>& metadata_initializer,
                         const ConfigurationPtr& config) {
  auto simple_partitioning = config->proto_config().simple_partitioning2();
  auto num_records = simple_partitioning.num_records();
  auto num_regions = config->num_regions();
  auto num_partitions = config->num_partitions();
  auto partition = config->local_partition();

  // Create a value of specified size by repeating the character 'a'
  string value(simple_partitioning.record_size_bytes(), 'a');

  LOG(INFO) << "Generating ~" << num_records / num_partitions << " records using " << kDataGenThreads << " threads. "
            << "Record size = " << simple_partitioning.record_size_bytes() << " bytes";

  std::atomic<uint64_t> counter = 0;
  std::atomic<size_t> num_done = 0;
  auto GenerateFn = [&](uint64_t from_key, uint64_t to_key) {
    for (uint64_t key = from_key; key < to_key; key++) {
      uint32_t key_partition = key / num_regions % num_partitions;
      if (key_partition == partition) {
        Record record(value);
        record.SetMetadata(metadata_initializer->Compute(std::to_string(key)));
        storage->Write(std::to_string(key), record);
        counter++;
      }
    }
    num_done++;
  };
  std::vector<std::thread> threads;
  uint64_t range = num_records / kDataGenThreads + 1;
  for (uint32_t i = 0; i < kDataGenThreads; i++) {
    uint64_t start = i * range;
    threads.emplace_back(GenerateFn, start, std::min(start + range, num_records));
  }
  while (num_done < kDataGenThreads) {
    std::this_thread::sleep_for(std::chrono::seconds(5));
    LOG(INFO) << "Generated " << counter.load() << " records";
  }
  for (auto& t : threads) {
    t.join();
  }
}

void GenerateTPCCData(shared_ptr<Storage> storage, const shared_ptr<MetadataInitializer>& metadata_initializer,
                      const ConfigurationPtr& config) {
  auto tpcc_partitioning = config->proto_config().tpcc_partitioning();
  auto storage_adapter = std::make_shared<tpcc::KVStorageAdapter>(storage, metadata_initializer);
  tpcc::LoadTables(storage_adapter, tpcc_partitioning.warehouses(), config->num_regions(), config->num_partitions(),
                   config->local_partition(), kDataGenThreads);
}

}  // namespace slog