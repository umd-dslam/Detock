#include <memory>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/metrics.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/clock_synchronizer.h"
#include "module/consensus.h"
#include "module/forwarder.h"
#include "module/log_manager.h"
#include "module/multi_home_orderer.h"
#include "module/scheduler.h"
#include "module/sequencer.h"
#include "module/server.h"
#include "proto/internal.pb.h"
#include "service/service_utils.h"
#include "storage/init.h"
#include "version.h"

DEFINE_string(config, "slog.conf", "Path to the configuration file");
DEFINE_string(address, "", "Address of the local machine");
DEFINE_string(data_dir, "", "Directory containing intial data");

using slog::Broker;
using slog::ConfigurationPtr;
using slog::MakeRunnerFor;

using std::make_shared;

int main(int argc, char* argv[]) {
  slog::InitializeService(&argc, &argv);

  LOG(INFO) << "SLOG version: " << SLOG_VERSION;
  auto zmq_version = zmq::version();
  LOG(INFO) << "ZMQ version " << std::get<0>(zmq_version) << "." << std::get<1>(zmq_version) << "."
            << std::get<2>(zmq_version);

#ifdef REMASTER_PROTOCOL_SIMPLE
  LOG(INFO) << "Simple remaster protocol";
#elif defined REMASTER_PROTOCOL_PER_KEY
  LOG(INFO) << "Per key remaster protocol";
#elif defined REMASTER_PROTOCOL_COUNTERLESS
  LOG(INFO) << "Counterless remaster protocol";
#else
  LOG(INFO) << "Remastering disabled";
#endif /* REMASTER_PROTOCOL_SIMPLE */

  CHECK(!FLAGS_address.empty()) << "Address must not be empty";
  auto config = slog::Configuration::FromFile(FLAGS_config, FLAGS_address);

  INIT_RECORDING(config);

  LOG(INFO) << "Local region: " << (int)config->local_region();
  LOG(INFO) << "Local replica: " << (int)config->local_replica();
  LOG(INFO) << "Local partition: " << config->local_partition();
  std::ostringstream os;
  for (auto r : config->replication_order()) {
    os << r << " ";
  }
  LOG(INFO) << "Replication order: " << os.str();
  LOG(INFO) << "Execution type: " << ENUM_NAME(config->execution_type(), slog::internal::ExecutionType);

  auto broker = Broker::New(config);

  auto config_name = FLAGS_config;
  if (auto pos = config_name.rfind('/'); pos != std::string::npos) {
    config_name = config_name.substr(pos + 1);
  }
  auto metrics_manager = make_shared<slog::MetricsRepositoryManager>(config_name, config);

  unique_ptr<slog::ModuleRunner> clock_sync;
  if (config->clock_synchronizer_port() != 0) {
    // Start the clock synchronizer early so that it runs while data is generating
    clock_sync = MakeRunnerFor<slog::ClockSynchronizer>(broker->context(), broker->config(), metrics_manager);
    clock_sync->StartInNewThread();
  }

  // Create and initialize storage layer
  auto [storage, metadata_initializer] = slog::MakeStorage(config, FLAGS_data_dir);

  vector<pair<unique_ptr<slog::ModuleRunner>, slog::ModuleId>> modules;
  // clang-format off
  modules.emplace_back(MakeRunnerFor<slog::Server>(broker, metrics_manager),
                       slog::ModuleId::SERVER);
  modules.emplace_back(MakeRunnerFor<slog::MultiHomeOrderer>(broker, metrics_manager),
                       slog::ModuleId::MHORDERER);
  modules.emplace_back(MakeRunnerFor<slog::LocalPaxos>(broker),
                       slog::ModuleId::LOCALPAXOS);
  modules.emplace_back(MakeRunnerFor<slog::Forwarder>(broker->context(), broker->config(), storage,
                                                      metadata_initializer, metrics_manager),
                       slog::ModuleId::FORWARDER);
  modules.emplace_back(MakeRunnerFor<slog::Sequencer>(broker->context(), broker->config(), metrics_manager),
                       slog::ModuleId::SEQUENCER);
  modules.emplace_back(MakeRunnerFor<slog::Scheduler>(broker, storage, metrics_manager),
                       slog::ModuleId::SCHEDULER);
  // clang-format on

  auto num_log_managers = broker->config()->num_log_managers();
  for (int i = 0; i < num_log_managers; i++) {
    std::vector<slog::RegionId> regions;
    for (int r = 0; r < broker->config()->num_regions(); r++) {
      if (r % num_log_managers == i) {
        regions.push_back(r);
      }
    }
    modules.emplace_back(MakeRunnerFor<slog::LogManager>(i, regions, broker, metrics_manager),
                         slog::ModuleId::LOG_MANAGER);
  }

  // One region is selected to globally order the multihome batches
  if (config->num_regions() > 1 && config->leader_region_for_multi_home_ordering() == config->local_region()) {
    modules.emplace_back(MakeRunnerFor<slog::GlobalPaxos>(broker), slog::ModuleId::GLOBALPAXOS);
  }

  // Block SIGINT from here so that the new threads inherit the block mask
  sigset_t signal_set;
  sigemptyset(&signal_set);
  sigaddset(&signal_set, SIGINT);
  pthread_sigmask(SIG_BLOCK, &signal_set, nullptr);

  // New modules cannot be bound to the broker after it starts so start
  // the Broker only after it is used to initialized all modules above.
  broker->StartInNewThreads();
  for (auto& [module, id] : modules) {
    std::optional<uint32_t> cpu;
    if (auto cpus = config->cpu_pinnings(id); !cpus.empty()) {
      cpu = cpus.front();
    }
    module->StartInNewThread(cpu);
  }

  // Suspense this thread until receiving SIGINT
  int sig;
  sigwait(&signal_set, &sig);

  // Shutdown all threads
  for (auto& module : modules) {
    module.first->Stop();
  }
  broker->Stop();

  return 0;
}