#include <memory>
#include <vector>

#include "common/configuration.h"
#include "common/constants.h"
#include "common/metrics.h"
#include "common/types.h"
#include "connection/broker.h"
#include "module/janus/acceptor.h"
#include "module/janus/coordinator.h"
#include "module/janus/scheduler.h"
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
using std::pair;
using std::unique_ptr;
using std::vector;

int main(int argc, char* argv[]) {
  slog::InitializeService(&argc, &argv);

  auto zmq_version = zmq::version();
  LOG(INFO) << "ZMQ version " << std::get<0>(zmq_version) << "." << std::get<1>(zmq_version) << "."
            << std::get<2>(zmq_version);

  CHECK(!FLAGS_address.empty()) << "Address must not be empty";
  auto config = slog::Configuration::FromFile(FLAGS_config, FLAGS_address);

  INIT_RECORDING(config);

  LOG(INFO) << "Local region: " << (int)config->local_region();
  LOG(INFO) << "Local replica: " << (int)config->local_replica();
  LOG(INFO) << "Local partition: " << config->local_partition();
  LOG(INFO) << "Execution type: " << ENUM_NAME(config->execution_type(), slog::internal::ExecutionType);

  auto broker = Broker::New(config);

  auto config_name = FLAGS_config;
  if (auto pos = config_name.rfind('/'); pos != std::string::npos) {
    config_name = config_name.substr(pos + 1);
  }
  auto metrics_manager = make_shared<slog::MetricsRepositoryManager>(config_name, config);

  // Create and initialize storage layer
  auto [storage, metadata_initializer] = slog::MakeStorage(config, FLAGS_data_dir);

  vector<pair<unique_ptr<slog::ModuleRunner>, slog::ModuleId>> modules;
  // clang-format off
  modules.emplace_back(MakeRunnerFor<slog::Server>(broker, metrics_manager),
                       slog::ModuleId::SERVER);
  modules.emplace_back(MakeRunnerFor<janus::Coordinator>(broker->context(), broker->config(), metrics_manager),
                       slog::ModuleId::FORWARDER);
  modules.emplace_back(MakeRunnerFor<janus::Acceptor>(broker->context(), broker->config(), metrics_manager),
                       slog::ModuleId::SEQUENCER);
  modules.emplace_back(MakeRunnerFor<janus::Scheduler>(broker, storage, metrics_manager),
                       slog::ModuleId::SCHEDULER);
  // clang-format on

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