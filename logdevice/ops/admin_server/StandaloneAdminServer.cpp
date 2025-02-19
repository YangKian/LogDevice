/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/ops/admin_server/StandaloneAdminServer.h"

#include <iostream>

#include <folly/futures/Future.h>

#include "logdevice/admin/AdminAPIHandler.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/common/ConfigInit.h"
#include "logdevice/common/NodesConfigurationInit.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/WheelTimer.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/plugin/LocationProvider.h"
#include "logdevice/common/plugin/ThriftServerFactory.h"
#include "logdevice/common/plugin/TraceLoggerFactory.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/RsmServerSnapshotStoreFactory.h"
#include "logdevice/server/thrift/SimpleThriftServer.h"

namespace facebook { namespace logdevice { namespace admin {
StandaloneAdminServer::StandaloneAdminServer(
    std::shared_ptr<PluginRegistry> plugins,
    std::shared_ptr<SettingsUpdater> settings_updater)
    : plugin_registry_(std::move(plugins)),
      settings_updater_(std::move(settings_updater)) {
  settings_updater_->registerSettings(admin_settings_);
  settings_updater_->registerSettings(server_settings_);
  settings_updater_->registerSettings(rebuilding_settings_);
  settings_updater_->registerSettings(locallogstore_settings_);
  settings_updater_->registerSettings(gossip_settings_);
  settings_updater_->registerSettings(settings_);
  settings_updater_->registerSettings(rocksdb_settings_);

  plugin_registry_->addOptions(settings_updater_.get());
  server_settings_subscription_ = server_settings_.subscribeToUpdates(
      std::bind(&StandaloneAdminServer::onSettingsUpdate, this));
}

void StandaloneAdminServer::start() {
  // ASCII ART
  ld_info(
      R"(
   __                ___           _
  / /  ___   __ _   /   \_____   _(_) ___ ___
 / /  / _ \ / _` | / /\ / _ \ \ / / |/ __/ _ \
/ /__| (_) | (_| |/ /_//  __/\ V /| | (_|  __/
\____/\___/ \__, /___,' \___| \_/ |_|\___\___|   Admin Server!
            |___/

  )");
  ld_info("Starting Standalone Admin Server");

  if (!folly::kIsDebug) {
    ld_info("asserts off (NDEBUG set)");
  } else {
    ld_info("asserts on (NDEBUG not set)");
  }

  ld_info("Config path: %s", server_settings_->config_path.c_str());
  std::string socket_addr;
  if (!admin_settings_->admin_unix_socket.empty()) {
    socket_addr = admin_settings_->admin_unix_socket;
  } else {
    socket_addr = std::to_string(admin_settings_->admin_port);
  }

  ld_info("Listening on: %s", socket_addr.c_str());
  ld_info(
      "Plugins loaded: %s", plugin_registry_->getStateDescriptionStr().c_str());

  std::shared_ptr<LocationProvider> location_plugin =
      plugin_registry_->getSinglePlugin<LocationProvider>(
          PluginType::LOCATION_PROVIDER);
  std::string plugin_location =
      location_plugin ? location_plugin->getMyLocation() : "";
  auto location = settings_->client_location;
  if (!location.has_value() && !plugin_location.empty()) {
    // if my-location was not specified, set the value to what the plugin
    // provides.
    folly::Optional<NodeLocation> res;
    res.assign(NodeLocation());
    if (res->fromDomainString(plugin_location) != 0) {
      // TODO
      /*
       *throw boost::program_options::error(
       *    "Invalid value for --my-location. Expecting valid location "
       *    "string: \"{region}.{dc}.{cluster}.{row}.{rack}\"");
       */
    }
    // settings_->client_location = res;
  }
  // Loading the config
  updateable_config_ = std::make_shared<UpdateableConfig>();

  server_config_subscription_ =
      updateable_config_->updateableServerConfig()->addHook(std::bind(
          &StandaloneAdminServer::onConfigUpdate, this, std::placeholders::_1));

  nodes_configuration_subscription_ =
      updateable_config_->updateableNodesConfiguration()->addHook(
          std::bind(&StandaloneAdminServer::onNodesConfigurationUpdate,
                    this,
                    std::placeholders::_1));

  initServerConfig();
  initNodesConfiguration();
  initStatsCollection();
  initProcessor();
  initNodesConfigurationManager();
  initLogsConfigManager();
  initClusterStateRefresher();
  initEventLog();
  initClusterMaintenanceStateMachine();
  initAdminServer();
}

void StandaloneAdminServer::initServerConfig() {
  ld_check(updateable_config_);
  ld_check(plugin_registry_);

  ConfigInit config_init(settings_->initial_config_load_timeout);
  int rv = config_init.attach(server_settings_->config_path,
                              plugin_registry_,
                              updateable_config_,
                              nullptr /* RemoteLogsConfig*/,
                              settings_);
  if (rv != 0) {
    ld_critical("Could not load the config file.");
    throw StandaloneAdminServerFailed();
  }
}

void StandaloneAdminServer::initNodesConfiguration() {
  using namespace facebook::logdevice::configuration::nodes;
  ld_check(updateable_config_);
  ld_check(plugin_registry_);

  if (!settings_->enable_nodes_configuration_manager) {
    ld_info("Not fetching the inital NodesConfiguration because "
            "NodesConfigurationManager is disabled.");
    return;
  }

  auto store = buildNodesConfigurationStore();
  // Create an empty NC in the NCS if it doesn't exist already. Most of the
  // time, this is a single read RTT (because the NC will be there), so it
  // should be fine to always do it.
  store->updateConfigSync(
      NodesConfigurationCodec::serialize(NodesConfiguration()),
      NodesConfigurationStore::Condition::createIfNotExists());
  NodesConfigurationInit config_init(std::move(store), settings_);
  // The store used by the standalone admin server shouldn't require a
  // procoessor. It's either a Rqlite NCS or a FileBasedNCS.
  auto success = config_init.initWithoutProcessor(
      updateable_config_->updateableNodesConfiguration());
  if (!success) {
    ld_critical("Failed to load the initial NodesConfiguration.");
    throw StandaloneAdminServerFailed();
  }
  ld_check(updateable_config_->getNodesConfiguration() != nullptr);
}

void StandaloneAdminServer::initProcessor() {
  std::shared_ptr<TraceLogger> trace_logger;
  std::shared_ptr<TraceLoggerFactory> trace_logger_factory =
      plugin_registry_->getSinglePlugin<TraceLoggerFactory>(
          PluginType::TRACE_LOGGER_FACTORY);
  if (!trace_logger_factory || settings_->trace_logger_disabled) {
    trace_logger = std::make_shared<NoopTraceLogger>(
        updateable_config_, /* my_node_id */ folly::none);
  } else {
    trace_logger = (*trace_logger_factory)(
        updateable_config_, /* my_node_id */ folly::none);
  }

  processor_ = ClientProcessor::create(updateable_config_,
                                       std::move(trace_logger),
                                       settings_,
                                       stats_.get(),
                                       plugin_registry_,
                                       /* credentials= */ "",
                                       "admin-server");
}

void StandaloneAdminServer::initNodesConfigurationManager() {
  using namespace facebook::logdevice::configuration::nodes;

  ld_check(processor_);
  ld_check(updateable_config_);

  if (!settings_->enable_nodes_configuration_manager) {
    ld_info(
        "NodesConfigurationManager is not enabled in the settings. Moving on.");
    return;
  }

  auto initial_nc = updateable_config_->getNodesConfiguration();
  ld_check(initial_nc);

  auto ncm = NodesConfigurationManagerFactory::create(
      NodesConfigurationManager::OperationMode::forTooling(),
      processor_.get(),
      // TODO: get NCS from NodesConfigurationInit instead
      buildNodesConfigurationStore());
  if (ncm == nullptr) {
    ld_critical("Unable to create NodesConfigurationManager during server "
                "creation!");
    throw ConstructorFailed();
  }

  if (!ncm->init(std::move(initial_nc))) {
    ld_critical(
        "Processing initial NodesConfiguration did not finish in time.");
    throw ConstructorFailed();
  }
  ld_info("NodesConfigurationManager started successfully.");
}

void StandaloneAdminServer::initLogsConfigManager() {
  ld_check(processor_);
  auto snapshot_store = RsmServerSnapshotStoreFactory::create(
      processor_.get(),
      settings_->rsm_snapshot_store_type,
      false, /* is_storage_node */
      std::to_string(configuration::InternalLogs::CONFIG_LOG_DELTAS.val_));
  if (!LogsConfigManager::createAndAttach(
          *processor_, std::move(snapshot_store), false /* is_writable */)) {
    err = E::INVALID_CONFIG;
    ld_critical("Internal LogsConfig Manager could not be started in Client. "
                "LogsConfig will not be available!");
    throw StandaloneAdminServerFailed();
  }
}

void StandaloneAdminServer::initAdminServer() {
  // Figure out the socket address for the admin server.
  auto server_config = updateable_config_->getServerConfig();
  ld_check(server_config);

  // Create a CPU thread pool executor.
  // TODO: Remove me when we have a shared CPU thread pool executor in processor
  cpu_executor_ = std::make_shared<folly::CPUThreadPoolExecutor>(25);
  folly::setCPUExecutor(cpu_executor_);

  Sockaddr listen_addr;
  if (!admin_settings_->admin_unix_socket.empty()) {
    listen_addr = Sockaddr(admin_settings_->admin_unix_socket);
  } else {
    listen_addr = Sockaddr("::", admin_settings_->admin_port);
  }

  std::string name = "LogDevice Admin API Service";
  api_handler_ = std::make_shared<AdminAPIHandler>(name,
                                                   processor_.get(),
                                                   settings_updater_,
                                                   server_settings_,
                                                   admin_settings_,
                                                   stats_.get());

  auto factory_plugin = plugin_registry_->getSinglePlugin<ThriftServerFactory>(
      PluginType::THRIFT_SERVER_FACTORY);
  if (factory_plugin) {
    admin_server_ = (*factory_plugin)(
        name, listen_addr, api_handler_, processor_->getRequestExecutor());
  } else {
    // Fallback to built-in SimpleThriftApiServer
    admin_server_ = std::make_unique<SimpleThriftServer>(
        name, listen_addr, api_handler_, processor_->getRequestExecutor());
  }

  ld_check(admin_server_);
  createAndAttachMaintenanceManager(api_handler_.get());
  admin_server_->start();
}

void StandaloneAdminServer::initClusterStateRefresher() {
  if (processor_ && processor_->cluster_state_) {
    processor_->cluster_state_->refreshClusterStateAsync();
    processor_->getWheelTimer().createTimer(
        [&]() { this->initClusterStateRefresher(); },
        settings_->cluster_state_refresh_interval);
  }
}

void StandaloneAdminServer::initStatsCollection() {
  if (settings_->stats_collection_interval.count() > 0) {
    auto params = StatsParams().setIsServer(false).setStatsSet(
        StatsParams::StatsSet::ADMIN_SERVER);
    // avoid instantianting thread-local Stats unnecessarily
    stats_ = std::make_unique<StatsHolder>(std::move(params));
  }
  // TODO: Validate that SSL Certificates exist
  stats_thread_ = StatsCollectionThread::maybeCreate(
      settings_,
      updateable_config_->get()->serverConfig(),
      plugin_registry_,
      /* num_shards */ 0,
      stats_.get());
}

void StandaloneAdminServer::initEventLog() {
  auto snapshot_store = RsmServerSnapshotStoreFactory::create(
      processor_.get(),
      settings_->rsm_snapshot_store_type,
      false, /* is_storage_node */
      std::to_string(configuration::InternalLogs::EVENT_LOG_DELTAS.val_));
  auto workerType = EventLogStateMachine::workerType(processor_.get());
  auto workerId = worker_id_t(EventLogStateMachine::getWorkerIdx(
      processor_->getWorkerCount(workerType)));
  event_log_ = std::make_unique<EventLogStateMachine>(
      settings_, std::move(snapshot_store), workerId, workerType);
  event_log_->enableSendingUpdatesToWorkers();

  std::unique_ptr<Request> req =
      std::make_unique<StartEventLogStateMachineRequest>(event_log_.get());

  const int rv = processor_->postRequest(req);
  if (rv != 0) {
    ld_error("Cannot post request to start event log state machine: %s (%s)",
             error_name(err),
             error_description(err));
    throw StandaloneAdminServerFailed();
  }
}

void StandaloneAdminServer::initClusterMaintenanceStateMachine() {
  if (admin_settings_->enable_cluster_maintenance_state_machine ||
      admin_settings_->enable_maintenance_manager) {
    cluster_maintenance_state_machine_ =
        std::make_unique<maintenance::ClusterMaintenanceStateMachine>(
            admin_settings_, nullptr /* snapshot store */);

    std::unique_ptr<Request> req = std::make_unique<
        maintenance::StartClusterMaintenanceStateMachineRequest>(
        cluster_maintenance_state_machine_.get(),
        maintenance::ClusterMaintenanceStateMachine::workerType(
            processor_.get()));

    const int rv = processor_->postRequest(req);
    if (rv != 0) {
      ld_error("Cannot post request to start cluster maintenance state "
               "machine: %s (%s)",
               error_name(err),
               error_description(err));
      throw StandaloneAdminServerFailed();
    }
  }
}

void StandaloneAdminServer::createAndAttachMaintenanceManager(
    AdminAPIHandler* handler) {
  ld_check(handler);
  ld_check(event_log_);

  if (admin_settings_->enable_maintenance_manager) {
    ld_check(cluster_maintenance_state_machine_);
    auto deps = std::make_unique<maintenance::MaintenanceManagerDependencies>(
        processor_.get(),
        admin_settings_,
        rebuilding_settings_,
        cluster_maintenance_state_machine_.get(),
        event_log_.get(),
        std::make_unique<maintenance::SafetyCheckScheduler>(
            processor_.get(), admin_settings_, handler->getSafetyChecker()),
        std::make_unique<maintenance::MaintenanceLogWriter>(processor_.get()),
        std::make_unique<maintenance::MaintenanceManagerTracer>(
            processor_->getTraceLogger()));
    auto worker_idx = processor_->selectWorkerRandomly(
        configuration::InternalLogs::MAINTENANCE_LOG_DELTAS.val_ /*seed*/,
        maintenance::MaintenanceManager::workerType(processor_.get()));
    auto& w = processor_->getWorker(
        worker_idx,
        maintenance::MaintenanceManager::workerType(processor_.get()));
    maintenance_manager_ =
        std::make_unique<maintenance::MaintenanceManager>(&w, std::move(deps));
    handler->setMaintenanceManager(maintenance_manager_.get());
    maintenance_manager_->start();
  } else {
    ld_info(
        "Not initializing MaintenanceManager since it is disabled in settings");
  }
}

void StandaloneAdminServer::shutdown() {
  SteadyTimestamp start_ts(SteadyTimestamp::now());
  SCOPE_EXIT {
    ld_info("Shutting down took%lums", msec_since(start_ts.timePoint()));
  };
  ld_info("Initiating shutdown");
  server_config_subscription_.deregister();
  ld_info("Stopping AdminServer, no new requests after this point.");
  if (admin_server_) {
    admin_server_->stop();
    ld_info("Admin API server stopped accepting requests");
  }
  if (maintenance_manager_) {
    maintenance_manager_->stop();
  }
  if (processor_) {
    ld_info("Stopping accepting work on all workers.");
    std::vector<folly::SemiFuture<folly::Unit>> futures =
        fulfill_on_all_workers<folly::Unit>(
            processor_.get(),
            [](folly::Promise<folly::Unit> p) -> void {
              auto* worker = Worker::onThisThread();
              worker->stopAcceptingWork();
              p.setValue();
            },
            /* request_type = */ RequestType::MISC,
            /* with_retrying = */ true);
    ld_info("Waiting for workers to acknowledge.");

    folly::collectAll(futures.begin(), futures.end()).get();
    ld_info("Workers acknowledged stopping accepting new work");

    ld_info("Finishing work and closing sockets on all workers.");
    futures = fulfill_on_all_workers<folly::Unit>(
        processor_.get(),
        [](folly::Promise<folly::Unit> p) -> void {
          auto* worker = Worker::onThisThread();
          worker->finishWorkAndCloseSockets();
          p.setValue();
        },
        /* request_type = */ RequestType::MISC,
        /* with_retrying = */ true);
    ld_info("Waiting for workers to acknowledge.");

    folly::collectAll(futures.begin(), futures.end()).get();
    ld_info("Workers finished all works.");

    if (stats_thread_) {
      ld_info("Stopping StatsCollectionThread.");
      stats_thread_->shutDown();
      stats_thread_.reset();
      ld_info("StatsCollectionThread Stopped.");
    }

    // Prevent the admin server from holding a dangling pointer to the
    // maintenance manager
    api_handler_->setMaintenanceManager(nullptr);

    maintenance_manager_.reset();
    cluster_maintenance_state_machine_.reset();

    ld_info("Stopping Processor");
    processor_->waitForWorkers();
    processor_->shutdown();
    if (admin_server_) {
      ld_info("Destroying AdminServer");
      admin_server_.reset();
    }
  }

  shutdown_requested_.store(true);
  main_thread_sem_.post();
}

static void set_admin_server_log_file(
    const UpdateableSettings<ServerSettings>& server_settings) {
  static std::string prev;
  if (prev == server_settings->log_file) {
    // This setting did not change.
    return;
  }

  ld_info("Logging to %s",
          server_settings->log_file.empty()
              ? "stderr"
              : server_settings->log_file.c_str());

  if (!server_settings->log_file.empty()) {
    int log_file_fd = open(
        server_settings->log_file.c_str(), O_APPEND | O_CREAT | O_WRONLY, 0666);
    if (log_file_fd >= 0) {
      dbg::useFD(log_file_fd);
    } else {
      ld_error("Failed to open error log file %s. Will keep logging to %s",
               server_settings->log_file.c_str(),
               prev.empty() ? "stderr" : prev.c_str());
    }
  } else {
    dbg::useFD(STDERR_FILENO);
  }

  dbg::enableNonblockingPipe();

  prev = server_settings->log_file;
}

void StandaloneAdminServer::onSettingsUpdate() {
  dbg::assertOnData = server_settings_->assert_on_data;
  dbg::currentLevel = server_settings_->loglevel;
  dbg::setLogLevelOverrides(server_settings_->loglevel_overrides);

  set_admin_server_log_file(server_settings_);
}

bool StandaloneAdminServer::onConfigUpdate(ServerConfig& config) {
  SteadyTimestamp start_ts(SteadyTimestamp::now());
  SCOPE_EXIT {
    ld_info("Updating settings from config took %lums",
            msec_since(start_ts.timePoint()));
  };
  auto settings = config.getServerSettingsConfig();
  if (settings_updater_) {
    // Ensure that settings are updated when we receive new config.
    settings_updater_->setFromConfig(settings);
  }
  return true;
}

bool StandaloneAdminServer::onNodesConfigurationUpdate(
    const NodesConfiguration& config) {
  return allNodesHaveName(config);
}

bool StandaloneAdminServer::allNodesHaveName(const NodesConfiguration& config) {
  for (const auto& node : *config.getServiceDiscovery()) {
    if (node.second.name == "") {
      ld_error("N%d doesn't have a name. Rejecting config ..", node.first);
      return false;
    }
  }
  return true;
}

// Builds an an admin client based NodesConfigurationStore
std::unique_ptr<configuration::nodes::NodesConfigurationStore>
StandaloneAdminServer::buildNodesConfigurationStore() {
  using namespace configuration::nodes;
  // AdminServer should use an admin compatible NCS
  settings_updater_->setInternalSetting("admin-client-capabilities", "true");
  return NodesConfigurationStoreFactory::create(
      *updateable_config_->get(), *settings_.get());
}

void StandaloneAdminServer::waitForShutdown() {
  for (;;) {
    main_thread_sem_.wait();
    if (shutdown_requested_.load()) {
      break;
    }
    ld_check(false);
  }
}
}}} // namespace facebook::logdevice::admin
