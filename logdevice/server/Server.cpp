/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/Server.h"

#include <folly/MapUtil.h>
#include <folly/io/async/EventBaseThread.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "logdevice/admin/AdminAPIHandler.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/admin/maintenance/MaintenanceManager.h"
#include "logdevice/admin/settings/AdminServerSettings.h"
#include "logdevice/common/ConfigInit.h"
#include "logdevice/common/ConnectionKind.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/CopySetManager.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/NodesConfigurationInit.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/RqliteClient.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/SequencerPlacement.h"
#include "logdevice/common/StaticSequencerPlacement.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/Node.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/configuration/nodes/NodeIndicesAllocator.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/configuration/nodes/RqliteNodesConfigurationStore.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogStateMachine.h"
#include "logdevice/common/nodeset_selection/NodeSetSelectorFactory.h"
#include "logdevice/common/plugin/ThriftServerFactory.h"
#include "logdevice/common/plugin/TraceLoggerFactory.h"
#include "logdevice/common/settings/SSLSettingValidation.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/stats/PerShardHistograms.h"
#include "logdevice/server/FailureDetector.h"
#include "logdevice/server/IOFaultInjection.h"
#include "logdevice/server/LazySequencerPlacement.h"
#include "logdevice/server/LogStoreMonitor.h"
#include "logdevice/server/MyNodeIDFinder.h"
#include "logdevice/server/NodeRegistrationHandler.h"
#include "logdevice/server/RsmServerSnapshotStoreFactory.h"
#include "logdevice/server/ServerProcessor.h"
#include "logdevice/server/UnreleasedRecordDetector.h"
#include "logdevice/server/epoch_store/FileEpochStore.h"
#include "logdevice/server/epoch_store/RqliteEpochStore.h"
#include "logdevice/server/fatalsignal.h"
#include "logdevice/server/locallogstore/ClusterMarkerChecker.h"
#include "logdevice/server/locallogstore/RocksDBMetricsExport.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/rebuilding/RebuildingCoordinator.h"
#include "logdevice/server/rebuilding/RebuildingSupervisor.h"
#include "logdevice/server/shutdown.h"
#include "logdevice/server/storage_tasks/RecordCacheRepopulationTask.h"
#include "logdevice/server/storage_tasks/ShardedStorageThreadPool.h"
#include "logdevice/server/thrift/SimpleThriftServer.h"
#include "logdevice/server/thrift/api/LogDeviceAPIThriftHandler.h"
#include "logdevice/server/util.h"

using namespace facebook::logdevice::configuration::nodes;
using facebook::logdevice::configuration::LocalLogsConfig;

namespace facebook { namespace logdevice {

static StatsHolder* errorStats = nullptr;

static void bumpErrorCounter(dbg::Level level) {
  switch (level) {
    case dbg::Level::INFO:
    case dbg::Level::NOTIFY:
    case dbg::Level::WARNING:
      STAT_INCR(errorStats, production_notices);
      return;
    case dbg::Level::ERROR:
      STAT_INCR(errorStats, severe_errors);
      return;
    case dbg::Level::CRITICAL:
    case dbg::Level::NONE:
      STAT_INCR(errorStats, critical_errors);
      return;
    case dbg::Level::SPEW:
    case dbg::Level::DEBUG:
      // Don't bother updating.
      return;
  }
  ld_check(false);
}

static ConnectionKind
priorityToConnectionKind(NodeServiceDiscovery::ClientNetworkPriority priority) {
  using Priority = NodeServiceDiscovery::ClientNetworkPriority;
  switch (priority) {
    case Priority::LOW:
      return ConnectionKind::DATA_LOW_PRIORITY;
    case Priority::MEDIUM:
      return ConnectionKind::DATA;
    case Priority::HIGH:
      return ConnectionKind::DATA_HIGH_PRIORITY;
  }
  folly::assume_unreachable();
}

bool ServerParameters::shutdownIfMyNodeInfoChanged(
    const NodesConfiguration& config) {
  if (!my_node_id_.has_value()) {
    return true;
  }

  if (!hasMyNodeInfoChanged(config)) {
    return true;
  }

  ld_critical("Configuration mismatch detected, rejecting the config.");
  if (server_settings_->shutdown_on_node_configuration_mismatch) {
    // Tempporary hack to get a quick exit until we have fencing
    // support available from WS.
    if (server_settings_->hard_exit_on_node_configuration_mismatch) {
      ld_critical(
          "--shutdown-on-node-configuration-mismatch and "
          "hard-exit-on-node-configuration-mismatch are set, hard exiting.");
      _exit(EXIT_FAILURE);
    }
    ld_critical("--shutdown-on-node-configuration-mismatch is set, gracefully "
                "shutting down.");
    requestStop();
  }
  return false;
}

bool ServerParameters::hasMyNodeInfoChanged(const NodesConfiguration& config) {
  ld_check(my_node_id_.has_value());
  ld_check(my_node_id_finder_);

  auto node_id = my_node_id_finder_->calculate(config);
  if (!node_id.has_value()) {
    ld_error("Couldn't find my node ID in config.");
    return true;
  }

  if (my_node_id_.value() != node_id) {
    ld_error("My node ID changed from %s to %s.",
             my_node_id_->toString().c_str(),
             node_id->toString().c_str());
    return true;
  }

  const auto old_service_discovery =
      updateable_config_->getNodesConfiguration()->getNodeServiceDiscovery(
          node_id->index());
  const auto new_service_discovery =
      config.getNodeServiceDiscovery(node_id->index());

  ld_check(old_service_discovery);
  ld_check(new_service_discovery);

  if (old_service_discovery->version != new_service_discovery->version) {
    ld_error("My version changed from %lu to %lu.",
             old_service_discovery->version,
             new_service_discovery->version);
    return true;
  }

  // No change detected
  return false;
}

bool ServerParameters::updateConfigSettings(ServerConfig& config) {
  SteadyTimestamp start_ts(SteadyTimestamp::now());
  SCOPE_EXIT {
    ld_info("Updating settings from config took %lums",
            msec_since(start_ts.timePoint()));
  };

  try {
    settings_updater_->setFromConfig(config.getServerSettingsConfig());
  } catch (const boost::program_options::error&) {
    return false;
  }
  return true;
}

bool ServerParameters::onServerConfigUpdate(ServerConfig& config) {
  return updateConfigSettings(config);
}

bool ServerParameters::setConnectionLimits() {
  if (server_settings_->fd_limit == 0 ||
      server_settings_->num_reserved_fds == 0) {
    ld_debug("not enforcing limits on incoming connections");
    return true;
  }

  std::shared_ptr<const Settings> settings = processor_settings_.get();
  const size_t nodes =
      updateable_config_->getNodesConfiguration()->clusterSize();
  const size_t workers = settings->num_workers;

  const int available =
      server_settings_->fd_limit - server_settings_->num_reserved_fds;
  if (available < 0) {
    ld_error("number of reserved fds (%d) is higher than the fd limit (%d)",
             server_settings_->num_reserved_fds,
             server_settings_->fd_limit);
    return false;
  }

  // To get the maximum number of connections the server's willing to accept,
  // subtract the expected number of outgoing connections -- one per worker
  // to each other node -- from the number of available fds (and some more,
  // to be on the safe side).
  int max_incoming = available - nodes * workers * 1.5;
  if (max_incoming < 1) {
    ld_error("not enough fds for incoming connections with fd limit %d and "
             "num reserved %d",
             server_settings_->fd_limit,
             server_settings_->num_reserved_fds);
    return false;
  }

  // In addition to outgoing connections, each node is expected to have one
  // connection from each of other nodes' worker threads, so take that into
  // account when calculating the max number of _client_ connections.
  int max_external = max_incoming - nodes * workers * 1.5;
  if (max_external < 1) {
    ld_error("not enough fds for external connections with fd limit %d and "
             "num reserved %d",
             server_settings_->fd_limit,
             server_settings_->num_reserved_fds);
    return false;
  }

  ld_info("Max incoming connections: %d", max_incoming);
  settings_updater_->setInternalSetting(
      "max-incoming-connections", std::to_string(max_incoming));

  ld_info("Max external connections: %d", max_external);
  settings_updater_->setInternalSetting(
      "max-external-connections", std::to_string(max_external));

  // We're not subscribing to config changes here because these require
  // restarting the server to take effect.
  STAT_SET(getStats(), fd_limit, server_settings_->fd_limit);
  STAT_SET(getStats(), num_reserved_fds, server_settings_->num_reserved_fds);
  STAT_SET(getStats(), max_incoming_connections, max_incoming);
  STAT_SET(getStats(), max_external_connections, max_external);

  return true;
}

bool ServerParameters::initMyNodeIDFinder() {
  std::unique_ptr<NodeIDMatcher> id_matcher;
  // TODO(T44427489): When name is enforced in config, we can always use the
  // name to search for ourself in the config.
  if (server_settings_->enable_node_self_registration) {
    id_matcher = NodeIDMatcher::byName(server_settings_->name);
  } else if (!server_settings_->unix_socket.empty()) {
    id_matcher = NodeIDMatcher::byUnixSocket(server_settings_->unix_socket);
  } else {
    id_matcher = NodeIDMatcher::byTCPPort(server_settings_->port);
  }

  if (id_matcher == nullptr) {
    return false;
  }

  my_node_id_finder_ = std::make_unique<MyNodeIDFinder>(std::move(id_matcher));
  return true;
}

bool ServerParameters::registerAndUpdateNodeInfo(
    std::shared_ptr<NodesConfigurationStore> nodes_configuration_store) {
  NodeRegistrationHandler handler{
      *server_settings_.get(),
      *admin_server_settings_.get(),
      updateable_config_->updateableNodesConfiguration(),
      nodes_configuration_store};
  // Find our NodeID from the published NodesConfiguration
  if (auto my_node_id = my_node_id_finder_->calculate(
          *updateable_config_->getNodesConfiguration())) {
    ld_check(my_node_id->isNodeID());
    // We store our node ID on exiting the scope to avoid being preempted during
    // self-registration process.
    SCOPE_EXIT {
      my_node_id_ = my_node_id;
    };

    if (server_settings_->enable_node_self_registration) {
      // If self registration is enabled, let's check if our version is correct.
      const auto service_discovery =
          updateable_config_->getNodesConfiguration()->getNodeServiceDiscovery(
              my_node_id->index());
      ld_check(service_discovery);
      const auto old_version = service_discovery->version;
      const auto new_version = server_settings_->version.value_or(old_version);
      const auto task_handle =
          folly::get_default(service_discovery->tags, "handle", "");
      const auto container_handle =
          folly::get_default(service_discovery->tags, "container", "");
      if (new_version < old_version) {
        ld_error("Found the node with the same name but higher version (%lu > "
                 "%lu) in the config - task handle: %s, container handle: %s",
                 old_version,
                 new_version,
                 task_handle.c_str(),
                 container_handle.c_str());
        return false;
      }
      // Now let's make sure that our attributes are up to date.
      Status status = handler.updateSelf(my_node_id->index());
      if (status == Status::OK) {
        ld_info("Successfully updated the NodesConfiguration");
        // Refetch the NodesConfiguration to detect the modification that we
        // proposed.
        initNodesConfiguration(nodes_configuration_store);
      } else if (status == Status::UPTODATE) {
        ld_info("No NodesConfiguration update is needed");
        return true;
      } else {
        ld_error("Failed to update my node info: (%s): %s",
                 error_name(status),
                 error_description(err));
        return false;
      }
    } else {
      // Self registration is not enabled. No need to validate the attributes,
      // we assume that they are correct. We're good to go!
      return true;
    }
  } else {
    if (server_settings_->enable_node_self_registration) {
      // We didn't find ourself in the config, let's register if self
      // registration is enabled otherwise abort.
      ld_check(processor_settings_->enable_nodes_configuration_manager);
      ld_check(processor_settings_
                   ->use_nodes_configuration_manager_nodes_configuration);
      auto result = handler.registerSelf(NodeIndicesAllocator{});
      if (result.hasError()) {
        ld_error("Failed to self register: (%s): %s",
                 error_name(result.error()),
                 error_description(result.error()));
        return false;
      }
      ld_info("Successfully registered as N%d", *result);
      // Refetch the NodesConfiguration to detect the modification that we
      // proposed.
      initNodesConfiguration(nodes_configuration_store);

      // By now, we're sure that this is index is in config, let's populate
      // our NodeID.
      if (!updateable_config_->getNodesConfiguration()
               ->isNodeInServiceDiscoveryConfig(*result)) {
        ld_error("Couldn't find myself (N%d) in the config, even after "
                 "self-registering. It might mean the NodesConfigurationStore "
                 "returned a stale version. This shouldn't really happen and "
                 "might indicate a bug somewhere.",
                 *result);
        return false;
      }
      my_node_id_ =
          updateable_config_->getNodesConfiguration()->getNodeID(*result);
    } else {
      ld_error("Failed to identify my node index in config, and self "
               "registration is disabled. Can't proceed, will abort.");
      return false;
    }
  }
  // Let's wait a bit for config propagation to the rest of the cluster so
  // that they recognize us when we talk to them. It's ok if they don't,
  // that's why we didn't implement complicated verfication logic in here.
  //
  // TODO(T53579322): Harden the startup of the node to avoid crashing when
  // it's still unknown to the rest of the cluster

  /* sleep override */
  std::this_thread::sleep_for(
      server_settings_->sleep_secs_after_self_registeration);

  return true;
}

ServerParameters::ServerParameters(
    std::shared_ptr<SettingsUpdater> settings_updater,
    UpdateableSettings<ServerSettings> server_settings,
    UpdateableSettings<RebuildingSettings> rebuilding_settings,
    UpdateableSettings<LocalLogStoreSettings> locallogstore_settings,
    UpdateableSettings<GossipSettings> gossip_settings,
    UpdateableSettings<Settings> processor_settings,
    UpdateableSettings<RocksDBSettings> rocksdb_settings,
    UpdateableSettings<AdminServerSettings> admin_server_settings,
    std::shared_ptr<PluginRegistry> plugin_registry,
    std::function<void()> stop_handler)
    : plugin_registry_(std::move(plugin_registry)),
      server_stats_(StatsParams().setIsServer(true)),
      settings_updater_(std::move(settings_updater)),
      server_settings_(std::move(server_settings)),
      rebuilding_settings_(std::move(rebuilding_settings)),
      locallogstore_settings_(std::move(locallogstore_settings)),
      gossip_settings_(std::move(gossip_settings)),
      processor_settings_(std::move(processor_settings)),
      rocksdb_settings_(std::move(rocksdb_settings)),
      admin_server_settings_(std::move(admin_server_settings)),
      stop_handler_(std::move(stop_handler)) {
  ld_check(stop_handler_);
}

void ServerParameters::init() {
  // Note: this won't work well if there are multiple Server instances in the
  // same process: only one of them will get its error counter bumped. Also,
  // there's a data race if the following two lines are run from multiple
  // threads.
  bool multiple_servers_in_same_process =
      errorStats != nullptr || dbg::bumpErrorCounterFn != nullptr;
  errorStats = &server_stats_;
  dbg::bumpErrorCounterFn = &bumpErrorCounter;
  if (multiple_servers_in_same_process) {
    ld_warning("Multiple Server instances coexist in the same process. Only "
               "one of them will receive error stats ('severe_errors', "
               "'critical_errors' etc).");
  }

  auto updateable_server_config = std::make_shared<UpdateableServerConfig>();
  auto updateable_logs_config = std::make_shared<UpdateableLogsConfig>();
  auto updateable_rqlite_config = std::make_shared<UpdateableRqliteConfig>();
  updateable_config_ =
      std::make_shared<UpdateableConfig>(updateable_server_config,
                                         updateable_logs_config,
                                         updateable_rqlite_config);

  server_config_hook_handles_.push_back(updateable_server_config->addHook(
      std::bind(&ServerParameters::onServerConfigUpdate,
                this,
                std::placeholders::_1)));
  nodes_configuration_hook_handles_.push_back(
      updateable_config_->updateableNodesConfiguration()->addHook(
          std::bind(&ServerParameters::shutdownIfMyNodeInfoChanged,
                    this,
                    std::placeholders::_1)));

  {
    ConfigInit config_init(
        processor_settings_->initial_config_load_timeout, getStats());
    int rv = config_init.attach(server_settings_->config_path,
                                plugin_registry_,
                                updateable_config_,
                                nullptr,
                                processor_settings_);
    if (rv != 0) {
      throw ConstructorFailed();
    }
  }

  std::shared_ptr<NodesConfigurationStore> nodes_configuration_store;

  if (processor_settings_->enable_nodes_configuration_manager) {
    nodes_configuration_store = buildNodesConfigurationStore();
    if (nodes_configuration_store == nullptr) {
      ld_error("Failed to build a NodesConfigurationStore.");
      throw ConstructorFailed();
    }
  }

  if (processor_settings_->enable_nodes_configuration_manager) {
    ld_check(nodes_configuration_store);
    if (!initNodesConfiguration(nodes_configuration_store)) {
      throw ConstructorFailed();
    }
    ld_check(updateable_config_->getNodesConfiguration() != nullptr);
  }

  // Initialize the MyNodeIDFinder that will be used to find our NodeID from
  // the config.
  if (!initMyNodeIDFinder()) {
    ld_error("Failed to construct MyNodeIDFinder");
    throw ConstructorFailed();
  }

  if (!registerAndUpdateNodeInfo(nodes_configuration_store)) {
    throw ConstructorFailed();
  }
  ld_check(my_node_id_.has_value());

  if (updateable_logs_config->get() == nullptr) {
    // Initialize logdevice with an empty LogsConfig that only contains the
    // internal logs and is marked as not fully loaded.
    auto logs_config = std::make_shared<LocalLogsConfig>();
    updateable_config_->updateableLogsConfig()->update(logs_config);
  }

  auto config = updateable_config_->get();
  // sets the InternalLogs of LocalLogsConfig.
  config->localLogsConfig()->setInternalLogsConfig(
      config->serverConfig()->getInternalLogsConfig());

  NodeID node_id = my_node_id_.value();
  ld_info("My Node ID is %s", node_id.toString().c_str());
  const auto& nodes_configuration = updateable_config_->getNodesConfiguration();
  ld_check(
      nodes_configuration->isNodeInServiceDiscoveryConfig(node_id.index()));

  ld_info(
      "My version is %lu",
      nodes_configuration->getNodeServiceDiscovery(node_id.index())->version);

  if (!setConnectionLimits()) {
    throw ConstructorFailed();
  }

  // Construct the Server Trace Logger
  std::shared_ptr<TraceLoggerFactory> trace_logger_factory =
      plugin_registry_->getSinglePlugin<TraceLoggerFactory>(
          PluginType::TRACE_LOGGER_FACTORY);
  if (!trace_logger_factory || processor_settings_->trace_logger_disabled) {
    trace_logger_ =
        std::make_shared<NoopTraceLogger>(updateable_config_, my_node_id_);
  } else {
    trace_logger_ = (*trace_logger_factory)(updateable_config_, my_node_id_);
  }

  storage_node_ = nodes_configuration->isStorageNode(my_node_id_->index());
  num_db_shards_ = storage_node_
      ? nodes_configuration->getNodeStorageAttribute(my_node_id_->index())
            ->num_shards
      : 0;

  run_sequencers_ = nodes_configuration->isSequencerNode(my_node_id_->index());
  if (run_sequencers_ &&
      server_settings_->sequencer == SequencerOptions::NONE) {
    ld_error("This node is configured as a sequencer, but -S option is "
             "not set");
    throw ConstructorFailed();
  }

  // This is a hack to update num_logs_configured across all stat objects
  // so that aggregate returns the correct value when number of log decreases
  auto num_logs = config->localLogsConfig()->size();
  auto func = [&](logdevice::Stats& stats) {
    stats.num_logs_configured = num_logs;
  };
  getStats()->runForEach(func);

  logs_config_subscriptions_.emplace_back(
      updateable_config_->updateableLogsConfig()->subscribeToUpdates([this]() {
        std::shared_ptr<configuration::LocalLogsConfig> config =
            std::dynamic_pointer_cast<configuration::LocalLogsConfig>(
                updateable_config_->getLogsConfig());
        auto num_logs = config->size();
        auto func = [&](logdevice::Stats& stats) {
          stats.num_logs_configured = num_logs;
        };
        getStats()->runForEach(func);
      }));
}

ServerParameters::~ServerParameters() {
  server_config_subscriptions_.clear();
  logs_config_subscriptions_.clear();
  server_config_hook_handles_.clear();
  dbg::bumpErrorCounterFn = nullptr;
}

bool ServerParameters::isStorageNode() const {
  return storage_node_;
}

size_t ServerParameters::getNumDBShards() const {
  return num_db_shards_;
}

std::unique_ptr<configuration::nodes::NodesConfigurationStore>
ServerParameters::buildNodesConfigurationStore() {
  return NodesConfigurationStoreFactory::create(
      *updateable_config_->get(), *getProcessorSettings().get());
}

bool ServerParameters::initNodesConfiguration(
    std::shared_ptr<configuration::nodes::NodesConfigurationStore> store) {
  // Create an empty NC in the NCS if it doesn't exist already. Most of the
  // time, this is a single read RTT (because the NC will be there), so it
  // should be fine to always do it.
  store->updateConfigSync(
      NodesConfigurationCodec::serialize(NodesConfiguration()),
      NodesConfigurationStore::Condition::createIfNotExists());
  NodesConfigurationInit config_init(std::move(store), getProcessorSettings());
  return config_init.initWithoutProcessor(
      updateable_config_->updateableNodesConfiguration());
}

bool ServerParameters::isSequencingEnabled() const {
  return run_sequencers_;
}

bool ServerParameters::isFastShutdownEnabled() const {
  return fast_shutdown_enabled_.load();
}

void ServerParameters::setFastShutdownEnabled(bool enabled) {
  fast_shutdown_enabled_.store(enabled);
}

std::shared_ptr<SettingsUpdater> ServerParameters::getSettingsUpdater() {
  return settings_updater_;
}

std::shared_ptr<UpdateableConfig> ServerParameters::getUpdateableConfig() {
  return updateable_config_;
}

std::shared_ptr<TraceLogger> ServerParameters::getTraceLogger() {
  return trace_logger_;
}

StatsHolder* ServerParameters::getStats() {
  return &server_stats_;
}

void ServerParameters::requestStop() {
  stop_handler_();
}

Server::Server(ServerParameters* params)
    : params_(params),
      server_settings_(params_->getServerSettings()),
      updateable_config_(params_->getUpdateableConfig()),
      server_config_(updateable_config_->getServerConfig()),
      settings_updater_(params_->getSettingsUpdater()),
      admin_command_processor_(std::make_unique<CommandProcessor>(this)),
      conn_budget_backlog_(server_settings_->connection_backlog),
      conn_budget_backlog_unlimited_(std::numeric_limits<uint64_t>::max()) {
  ld_check(params_);
  start_time_ = std::chrono::system_clock::now();

  if (!(initListeners() && initStore() && initLogStorageStateMap() &&
        initStorageThreadPool() && initProcessor() && initFailureDetector() &&
        startWorkers() && initNCM() && repopulateRecordCaches() &&
        initSequencers() && initSequencerPlacement() &&
        initRebuildingCoordinator() && initClusterMaintenanceStateMachine() &&
        initLogStoreMonitor() && initUnreleasedRecordDetector() &&
        initLogsConfigManager() && initAdminServer() && initThriftServers() &&
        initRocksDBMetricsExport())) {
    _exit(EXIT_FAILURE);
  }
}

template <typename T, typename... Args>
static std::unique_ptr<Listener> initListener(int port,
                                              const std::string& unix_socket,
                                              bool ssl,
                                              Args&&... args) {
  if (port > 0 || !unix_socket.empty()) {
    const auto conn_iface = unix_socket.empty()
        ? Listener::InterfaceDef(port, ssl)
        : Listener::InterfaceDef(unix_socket, ssl);

    try {
      return std::make_unique<T>(conn_iface, args...);
    } catch (const ConstructorFailed&) {
      ld_error("Failed to construct a Listener on %s",
               conn_iface.describe().c_str());
      throw;
    }
  }
  return nullptr;
}

bool Server::initListeners() {
  // create listeners (and bind to ports/socket paths specified on the command
  // line) first; exit early if ports / socket paths are taken.

  try {
    auto conn_shared_state =
        std::make_shared<ConnectionListener::SharedState>();

    connection_listener_loop_ = std::make_unique<folly::EventBaseThread>(
        true,
        nullptr,
        ConnectionListener::connectionKindToThreadName(ConnectionKind::DATA));

    connection_listener_ = initListener<ConnectionListener>(
        server_settings_->port,
        server_settings_->unix_socket,
        false,
        folly::getKeepAliveToken(connection_listener_loop_->getEventBase()),
        conn_shared_state,
        ConnectionKind::DATA,
        conn_budget_backlog_,
        server_settings_->enable_dscp_reflection);

    auto nodes_configuration = updateable_config_->getNodesConfiguration();
    ld_check(nodes_configuration);
    NodeID node_id = params_->getMyNodeID().value();
    const NodeServiceDiscovery* node_svc =
        nodes_configuration->getNodeServiceDiscovery(node_id.index());
    ld_check(node_svc);

    // Validate certificates if needed
    if (node_svc->ssl_address.has_value() ||
        params_->getProcessorSettings().get()->ssl_on_gossip_port) {
      if (!validateSSLCertificatesExist(
              params_->getProcessorSettings().get())) {
        // validateSSLCertificatesExist() should output the error
        return false;
      }
    }

    // Gets UNIX socket or port number from a SocketAddress
    auto getSocketOrPort = [](const folly::SocketAddress& addr,
                              std::string& socket_out,
                              int& port_out) {
      socket_out.clear();
      port_out = -1;
      try {
        socket_out = addr.getPath();
      } catch (std::invalid_argument& sock_exception) {
        try {
          port_out = addr.getPort();
        } catch (std::invalid_argument& port_exception) {
          return false;
        }
      }
      return true;
    };

    if (node_svc->ssl_address.has_value()) {
      std::string ssl_unix_socket;
      int ssl_port = -1;
      if (!getSocketOrPort(node_svc->ssl_address.value().getSocketAddress(),
                           ssl_unix_socket,
                           ssl_port)) {
        ld_error("SSL port/address couldn't be parsed for this node(%s)",
                 node_id.toString().c_str());
        return false;
      } else {
        ssl_connection_listener_ = initListener<ConnectionListener>(
            ssl_port,
            ssl_unix_socket,
            true,
            folly::getKeepAliveToken(connection_listener_loop_->getEventBase()),
            conn_shared_state,
            ConnectionKind::DATA_SSL,
            conn_budget_backlog_,
            server_settings_->enable_dscp_reflection);
      }
    }

    auto gossip_sock_addr = node_svc->getGossipAddress().getSocketAddress();
    auto hostStr = node_svc->default_client_data_address.toString();
    auto gossip_addr_str = node_svc->getGossipAddress().toString();
    if (gossip_addr_str != hostStr) {
      std::string gossip_unix_socket;
      int gossip_port = -1;
      bool gossip_in_config =
          getSocketOrPort(gossip_sock_addr, gossip_unix_socket, gossip_port);
      if (!gossip_in_config) {
        ld_info("No gossip address/port available for node(%s) in config"
                ", can't initialize a Gossip Listener.",
                node_id.toString().c_str());
      } else if (!params_->getGossipSettings()->enabled) {
        ld_info("Not initializing a gossip listener,"
                " since gossip-enabled is not set.");
      } else {
        ld_info("Initializing a gossip listener.");
        gossip_listener_loop_ = std::make_unique<folly::EventBaseThread>(
            true,
            nullptr,
            ConnectionListener::connectionKindToThreadName(
                ConnectionKind::GOSSIP));

        gossip_listener_ = initListener<ConnectionListener>(
            gossip_port,
            gossip_unix_socket,
            false,
            folly::getKeepAliveToken(gossip_listener_loop_->getEventBase()),
            conn_shared_state,
            ConnectionKind::GOSSIP,
            conn_budget_backlog_unlimited_,
            server_settings_->enable_dscp_reflection);
      }
    } else {
      ld_info("Gossip listener initialization not required"
              ", gossip_addr_str:%s",
              gossip_addr_str.c_str());
    }

    folly::Optional<Sockaddr> server_to_server_addr_opt =
        node_svc->server_to_server_address;
    if (server_to_server_addr_opt.has_value()) {
      std::string server_to_server_socket;
      int server_to_server_port = -1;
      if (!getSocketOrPort(server_to_server_addr_opt.value().getSocketAddress(),
                           server_to_server_socket,
                           server_to_server_port)) {
        ld_error("Server-to-server port/address couldn't be parsed for this "
                 "node(%s)",
                 node_id.toString().c_str());
        return false;
      }

      server_to_server_listener_loop_ =
          std::make_unique<folly::EventBaseThread>(
              /* autostart */ true,
              /* eventBaseManager */ nullptr,
              ConnectionListener::connectionKindToThreadName(
                  ConnectionKind::SERVER_TO_SERVER));
      server_to_server_listener_ = initListener<ConnectionListener>(
          server_to_server_port,
          server_to_server_socket,
          /* ssl */ true,
          folly::getKeepAliveToken(
              server_to_server_listener_loop_->getEventBase()),
          conn_shared_state,
          ConnectionKind::SERVER_TO_SERVER,
          conn_budget_backlog_unlimited_,
          server_settings_->enable_dscp_reflection);
    }

    for (const auto& [priority, socket_addr] :
         node_svc->addresses_per_priority) {
      std::string socket_str;
      int port = -1;
      if (!getSocketOrPort(socket_addr.getSocketAddress(), socket_str, port)) {
        ld_error("Node(%s): Cannot parse port/address for network priority %s",
                 node_id.toString().c_str(),
                 apache::thrift::util::enumNameSafe(priority).c_str());
        return false;
      }

      auto listener = initListener<ConnectionListener>(
          port,
          socket_str,
          /* ssl */ true,
          folly::getKeepAliveToken(connection_listener_loop_->getEventBase()),
          conn_shared_state,
          priorityToConnectionKind(priority),
          conn_budget_backlog_,
          server_settings_->enable_dscp_reflection);

      listeners_per_network_priority_.emplace(
          std::piecewise_construct,
          std::forward_as_tuple(priority),
          std::forward_as_tuple(std::move(listener)));
    };
  } catch (const ConstructorFailed&) {
    // failed to initialize listeners
    return false;
  }

  return true;
}

bool Server::initThriftServers() {
  const auto server_settings = params_->getServerSettings();
  auto nodes_configuration = updateable_config_->getNodesConfiguration();
  ld_check(nodes_configuration);
  NodeID node_id = params_->getMyNodeID().value();
  const NodeServiceDiscovery* node_svc =
      nodes_configuration->getNodeServiceDiscovery(node_id.index());
  ld_check(node_svc);

  s2s_thrift_api_handle_ =
      initThriftServer("s2s-api", node_svc->server_thrift_api_address);

  c2s_thrift_api_handle_ =
      initThriftServer("c2s-api", node_svc->client_thrift_api_address);
  return true;
}

std::unique_ptr<LogDeviceThriftServer>
Server::initThriftServer(std::string name,
                         const folly::Optional<Sockaddr>& address) {
  if (!address) {
    ld_info("%s Thrift API server disabled", name.c_str());
    return nullptr;
  }

  auto handler =
      std::make_shared<LogDeviceAPIThriftHandler>(name,
                                                  processor_.get(),
                                                  params_->getSettingsUpdater(),
                                                  params_->getServerSettings(),
                                                  params_->getStats());

  auto factory_plugin =
      params_->getPluginRegistry()->getSinglePlugin<ThriftServerFactory>(
          PluginType::THRIFT_SERVER_FACTORY);
  ld_info("Initializing Thrift Server: %s", name.c_str());
  if (factory_plugin) {
    return (*factory_plugin)(
        name, *address, std::move(handler), processor_->getRequestExecutor());
  } else {
    // Fallback to built-in SimpleThriftApiServer
    return std::make_unique<SimpleThriftServer>(
        name, *address, std::move(handler), processor_->getRequestExecutor());
  }
}

bool Server::initStore() {
  const std::string local_log_store_path =
      params_->getLocalLogStoreSettings()->local_log_store_path;
  if (params_->isStorageNode()) {
    if (local_log_store_path.empty()) {
      ld_critical("This node is identified as a storage node in config (it has "
                  "a 'weight' attribute), but --local-log-store-path is not "
                  "set ");
      return false;
    }
    auto rocksdb_plugin =
        params_->getPluginRegistry()->getSinglePlugin<RocksDBCustomiserFactory>(
            PluginType::ROCKSDB_CUSTOMISER_FACTORY);

    node_index_t node_index = params_->getMyNodeID()->index();
    uint64_t node_version = updateable_config_->getNodesConfiguration()
                                ->getNodeServiceDiscovery(node_index)
                                ->version;

    // If there's no plugin, use the default customiser.
    std::unique_ptr<RocksDBCustomiser> rocksdb_customiser =
        rocksdb_plugin == nullptr
        ? std::make_unique<RocksDBCustomiser>()
        : (*rocksdb_plugin)(
              local_log_store_path,
              updateable_config_->getServerConfig()->getClusterName(),
              node_index,
              node_version,
              params_->getNumDBShards(),
              params_->getRocksDBSettings());
    ld_check(rocksdb_customiser);

    try {
      auto local_settings = params_->getProcessorSettings().get();
      sharded_store_ = std::make_unique<ShardedRocksDBLocalLogStore>(
          local_log_store_path,
          params_->getNumDBShards(),
          params_->getRocksDBSettings(),
          std::move(rocksdb_customiser),
          params_->getStats());

      sharded_store_->init(*local_settings,
                           params_->getRebuildingSettings(),
                           updateable_config_,
                           &g_rocksdb_caches);
      if (!server_settings_->ignore_cluster_marker &&
          !ClusterMarkerChecker::check(*sharded_store_,
                                       *server_config_,
                                       params_->getMyNodeID().value())) {
        ld_critical("Could not initialize log store cluster marker mismatch!");
        return false;
      }
      auto& io_fault_injection = IOFaultInjection::instance();
      io_fault_injection.init(sharded_store_->numShards());
    } catch (const ConstructorFailed& ex) {
      ld_critical("Failed to initialize local log store");
      return false;
    }
  }

  return true;
}

bool Server::initStorageThreadPool() {
  if (!params_->isStorageNode()) {
    return true;
  }
  auto local_settings = params_->getProcessorSettings().get();
  // Size the storage thread pool task queue to never fill up.
  size_t task_queue_size =
      local_settings->num_workers * local_settings->max_inflight_storage_tasks;
  sharded_storage_thread_pool_.reset(
      new ShardedStorageThreadPool(sharded_store_.get(),
                                   server_settings_->storage_pool_params,
                                   server_settings_,
                                   params_->getProcessorSettings(),
                                   task_queue_size,
                                   params_->getStats(),
                                   params_->getTraceLogger()));
  return true;
}

bool Server::initProcessor() {
  ld_check(!params_->isStorageNode() || log_storage_state_map_);
  try {
    processor_ = ServerProcessor::createWithoutStarting(
        sharded_storage_thread_pool_.get(),
        std::move(log_storage_state_map_),
        params_->getServerSettings(),
        params_->getGossipSettings(),
        params_->getAdminServerSettings(),
        updateable_config_,
        params_->getTraceLogger(),
        params_->getProcessorSettings(),
        params_->getStats(),
        params_->getPluginRegistry(),
        "",
        "",
        "ld:srv", // prefix of worker thread names
        params_->getMyNodeID());

    if (sharded_storage_thread_pool_) {
      sharded_storage_thread_pool_->setProcessor(processor_.get());

      // Give sharded_store_ a pointer to the thread pool, after
      // the thread pool has a pointer to Processor.
      sharded_store_->setShardedStorageThreadPool(
          sharded_storage_thread_pool_.get());
    }

    processor_->setServerInstanceId(
        SystemTimestamp::now().toMilliseconds().count());
  } catch (const ConstructorFailed&) {
    ld_error("Failed to construct a Processor: error %d (%s)",
             static_cast<int>(err),
             error_description(err));
    return false;
  }
  return true;
}

bool Server::initLogStorageStateMap() {
  if (!params_->isStorageNode()) {
    return true;
  }

  shard_size_t nshards = sharded_store_->numShards();
  log_storage_state_map_ = std::make_unique<LogStorageStateMap>(
      nshards,
      params_->getStats(),
      params_->getProcessorSettings()->enable_record_cache,
      params_->getProcessorSettings()->log_state_recovery_interval);

  /*
   * It is important to differentiate between the following cases
   * while loading the `LogStorageState`.
   *
   * 1. The metadata is not present.
   * 2. The metadata is present but could not be read due to some error.
   *
   * E.g. `TRIM_POINT` would be set to `LSN_INVALID` in both such
   * cases.
   *
   * Case 1 is simple. However, case 2 needs some special handling as
   * below.
   *
   * - If the `log_id` of the problematic metadata is known, the
   *   `LogStorageState` could be marked in error with
   *   `notePermanentError`. Any consumers of `LogStorageState` would
   *   then check for the permanent error before trusting its
   *   content.
   *   This case could arise if the metadata `key` is intact but its
   *   `value` is malformed.
   *
   * - If the `log_id` of the metadata could not be read, then the
   *   contents for such UNKNOWN log in that shard cannot be served
   *   correctly. Therefore, any future IO to that shard is disabled
   *   by switching its storage backend to `FailingLocalLogStore`. It
   *   is safe to switch the backend at this stage in the startup as
   *   no one else has access to it.  If switching the backend fails
   *   for some reason, the server startup is aborted.
   */

  auto make_traverser =
      [&lsmap = log_storage_state_map_](
          shard_index_t shard,
          std::function<void(LogStorageState*, std::unique_ptr<LogMetadata>)>
              fn) {
        return [shard, fn, &lsmap](logid_t log_id,
                                   std::unique_ptr<LogMetadata> meta,
                                   Status status) {
          auto log_state = lsmap->insertOrGet(log_id, shard);
          if (log_state->hasPermanentError()) {
            return;
          }
          if (status == E::OK) {
            fn(log_state, std::move(meta));
          } else {
            ld_check_eq(status, E::MALFORMED_RECORD);
            log_state->notePermanentError("Populating LogStorageState");
          }
        };
      };

  std::vector<std::future<bool>> futures;
  for (shard_index_t shard = 0; shard < nshards; ++shard) {
    futures.push_back(std::async(
        std::launch::async,
        [shard, &make_traverser, &sharded_store = sharded_store_]() {
          ThreadID::set(ThreadID::UTILITY,
                        folly::sformat("ld:populateLogState{}", shard));
          auto store = sharded_store->getByIndex(shard);
          auto trim_point_traverser = make_traverser(
              shard,
              [](LogStorageState* log_state,
                 std::unique_ptr<LogMetadata> meta) {
                log_state->updateTrimPoint(
                    dynamic_cast<TrimMetadata*>(meta.get())->trim_point_);
              });
          auto lce_traverser = make_traverser(
              shard,
              [](LogStorageState* log_state,
                 std::unique_ptr<LogMetadata> meta) {
                log_state->updateLastCleanEpoch(
                    dynamic_cast<LastCleanMetadata*>(meta.get())->epoch_);
              });
          auto last_released_traverser = make_traverser(
              shard,
              [](LogStorageState* log_state,
                 std::unique_ptr<LogMetadata> meta) {
                log_state->updateLastReleasedLSN(
                    dynamic_cast<LastReleasedMetadata*>(meta.get())
                        ->last_released_lsn_,
                    LogStorageState::LastReleasedSource::LOCAL_LOG_STORE);
              });
          int rv = store->traverseLogsMetadata(
              LogMetadataType::TRIM_POINT, trim_point_traverser);
          if (rv != 0) {
            ld_error("Failed to populate Trim Points from shard %d: %s.",
                     shard,
                     error_name(err));
            goto out;
          }

          rv = store->traverseLogsMetadata(
              LogMetadataType::LAST_CLEAN, lce_traverser);
          if (rv != 0) {
            ld_error("Failed to populate Last Clean Epochs from shard %d: %s",
                     shard,
                     error_name(err));
            goto out;
          }

          rv = store->traverseLogsMetadata(
              LogMetadataType::LAST_RELEASED, last_released_traverser);
          if (rv != 0) {
            ld_error("Failed to populate Last Released LSN from shard %d: %s",
                     shard,
                     error_name(err));
          }

        out:
          if (rv != 0 && !sharded_store->switchToFailingLocalLogStore(shard)) {
            ld_critical("Failed to disable shard %d.", shard);
            return false;
          }
          return true;
        }));
  }

  bool ret = true;
  for (auto& f : futures) {
    if (!f.get()) {
      ret = false;
    }
  }
  ld_info(
      "Populating log storage state map %s.", ret ? "successful" : "failed");
  return ret;
}

bool Server::initFailureDetector() {
  if (params_->getGossipSettings()->enabled) {
    try {
      processor_->failure_detector_ = std::make_unique<FailureDetector>(
          params_->getGossipSettings(), processor_.get(), params_->getStats());
      if (processor_->getHealthMonitor()) {
        processor_->getHealthMonitor()->setFailureDetector(
            processor_->failure_detector_.get());
      }
    } catch (const ConstructorFailed&) {
      ld_error(
          "Failed to construct FailureDetector: %s", error_description(err));
      return false;
    }
  } else {
    ld_info("Not initializing gossip based failure detector,"
            " since --gossip-enabled is not set");
  }

  return true;
}

bool Server::startWorkers() {
  processor_->startRunning();
  return true;
}

bool Server::initNCM() {
  if (params_->getProcessorSettings()->enable_nodes_configuration_manager) {
    // create and initialize NodesConfigurationManager (NCM) and attach it to
    // the Processor

    auto my_node_id = params_->getMyNodeID().value();
    auto node_svc_discovery =
        updateable_config_->getNodesConfiguration()->getNodeServiceDiscovery(
            my_node_id);
    if (node_svc_discovery == nullptr) {
      ld_critical(
          "NodeID '%s' doesn't exist in the NodesConfiguration of %s",
          my_node_id.toString().c_str(),
          updateable_config_->getServerConfig()->getClusterName().c_str());
      return false;
    }
    auto roleset = node_svc_discovery->getRoles();

    // TODO: get NCS from NodesConfigurationInit instead
    auto ncm = configuration::nodes::NodesConfigurationManagerFactory::create(
        processor_.get(), nullptr, roleset);
    if (ncm == nullptr) {
      ld_critical("Unable to create NodesConfigurationManager during server "
                  "creation!");
      return false;
    }
    ncm->upgradeToProposer();

    auto initial_nc = processor_->config_->getNodesConfiguration();
    if (!initial_nc) {
      // Currently this should only happen in tests as our boostrapping
      // workflow should always ensure the Processor has a valid
      // NodesConfiguration before initializing NCM. In the future we will
      // require a valid NC for Processor construction and will turn this into
      // a ld_check.
      ld_warning("NodesConfigurationManager initialized without a valid "
                 "NodesConfiguration in its Processor context. This should "
                 "only happen in tests.");
      initial_nc = std::make_shared<const NodesConfiguration>();
    }
    if (!ncm->init(std::move(initial_nc))) {
      ld_critical(
          "Processing initial NodesConfiguration did not finish in time.");
      return false;
    }
  }
  return true;
}

bool Server::repopulateRecordCaches() {
  if (!params_->isStorageNode()) {
    ld_info("Not repopulating record caches");
    return true;
  }

  // Callback function for each status
  std::map<Status, std::vector<int>> status_counts;
  auto callback = [&status_counts](Status status, shard_index_t shard_idx) {
    status_counts[status].push_back(shard_idx);
  };

  // Start RecordCacheRepopulationRequest. Only try to deserialize record
  // cache snapshot if record cache is enabled _and_ persisting record cache
  // is allowed. Otherwise, just drop all previous snapshots.
  std::unique_ptr<Request> req =
      std::make_unique<RepopulateRecordCachesRequest>(
          callback, params_->getProcessorSettings()->enable_record_cache);
  if (processor_->blockingRequest(req) != 0) {
    ld_critical("Failed to make a blocking request to repopulate record "
                "caches!");
    return false;
  }

  int num_failed_deletions = status_counts[E::FAILED].size();
  int num_failed_repopulations = status_counts[E::PARTIAL].size();
  int num_disabled_shards = status_counts[E::DISABLED].size();
  // Sanity check that no other status is used
  ld_check(num_failed_deletions + num_failed_repopulations +
               num_disabled_shards + status_counts[E::OK].size() ==
           params_->getNumDBShards());

  auto getAffectedShards = [&](Status status) {
    std::string result;
    folly::join(", ", status_counts[status], result);
    return result;
  };

  if (num_failed_deletions) {
    ld_critical("Failed to delete snapshots on the following enabled shards: "
                "[%s]",
                getAffectedShards(E::FAILED).c_str());
    return false;
  }
  if (num_failed_repopulations) {
    ld_error("Failed to repopulate all snapshot(s) due to data corruption or "
             "size limit, leaving caches empty or paritally populated. "
             "Affected shards: [%s]",
             getAffectedShards(E::PARTIAL).c_str());
  }
  if (num_disabled_shards) {
    ld_info("Did not repopulate record caches from disabled shards: [%s]",
            getAffectedShards(E::DISABLED).c_str());
  }
  return true;
}

bool Server::initSequencers() {
  // Create an instance of EpochStore.
  std::unique_ptr<EpochStore> epoch_store;

  if (!server_settings_->epoch_store_path.empty()) {
    try {
      ld_info("Initializing FileEpochStore");
      epoch_store = std::make_unique<FileEpochStore>(
          server_settings_->epoch_store_path,
          processor_->getRequestExecutor(),
          processor_->getOptionalMyNodeID(),
          updateable_config_->updateableNodesConfiguration());
    } catch (const ConstructorFailed&) {
      ld_error(
          "Failed to construct FileEpochStore: %s", error_description(err));
      return false;
    }
  } else {
    ld_info("Initializing RqliteEpochStore");
    try {
      epoch_store = std::make_unique<RqliteEpochStore>(
          server_config_->getClusterName(),
          processor_->getRequestExecutor(),
          std::make_shared<RqliteClient>(
              updateable_config_->updateableRqliteConfig()
                  ->get()
                  ->getRqliteUri()),
          processor_->getOptionalMyNodeID(),
          updateable_config_->updateableNodesConfiguration());
    } catch (const ConstructorFailed&) {
      ld_error(
          "Failed to construct RqliteEpochStore: %s", error_description(err));
      return false;
    }
  }

  processor_->allSequencers().setEpochStore(std::move(epoch_store));

  return true;
}

bool Server::initLogStoreMonitor() {
  if (params_->isStorageNode()) {
    logstore_monitor_ =
        std::make_unique<LogStoreMonitor>(processor_.get(),
                                          rebuilding_supervisor_.get(),
                                          params_->getLocalLogStoreSettings());
    logstore_monitor_->start();
  }

  return true;
}

bool Server::initRocksDBMetricsExport() {
  if (sharded_store_) {
    auto registry = params_->getPluginRegistry();
    auto metrics_export = registry->getSinglePlugin<RocksDBMetricsExport>(
        PluginType::ROCKSDB_METRICS_EXPORT);
    if (metrics_export) {
      (*metrics_export)(sharded_store_.get(), processor_.get());
    }
  }

  return true;
}

bool Server::initSequencerPlacement() {
  // SequencerPlacement has a pointer to Processor and will notify it of
  // placement updates
  if (params_->isSequencingEnabled()) {
    try {
      std::shared_ptr<SequencerPlacement> placement_ptr;

      switch (server_settings_->sequencer) {
        case SequencerOptions::ALL:
          ld_info("using SequencerOptions::ALL");
          placement_ptr =
              std::make_shared<StaticSequencerPlacement>(processor_.get());
          break;

        case SequencerOptions::LAZY:
          ld_info("using SequencerOptions::LAZY");
          placement_ptr = std::make_shared<LazySequencerPlacement>(
              processor_.get(), params_->getGossipSettings());
          break;

        case SequencerOptions::NONE:
          ld_check(false);
          break;
      }

      sequencer_placement_.update(std::move(placement_ptr));
    } catch (const ConstructorFailed& ex) {
      ld_error("Failed to initialize SequencerPlacement object");
      return false;
    }
  }

  return true;
}

bool Server::initRebuildingCoordinator() {
  std::shared_ptr<Configuration> config = processor_->config_->get();

  bool enable_rebuilding = false;
  if (params_->getRebuildingSettings()->disable_rebuilding) {
    ld_info("Rebuilding is disabled.");
  } else if (!config->logsConfig()->logExists(
                 configuration::InternalLogs::EVENT_LOG_DELTAS)) {
    ld_error("No event log is configured but rebuilding is enabled. Configure "
             "an event log by populating the \"internal_logs\" section of the "
             "server config and restart this server");
  } else {
    ld_info("Initializing EventLog RSM and RebuildingCoordinator");
    enable_rebuilding = true;
    std::unique_ptr<RSMSnapshotStore> snapshot_store =
        RsmServerSnapshotStoreFactory::create(
            processor_.get(),
            params_->getProcessorSettings()->rsm_snapshot_store_type,
            params_->isStorageNode(),
            std::to_string(configuration::InternalLogs::EVENT_LOG_DELTAS.val_));
    auto workerType = EventLogStateMachine::workerType(processor_.get());
    auto workerId = worker_id_t(EventLogStateMachine::getWorkerIdx(
        processor_->getWorkerCount(workerType)));
    event_log_ =
        std::make_unique<EventLogStateMachine>(params_->getProcessorSettings(),
                                               std::move(snapshot_store),
                                               workerId,
                                               workerType);
    event_log_->enableSendingUpdatesToWorkers();
    event_log_->setMyNodeID(params_->getMyNodeID().value());
    enable_rebuilding = true;
  }

  if (sharded_store_) {
    if (!enable_rebuilding) {
      // We are not enabling rebuilding. Notify Processor that all
      // shards are authoritative.
      for (uint32_t shard = 0; shard < sharded_store_->numShards(); ++shard) {
        getProcessor()->markShardAsNotMissingData(shard);
        getProcessor()->markShardClean(shard);
      }
    } else {
      ld_check(event_log_);

      rebuilding_supervisor_ = std::make_unique<RebuildingSupervisor>(
          event_log_.get(),
          processor_.get(),
          params_->getRebuildingSettings(),
          params_->getAdminServerSettings());
      ld_info("Starting RebuildingSupervisor");
      rebuilding_supervisor_->start();

      rebuilding_coordinator_ = std::make_unique<RebuildingCoordinator>(
          processor_->config_,
          event_log_.get(),
          processor_.get(),
          rebuilding_supervisor_.get(),
          params_->getRebuildingSettings(),
          params_->getAdminServerSettings(),
          sharded_store_.get(),
          std::make_unique<maintenance::MaintenanceManagerTracer>(
              params_->getTraceLogger()));
      ld_info("Starting RebuildingCoordinator");
      if (rebuilding_coordinator_->start() != 0) {
        return false;
      }
    }
  }

  if (event_log_) {
    std::unique_ptr<Request> req =
        std::make_unique<StartEventLogStateMachineRequest>(event_log_.get());

    const int rv = processor_->postRequest(req);
    if (rv != 0) {
      ld_error("Cannot post request to start event log state machine: %s (%s)",
               error_name(err),
               error_description(err));
      ld_check(false);
      return false;
    }
  }

  return true;
}

bool Server::createAndAttachMaintenanceManager(AdminAPIHandler* handler) {
  // MaintenanceManager can generally be run on any server. However
  // MaintenanceManager lacks the leader election logic and hence
  // we cannot have multiple MaintenanceManager-s running for the
  // same cluster. To avoid this, we do want MaintenanceManager run
  // on regular logdevice servers except for purpose of testing, where
  // the node that should run a instance of MaintenanceManager can be
  // directly controlled.
  const auto admin_settings = params_->getAdminServerSettings();
  if (admin_settings->enable_maintenance_manager) {
    ld_check(cluster_maintenance_state_machine_);
    ld_check(event_log_);
    ld_check(handler);
    auto deps = std::make_unique<maintenance::MaintenanceManagerDependencies>(
        processor_.get(),
        admin_settings,
        params_->getRebuildingSettings(),
        cluster_maintenance_state_machine_.get(),
        event_log_.get(),
        std::make_unique<maintenance::SafetyCheckScheduler>(
            processor_.get(), admin_settings, handler->getSafetyChecker()),
        std::make_unique<maintenance::MaintenanceLogWriter>(processor_.get()),
        std::make_unique<maintenance::MaintenanceManagerTracer>(
            params_->getTraceLogger()));
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
    ld_info("Not initializing MaintenanceManager since it is disabled in "
            "settings");
  }
  return true;
}

bool Server::initClusterMaintenanceStateMachine() {
  if (params_->getAdminServerSettings()
          ->enable_cluster_maintenance_state_machine ||
      params_->getAdminServerSettings()->enable_maintenance_manager) {
    cluster_maintenance_state_machine_ =
        std::make_unique<maintenance::ClusterMaintenanceStateMachine>(
            params_->getAdminServerSettings(), nullptr /* snapshot store */);

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
      ld_check(false);
      return false;
    }
  }
  return true;
}

bool Server::initUnreleasedRecordDetector() {
  if (params_->isStorageNode()) {
    unreleased_record_detector_ = std::make_shared<UnreleasedRecordDetector>(
        processor_.get(), params_->getProcessorSettings());
    unreleased_record_detector_->start();
  }

  return true;
}

bool Server::startConnectionListener(std::unique_ptr<Listener>& handle) {
  ConnectionListener* listener =
      checked_downcast<ConnectionListener*>(handle.get());
  listener->setProcessor(processor_.get());
  // Assign callback function to listener
  if (processor_->getHealthMonitor()) {
    listener->setConnectionLimitReachedCallback(
        [&hm = *(processor_->getHealthMonitor())]() {
          hm.reportConnectionLimitReached();
        });
  }
  return listener->startAcceptingConnections().wait().value();
}

bool Server::initLogsConfigManager() {
  std::unique_ptr<RSMSnapshotStore> snapshot_store =
      RsmServerSnapshotStoreFactory::create(
          processor_.get(),
          params_->getProcessorSettings().get()->rsm_snapshot_store_type,
          params_->isStorageNode(),
          std::to_string(configuration::InternalLogs::CONFIG_LOG_DELTAS.val_));
  return LogsConfigManager::createAndAttach(
      *processor_, std::move(snapshot_store), true /* is_writable */);
}

bool Server::initAdminServer() {
  if (params_->getServerSettings()->admin_enabled) {
    // Figure out the socket address for the admin server.
    auto server_config = updateable_config_->getServerConfig();
    ld_check(server_config);

    auto my_node_id = params_->getMyNodeID().value();
    auto svd =
        updateable_config_->getNodesConfiguration()->getNodeServiceDiscovery(
            my_node_id.index());
    ld_check(svd);

    auto admin_listen_addr = svd->admin_address;
    if (!admin_listen_addr.has_value()) {
      const auto& admin_settings = params_->getAdminServerSettings();
      if (!admin_settings->admin_unix_socket.empty()) {
        admin_listen_addr = Sockaddr(admin_settings->admin_unix_socket);
      } else {
        admin_listen_addr = Sockaddr("::", admin_settings->admin_port);
      }
      ld_warning(
          "The admin-enabled setting is true, but "
          "admin_address/admin_port are missing from the config. Will use "
          "default address (%s) instead. Please consider setting a port in "
          "the "
          "config",
          admin_listen_addr->toString().c_str());
    }

    std::string name = "LogDevice Admin API Service";
    auto handler =
        std::make_shared<AdminAPIHandler>(name,
                                          processor_.get(),
                                          params_->getSettingsUpdater(),
                                          params_->getServerSettings(),
                                          params_->getAdminServerSettings(),
                                          params_->getStats());

    auto factory_plugin =
        params_->getPluginRegistry()->getSinglePlugin<ThriftServerFactory>(
            PluginType::THRIFT_SERVER_FACTORY);

    auto address = admin_listen_addr.value();
    if (factory_plugin) {
      admin_server_handle_ = (*factory_plugin)(
          name, address, handler, processor_->getRequestExecutor());
    } else {
      // Fallback to built-in SimpleThriftApiServer
      admin_server_handle_ = std::make_unique<SimpleThriftServer>(
          name, address, handler, processor_->getRequestExecutor());
    }

    if (sharded_store_) {
      handler->setShardedRocksDBStore(sharded_store_.get());
    }
    createAndAttachMaintenanceManager(handler.get());
    handler->setAdminCommandHandler(
        std::bind(&CommandProcessor::asyncProcessCommand,
                  admin_command_processor_.get(),
                  std::placeholders::_1,
                  std::placeholders::_2));
  } else {
    ld_info("Not initializing Admin API,"
            " since admin-enabled server setting is set to false");
  }
  return true;
}

bool Server::startListening() {
  // start accepting new connections
  if (!startConnectionListener(connection_listener_)) {
    return false;
  }

  if (gossip_listener_loop_ && !startConnectionListener(gossip_listener_)) {
    return false;
  }

  // Now that gossip listener is running, let's start gossiping.
  if (processor_->failure_detector_ != nullptr) {
    processor_->failure_detector_->start();
  }

  if (ssl_connection_listener_ &&
      !startConnectionListener(ssl_connection_listener_)) {
    return false;
  }

  if (s2s_thrift_api_handle_ && !s2s_thrift_api_handle_->start()) {
    return false;
  }

  if (c2s_thrift_api_handle_ && !c2s_thrift_api_handle_->start()) {
    return false;
  }

  if (admin_server_handle_ && !admin_server_handle_->start()) {
    return false;
  }

  if (server_to_server_listener_loop_ &&
      !startConnectionListener(server_to_server_listener_)) {
    return false;
  }

  for (auto& [_, listener] : listeners_per_network_priority_) {
    if (!startConnectionListener(listener)) {
      return false;
    }
  }

  return true;
}

void Server::requestStop() {
  params_->requestStop();
}

void Server::gracefulShutdown() {
  if (is_shut_down_.exchange(true)) {
    return;
  }

  uint64_t shutdown_duration_ms = 0;
  shutdown_server(admin_server_handle_,
                  s2s_thrift_api_handle_,
                  c2s_thrift_api_handle_,
                  connection_listener_,
                  listeners_per_network_priority_,
                  gossip_listener_,
                  ssl_connection_listener_,
                  server_to_server_listener_,
                  connection_listener_loop_,
                  gossip_listener_loop_,
                  server_to_server_listener_loop_,
                  logstore_monitor_,
                  processor_,
                  sharded_storage_thread_pool_,
                  sharded_store_,
                  sequencer_placement_.get(),
                  rebuilding_coordinator_,
                  event_log_,
                  rebuilding_supervisor_,
                  unreleased_record_detector_,
                  cluster_maintenance_state_machine_,
                  params_->isFastShutdownEnabled(),
                  shutdown_duration_ms);
  ld_info("Shutdown took %ld ms", shutdown_duration_ms);
  STAT_ADD(params_->getStats(), shutdown_time_ms, shutdown_duration_ms);
}

void Server::shutdownWithTimeout() {
  bool done = false;
  std::condition_variable cv;
  std::mutex mutex;

  // perform all work in a separate thread so that we can specify a timeout
  std::thread thread([&]() {
    ThreadID::set(ThreadID::Type::UTILITY, "ld:shtdwn-timer");
    std::unique_lock<std::mutex> lock(mutex);
    if (!done && !cv.wait_for(lock, server_settings_->shutdown_timeout, [&]() {
          return done;
        })) {
      ld_warning("Timeout expired while waiting for shutdown to complete");
      fflush(stdout);
      // Make sure to dump a core to make it easier to investigate.
      std::abort();
    }
  });

  {
    gracefulShutdown();
    {
      std::lock_guard<std::mutex> lock(mutex);
      done = true;
    }
    cv.notify_one();
  }

  thread.join();
}

Processor* Server::getProcessor() const {
  return processor_.get();
}

RebuildingCoordinator* Server::getRebuildingCoordinator() {
  return rebuilding_coordinator_.get();
}

EventLogStateMachine* Server::getEventLogStateMachine() {
  return event_log_.get();
}

maintenance::MaintenanceManager* Server::getMaintenanceManager() {
  return maintenance_manager_.get();
}

Server::~Server() {
  shutdownWithTimeout();
}
}} // namespace facebook::logdevice
