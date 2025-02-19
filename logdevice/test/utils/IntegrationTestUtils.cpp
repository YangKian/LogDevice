/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/test/utils/IntegrationTestUtils.h"

#include <cstdio>
#include <ifaddrs.h>
#include <signal.h>
#include <unistd.h>

#include <folly/FileUtil.h>
#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <folly/Subprocess.h>
#include <folly/dynamic.h>
#include <folly/io/async/AsyncSocket.h>
#include <folly/io/async/EventBaseManager.h>
#include <folly/json.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thrift/lib/cpp2/async/HeaderClientChannel.h>

#include "common/fb303/if/gen-cpp2/FacebookServiceAsyncClient.h"
#include "logdevice/admin/AdminAPIUtils.h"
#include "logdevice/admin/if/gen-cpp2/AdminAPI.h"
#include "logdevice/admin/maintenance/MaintenanceLogWriter.h"
#include "logdevice/admin/toString.h"
#include "logdevice/common/CheckSealRequest.h"
#include "logdevice/common/EpochMetaDataUpdater.h"
#include "logdevice/common/FileConfigSource.h"
#include "logdevice/common/FileConfigSourceThread.h"
#include "logdevice/common/FlowGroup.h"
#include "logdevice/common/HashBasedSequencerLocator.h"
#include "logdevice/common/LegacyLogToShard.h"
#include "logdevice/common/NodeHealthStatus.h"
#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/StaticSequencerLocator.h"
#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/TextConfigUpdater.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/event_log/EventLogRebuildingSet.h"
#include "logdevice/common/nodeset_selection/NodeSetSelectorFactory.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/common/plugin/SequencerLocatorFactory.h"
#include "logdevice/common/test/InlineRequestPoster.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/include/Client.h"
#include "logdevice/include/ClientSettings.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientPluginHelper.h"
#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/lib/ops/EventLogUtils.h"
#include "logdevice/server/epoch_store/FileEpochStore.h"
#include "logdevice/server/locallogstore/LocalLogStore.h"
#include "logdevice/server/locallogstore/RocksDBLogStoreBase.h"
#include "logdevice/server/locallogstore/ShardedRocksDBLocalLogStore.h"
#include "logdevice/server/locallogstore/test/StoreUtil.h"
#include "logdevice/test/utils/AdminAPITestUtils.h"
#include "logdevice/test/utils/ServerInfo.h"
#include "logdevice/test/utils/port_selection.h"
#include "logdevice/test/utils/util.h"

using facebook::logdevice::configuration::LocalLogsConfig;
#ifdef FB_BUILD_PATHS
#include "common/files/FbcodePaths.h"
#endif

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

#ifdef FB_BUILD_PATHS
std::string defaultLogdevicedPath() {
  return "logdevice/server/logdeviced_nofb";
}
std::string defaultAdminServerPath() {
  return "logdevice/ops/admin_server/ld-admin-server-nofb";
}
std::string defaultMarkdownLDQueryPath() {
  return "logdevice/ops/ldquery/markdown-ldquery";
}
static const char* CHECKER_PATH =
    "logdevice/replication_checker/replication_checker_nofb";
#else
std::string defaultLogdevicedPath() {
  return "bin/logdeviced";
}
std::string defaultAdminServerPath() {
  return "bin/ld-admin-server";
}
std::string defaultMarkdownLDQueryPath() {
  return "bin/markdown-ldquery";
}
static const char* CHECKER_PATH = "bin/ld-replication-checker";
#endif

static std::string_view LOC_PREFIX = "rg1.dc1.cl1.rw1.rk";

namespace fs = boost::filesystem;

// Checks LOGDEVICE_TEST_PAUSE_FOR_GDB environment variable and pauses if
// requested
static void maybe_pause_for_gdb(Cluster&,
                                const std::vector<node_index_t>& indices);

namespace {
// Helper classes and functions used to parse the output of admin commands
template <typename T>
struct format_traits;
template <>
struct format_traits<int64_t> {
  typedef int64_t value_type;
  static constexpr const char* fmt = "%ld";
};
template <>
struct format_traits<std::string> {
  typedef char value_type[101];
  static constexpr const char* fmt = "%100s";
};
template <typename T>
std::map<std::string, T> parse(std::string output, std::string prefix) {
  std::map<std::string, T> out;
  std::vector<std::string> lines;
  folly::split("\r\n", output, lines, /* ignoreEmpty */ true);
  for (const std::string& line : lines) {
    typename format_traits<T>::value_type value;
    std::string format = prefix + " %100s " + format_traits<T>::fmt;
    char name[101];
    if (sscanf(line.c_str(), format.c_str(), name, &value) == 2) {
      out[name] = value;
    }
  }
  return out;
}

/*
 * Parses a line like:
 * "GOSSIP N6 ALIVE (gossip: 2, instance-id: 1466138232075, failover: 0,
 *   state: ALIVE) BOYCOTTED"
 * "GOSSIP N7 DEAD (gossip: 2, instance-id: 1466138232075, failover: 0,
 *    state: SUSPECT) -"
 * ...
 * and returns map entries like [N6]["ALIVE"], [N7]["SUSPECT"], ...
 */
std::map<std::string, std::string> parseGossipState(std::string output) {
  std::map<std::string, std::string> out;
  std::vector<std::string> lines;
  folly::split("\r\n", output, lines, /* ignoreEmpty */ true);
  for (const std::string& line : lines) {
    auto to = line.find_last_of(")");
    auto from = line.rfind(" ", to) + 1;
    std::string value = line.substr(from, to - from);

    std::string format = "GOSSIP %100s ";
    std::array<char, 101> name{};
    if (sscanf(line.c_str(), format.c_str(), name.begin()) == 1) {
      out[name.cbegin()] = value;
    }
  }

  return out;
}

/*
 * Returns [status, lsn] pair
 */
std::pair<std::string, std::string> parseTrimmableVersion(std::string output) {
  std::string status_out{"E::FAILED"};
  std::string lsn_str_out{"LSN_INVALID"};
  std::vector<std::string> lines;
  folly::split("\r\n", output, lines, /* ignoreEmpty */ true);

  std::array<char, 31> st = {{0}};
  std::array<char, 21> ver = {{0}};
  if (!lines.size()) {
    return std::make_pair(status_out, lsn_str_out);
  }
  auto line = lines[0];

  int ret =
      sscanf(line.c_str(), "st:%30s trimmable_ver:%20s", st.data(), ver.data());
  if (ret == 2) {
    status_out = st.data();
    lsn_str_out = ver.data();
  }
  return std::make_pair(status_out, lsn_str_out);
}

/**
 * Parses a string like:
 *
 * "GOSSIP N6 ALIVE (gossip: 2, instance-id: 1466138232075, failover: 0,
 *   state: ALIVE) BOYCOTTED"
 * "GOSSIP N7 DEAD (gossip: 2, instance-id: 1466138232075, failover: 0,
 *    state: SUSPECT) -"
 * ...
 * and returns map from node name to a pair of status and gossip: count.
 **/
std::map<std::string, std::pair<std::string, uint64_t>>
parseGossipCount(std::string output) {
  std::map<std::string, std::pair<std::string, uint64_t>> out;
  std::vector<std::string> lines;
  folly::split("\r\n", output, lines, /* ignoreEmpty */ true);

  for (const std::string& line : lines) {
    std::array<char, 101> name = {{0}};
    std::array<char, 101> status = {{0}};
    uint64_t count;

    if (sscanf(line.c_str(),
               "GOSSIP %100s %100s (gossip: %lu",
               name.data(),
               status.data(),
               &count) == 3) {
      out[name.data()] = std::pair<std::string, uint64_t>(status.data(), count);
    }
  }

  return out;
}

/**
 * Parses a string like:
 *
 * "GOSSIP N6 ALIVE (gossip: 2, instance-id: 1466138232075, failover: 0,
 *   state: ALIVE) BOYCOTTED"
 * "GOSSIP N7 DEAD (gossip: 2, instance-id: 1466138232075, failover: 0,
 *    state: SUSPECT) -"
 * ...
 * and returns a map with entries like [N6]["BOYCOTTED"], [N7]["-"]
 **/
std::map<std::string, std::string> parseGossipBoycottState(std::string output) {
  std::map<std::string, std::string> out;
  std::vector<std::string> lines;
  folly::split("\r\n", output, lines, /* ignoreEmpty */ true);
  for (const std::string& line : lines) {
    // read (max) 100 characters for name, then scan without assignment until a
    // close parenthesis, then read the boycott state
    std::string format = "GOSSIP %100s %*[^)]) %10s";
    std::array<char, 101> name{};
    std::array<char, 11> value{};
    if (sscanf(line.c_str(), format.c_str(), name.begin(), value.begin()) ==
        2) {
      out[name.cbegin()] = value.cbegin();
    }
  }

  return out;
}

/**
 * Parses the output of an admin command that outputs a json generated by the
 * AdminCommandTable utility (@see logdevice/common/AdminCommandTable.h) and
 * returns a vector of map from column name to value.
 * Parameters `node` and `command` are used only for error messages.
 */
std::vector<std::map<std::string, std::string>>
parseJsonAdminCommand(std::string data,
                      node_index_t node,
                      std::string command) {
  if (data.empty() || data.substr(0, strlen("ERROR")) == "ERROR") {
    // Silently ignore failure to send command, or errors returned in expected
    // format. This is not very nice, but many tests expect this behavior.
    return {};
  }

  std::vector<std::map<std::string, std::string>> res;
  folly::dynamic map;
  try {
    map = folly::parseJson(data);
  } catch (std::runtime_error& e) {
    ld_error("Got invalid json from N%d in response to '%s': %s",
             (int)node,
             command.c_str(),
             data.c_str());
    return {};
  }
  const auto headers = map["headers"];
  for (const auto& row : map["rows"]) {
    if (row.size() != headers.size()) {
      ld_error("Found row with invalid number of columns");
      ld_check(false);
      continue;
    }
    std::map<std::string, std::string> map_row;
    for (int i = 0; i < headers.size(); ++i) {
      const std::string v = row[i].isString() ? row[i].asString().c_str() : "";
      map_row[headers[i].asString().c_str()] = v;
      // map_row.insert(std::make_pair(headers[i], v));
    }
    res.emplace_back(std::move(map_row));
  }
  return res;
}
} // namespace

Cluster::Cluster(std::string root_path,
                 std::unique_ptr<TemporaryDirectory> root_pin,
                 std::string config_path,
                 std::string epoch_store_path,
                 std::string ncs_path,
                 std::string server_binary,
                 std::string admin_server_binary,
                 std::string cluster_name,
                 bool enable_logsconfig_manager,
                 dbg::Level default_log_level,
                 dbg::Colored default_log_colored,
                 NodesConfigurationSourceOfTruth nodes_configuration_sot)
    : root_path_(std::move(root_path)),
      root_pin_(std::move(root_pin)),
      config_path_(std::move(config_path)),
      epoch_store_path_(std::move(epoch_store_path)),
      ncs_path_(std::move(ncs_path)),
      server_binary_(std::move(server_binary)),
      admin_server_binary_(std::move(admin_server_binary)),
      cluster_name_(std::move(cluster_name)),
      enable_logsconfig_manager_(enable_logsconfig_manager),
      nodes_configuration_sot_(nodes_configuration_sot),
      default_log_level_(default_log_level),
      default_log_colored_(default_log_colored) {
  config_ = std::make_shared<UpdateableConfig>();
  client_settings_.reset(ClientSettings::create());
  ClientSettingsImpl* impl_settings =
      static_cast<ClientSettingsImpl*>(client_settings_.get());
  auto settings_updater = impl_settings->getSettingsUpdater();
  auto updater =
      std::make_shared<TextConfigUpdater>(config_->updateableServerConfig(),
                                          config_->updateableLogsConfig(),
                                          config_->updateableRqliteConfig(),
                                          impl_settings->getSettings());

  // Client should update its settings from the config file
  auto update_settings = [settings_updater](ServerConfig& config) -> bool {
    auto settings = config.getClientSettingsConfig();

    try {
      settings_updater->setFromConfig(settings);
    } catch (const boost::program_options::error&) {
      return false;
    }
    return true;
  };
  server_config_hook_handles_.push_back(
      config_->updateableServerConfig()->addHook(std::move(update_settings)));
  // Use small polling interval.
  auto config_source =
      std::make_unique<FileConfigSource>(std::chrono::milliseconds(100));
  config_source_ = config_source.get();
  updater->registerSource(std::move(config_source));
  updater->load("file:" + config_path_, nullptr);
  // Config reading shouldn't fail, we just generated it
  ld_check(config_->get() && "Invalid initial config");
  config_->updateableServerConfig()->setUpdater(updater);
  config_->updateableRqliteConfig()->setUpdater(updater);
  if (!impl_settings->getSettings()->enable_logsconfig_manager) {
    config_->updateableLogsConfig()->setUpdater(updater);
  } else {
    // create initial empty logsconfig
    auto logs_config = std::shared_ptr<LocalLogsConfig>(new LocalLogsConfig());
    logs_config->setInternalLogsConfig(
        config_->getServerConfig()->getInternalLogsConfig());
    config_->updateableLogsConfig()->update(logs_config);
  }

  nodes_configuration_updater_ =
      std::make_unique<NodesConfigurationFileUpdater>(
          config_->updateableNodesConfiguration(),
          buildNodesConfigurationStore());
}

logsconfig::LogAttributes
ClusterFactory::createLogAttributesStub(int nstorage_nodes) {
  auto attrs =
      logsconfig::LogAttributes().with_maxWritesInFlight(256).with_singleWriter(
          false);
  switch (nstorage_nodes) {
    case 1:
      attrs = attrs.with_replicationFactor(1).with_syncedCopies(0);
      break;
    case 2:
      attrs = attrs.with_replicationFactor(2).with_syncedCopies(0);
      break;
    default:
      attrs = attrs.with_replicationFactor(2).with_syncedCopies(0);
  }
  return attrs;
}

ClusterFactory::ClusterFactory() {
  populateDefaultServerSettings();
}

ClusterFactory& ClusterFactory::enableMessageErrorInjection() {
  // defaults
  double chance = 5.0;
  Status msg_status = E::CBREGISTERED;
  std::string env_chance;
  std::string env_status;
  if (getenv_switch("LOGDEVICE_TEST_MESSAGE_ERROR_CHANCE", &env_chance)) {
    double percent = std::stod(env_chance);
    if (percent < 0 || percent > 100) {
      ld_error("LOGDEVICE_TEST_MESSAGE_ERROR_CHANCE environment variable "
               "invalid. Got '%s', but must be between 0 and 100",
               env_chance.c_str());
    } else {
      chance = percent;
    }
  }

  if (getenv_switch("LOGDEVICE_TEST_MESSAGE_STATUS", &env_status)) {
    Status st = errorStrings().reverseLookup<std::string>(
        env_status, [](const std::string& s, const ErrorCodeInfo& e) {
          return s == e.name;
        });
    if (st == errorStrings().invalidEnum()) {
      ld_error("LOGDEVICE_TEST_MESSAGE_STATUS environment variable "
               "invalid. Got '%s'",
               env_status.c_str());
    } else {
      msg_status = st;
    }
  }

  return enableMessageErrorInjection(chance, msg_status);
}

ClusterFactory&
ClusterFactory::setConfigLogAttributes(logsconfig::LogAttributes attrs) {
  setInternalLogAttributes("config_log_deltas", attrs);
  setInternalLogAttributes("config_log_snapshots", attrs);
  return *this;
}

ClusterFactory&
ClusterFactory::setEventLogAttributes(logsconfig::LogAttributes attrs) {
  setInternalLogAttributes("event_log_deltas", attrs);
  setInternalLogAttributes("event_log_snapshots", attrs);
  return *this;
}

ClusterFactory&
ClusterFactory::setEventLogDeltaAttributes(logsconfig::LogAttributes attrs) {
  setInternalLogAttributes("event_log_deltas", attrs);
  return *this;
}

ClusterFactory&
ClusterFactory::setMaintenanceLogAttributes(logsconfig::LogAttributes attrs) {
  setInternalLogAttributes("maintenance_log_deltas", attrs);
  setInternalLogAttributes("maintenance_log_snapshots", attrs);
  return *this;
}

ClusterFactory& ClusterFactory::enableLogsConfigManager() {
  enable_logsconfig_manager_ = true;
  return *this;
}

logsconfig::LogAttributes
ClusterFactory::createDefaultLogAttributes(int nstorage_nodes) {
  return createLogAttributesStub(nstorage_nodes);
}

std::shared_ptr<const NodesConfiguration>
ClusterFactory::provisionNodesConfiguration(int nnodes) const {
  if (nodes_config_ != nullptr) {
    return nodes_config_;
  }

  configuration::Nodes nodes;

  int num_storage_nodes = 0;
  if (hash_based_sequencer_assignment_) {
    // Hash based sequencer assignment is used, all nodes are both sequencers
    // and storage nodes.
    for (int i = 0; i < nnodes; ++i) {
      Configuration::Node node;
      node.name = folly::sformat("server-{}", i);
      node.generation = 1;
      NodeLocation location;
      location.fromDomainString(std::string(LOC_PREFIX) +
                                std::to_string(i % num_racks_ + 1));
      node.location = location;

      node.addSequencerRole();
      node.addStorageRole(num_db_shards_);
      num_storage_nodes++;

      nodes[i] = std::move(node);
    }
  } else {
    // N0 is the only sequencer node.
    for (int i = 0; i < nnodes; ++i) {
      const bool is_storage_node = (nnodes == 1 || i > 0);
      Configuration::Node node;
      node.name = folly::sformat("server-{}", i);
      NodeLocation location;
      location.fromDomainString(std::string(LOC_PREFIX) +
                                std::to_string(i % num_racks_ + 1));
      node.location = location;
      node.generation = 1;
      if (i == 0) {
        node.addSequencerRole();
      }
      if (is_storage_node) {
        node.addStorageRole(num_db_shards_);
        num_storage_nodes++;
      }
      nodes[i] = std::move(node);
    }
  }

  ld_check(nnodes == (int)nodes.size());
  for (auto& it : nodes) {
    // this will be overridden later by createOneTry.
    it.second.address =
        Sockaddr(tcp_host_.empty() ? get_localhost_address_str() : tcp_host_,
                 std::to_string(it.first));
    if (!no_ssl_address_) {
      it.second.ssl_address.assign(
          Sockaddr(get_localhost_address_str(), std::to_string(it.first)));
    }
  }

  ReplicationProperty metadata_replication_property;
  if (meta_config_.has_value()) {
    auto meta_config = meta_config_.value();
    metadata_replication_property = ReplicationProperty::fromLogAttributes(
        meta_config.metadata_log_group->attrs());

    // Set which nodes are metadata nodes based on the passed nodeset
    // TODO: Deprecate the ability to pass nodesets in the MetaDataLogConfig
    // structure.
    std::set<node_index_t> metadata_nodes = {
        meta_config.metadata_nodes.begin(), meta_config.metadata_nodes.end()};
    for (auto& [nid, node] : nodes) {
      if (metadata_nodes.find(nid) != metadata_nodes.end()) {
        node.metadata_node = true;
      }
    }
  } else {
    int rep_factor = internal_logs_replication_factor_ > 0
        ? internal_logs_replication_factor_
        : 3;
    rep_factor = std::min(rep_factor, num_storage_nodes);

    metadata_replication_property =
        ReplicationProperty{{NodeLocationScope::NODE, rep_factor}};

    // metadata stored on all storage nodes with max replication factor 3
    for (auto& [_, node] : nodes) {
      node.metadata_node = true;
    }
  }

  return NodesConfigurationTestUtil::provisionNodes(
      std::move(nodes), std::move(metadata_replication_property));
}

std::unique_ptr<Cluster> ClusterFactory::create(int nnodes) {
  auto nodes_configuration = provisionNodesConfiguration(nnodes);
  int nstorage_nodes = nodes_configuration->getStorageNodes().size();

  logsconfig::LogAttributes log0;
  if (log_attributes_.has_value()) {
    // Caller supplied log config, use that.
    log0 = log_attributes_.value();
  } else {
    // Create a default log config with replication parameters that make sense
    // for a cluster of a given size.
    log0 = createDefaultLogAttributes(nstorage_nodes);
  }

  boost::icl::right_open_interval<logid_t::raw_type> logid_interval(
      1, num_logs_ + 1);
  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  logs_config->insert(logid_interval, log_group_name_, log0);
  logs_config->markAsFullyLoaded();

  Configuration::MetaDataLogsConfig meta_config;
  if (meta_config_.has_value()) {
    meta_config = meta_config_.value();
  } else {
    meta_config = createMetaDataLogsConfig({}, 0);
    if (!let_sequencers_provision_metadata_) {
      meta_config.sequencers_write_metadata_logs = false;
      meta_config.sequencers_provision_epoch_store = false;
    }
  }

  // Generic log configuration for internal logs
  logsconfig::LogAttributes internal_log_attrs =
      createLogAttributesStub(nstorage_nodes);

  // Internal logs shouldn't have a lower replication factor than data logs
  if (log_attributes_.has_value() &&
      log_attributes_.value().replicationFactor().hasValue() &&
      log_attributes_.value().replicationFactor().value() >
          internal_log_attrs.replicationFactor().value()) {
    internal_log_attrs = internal_log_attrs.with_replicationFactor(
        log_attributes_.value().replicationFactor().value());
  }
  if (internal_logs_replication_factor_ > 0) {
    internal_log_attrs = internal_log_attrs.with_replicationFactor(
        internal_logs_replication_factor_);
  }

  // configure the delta and snapshot logs if the user did not do so already.
  if (event_log_mode_ != EventLogMode::NONE &&
      !internal_logs_.logExists(
          configuration::InternalLogs::EVENT_LOG_DELTAS)) {
    setInternalLogAttributes("event_log_deltas", internal_log_attrs);
  }
  if (event_log_mode_ == EventLogMode::SNAPSHOTTED &&
      !internal_logs_.logExists(
          configuration::InternalLogs::EVENT_LOG_SNAPSHOTS)) {
    setInternalLogAttributes("event_log_snapshots", internal_log_attrs);
  }

  // configure the delta and snapshot logs for logsconfig
  // if the user did not do so already.
  if (!internal_logs_.logExists(
          configuration::InternalLogs::CONFIG_LOG_DELTAS)) {
    setInternalLogAttributes("config_log_deltas", internal_log_attrs);
  }

  if (!internal_logs_.logExists(
          configuration::InternalLogs::CONFIG_LOG_SNAPSHOTS)) {
    setInternalLogAttributes("config_log_snapshots", internal_log_attrs);
  }

  // configure the delta and snapshot logs for Maintenance log
  // if the user did not do so already
  if (!internal_logs_.logExists(
          configuration::InternalLogs::MAINTENANCE_LOG_DELTAS)) {
    setInternalLogAttributes("maintenance_log_deltas", internal_log_attrs);
  }

  if (!internal_logs_.logExists(
          configuration::InternalLogs::MAINTENANCE_LOG_SNAPSHOTS)) {
    setInternalLogAttributes("maintenance_log_snapshots", internal_log_attrs);
  }

  // Have all connections assigned to the ROOT scope and use the same
  // shaping config.
  configuration::TrafficShapingConfig ts_config;
  configuration::ShapingConfig read_throttling_config(
      std::set<NodeLocationScope>{NodeLocationScope::NODE},
      std::set<NodeLocationScope>{NodeLocationScope::NODE});
  if (use_default_traffic_shaping_config_) {
    auto root_fgp = ts_config.flowGroupPolicies.find(NodeLocationScope::ROOT);
    ld_check(root_fgp != ts_config.flowGroupPolicies.end());
    root_fgp->second.setConfigured(true);
    root_fgp->second.setEnabled(true);
    // Set burst capacity small to increase the likelyhood of experiencing
    // a message deferral during a test run.
    root_fgp->second.set(Priority::MAX,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000);
    root_fgp->second.set(Priority::CLIENT_HIGH,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000,
                         /*Max Bps*/ 2000000);
    // Provide 0 capacity for client normal so that it must always consume
    // bandwidth credit from the priority queue bucket.
    root_fgp->second.set(Priority::CLIENT_NORMAL,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 0,
                         /*Max Bps*/ 1000000);
    root_fgp->second.set(Priority::CLIENT_LOW,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000);
    root_fgp->second.set(Priority::BACKGROUND,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000,
                         /*Max Bps*/ 1100000);
    root_fgp->second.set(FlowGroup::PRIORITYQ_PRIORITY,
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 1000000);
    auto read_fgp =
        read_throttling_config.flowGroupPolicies.find(NodeLocationScope::NODE);
    ld_check(read_fgp != read_throttling_config.flowGroupPolicies.end());
    read_fgp->second.setConfigured(true);
    read_fgp->second.setEnabled(true);
    read_fgp->second.set(static_cast<Priority>(Priority::MAX),
                         /*Burst Bytes*/ 25000,
                         /*Guaranteed Bps*/ 50000);
    read_fgp->second.set(static_cast<Priority>(Priority::CLIENT_HIGH),
                         /*Burst Bytes*/ 20000,
                         /*Guaranteed Bps*/ 40000);
    read_fgp->second.set(static_cast<Priority>(Priority::CLIENT_NORMAL),
                         /*Burst Bytes*/ 15000,
                         /*Guaranteed Bps*/ 30000);
    read_fgp->second.set(static_cast<Priority>(Priority::CLIENT_LOW),
                         /*Burst Bytes*/ 10000,
                         /*Guaranteed Bps*/ 20000);
  }

  auto server_settings = ServerConfig::SettingsConfig();
  auto client_settings = ServerConfig::SettingsConfig();
  if (!enable_logsconfig_manager_) {
    // Default is true, so we only set to false if this option is not set.
    server_settings["enable-logsconfig-manager"] =
        client_settings["enable-logsconfig-manager"] = "false";
  }

  client_settings["event-log-snapshotting"] = "false";
  server_settings["event-log-snapshotting"] = "false";

  if (no_ssl_address_) {
    client_settings["ssl-load-client-cert"] = "false";
  } else {
    client_settings["ssl-ca-path"] =
        TEST_SSL_FILE("logdevice_test_valid_ca.cert");
  }

  auto config = std::make_unique<Configuration>(
      ServerConfig::fromDataTest(cluster_name_,
                                 std::move(meta_config),
                                 ServerConfig::PrincipalsConfig(),
                                 ServerConfig::SecurityConfig(),
                                 std::move(ts_config),
                                 std::move(read_throttling_config),
                                 std::move(server_settings),
                                 std::move(client_settings),
                                 internal_logs_),
      enable_logsconfig_manager_ ? nullptr : logs_config,
      std::move(nodes_configuration));
  logs_config->setInternalLogsConfig(
      config->serverConfig()->getInternalLogsConfig());

  if (getenv_switch("LOGDEVICE_TEST_USE_TCP")) {
    ld_info("LOGDEVICE_TEST_USE_TCP environment variable is set. Using TCP "
            "ports instead of unix domain sockets.");
    use_tcp_ = true;
  }

  return create(*config);
}

std::unique_ptr<Cluster>
ClusterFactory::createOneTry(const Configuration& source_config) {
  const std::string actual_server_binary = actualServerBinary();
  if (actual_server_binary.empty()) {
    // Abort early if this failed
    return nullptr;
  }
  const std::string actual_admin_server_binary = actualAdminServerBinary();
  if (actual_admin_server_binary.empty()) {
    // Abort early if this failed
    return nullptr;
  }

  auto nodes_configuration = source_config.getNodesConfiguration();
  const int nnodes = nodes_configuration->clusterSize();
  std::vector<node_index_t> node_ids(nnodes);
  std::map<node_index_t, node_gen_t> replacement_counters;

  int j = 0;
  for (const auto& [nid, _] : *nodes_configuration->getServiceDiscovery()) {
    ld_check(j < nnodes);
    node_ids[j++] = nid;
    auto* attrs = nodes_configuration->getNodeStorageAttribute(nid);
    replacement_counters[nid] = attrs ? attrs->generation : 1;
  }
  ld_check(j == nnodes);

  std::string root_path;
  std::unique_ptr<TemporaryDirectory> root_pin;
  if (root_path_.has_value()) {
    root_path = root_path_.value();
    boost::filesystem::create_directories(root_path);
  } else {
    // Create a directory that will contain all the data for this cluster
    root_pin = std::make_unique<TemporaryDirectory>("IntegrationTestUtils");
    root_path = root_pin->path().string();
  }

  std::string epoch_store_path = root_path + "/epoch_store";
  mkdir(epoch_store_path.c_str(), 0777);
  setServerSetting("epoch-store-path", epoch_store_path);

  ServerConfig::SettingsConfig server_settings =
      source_config.serverConfig()->getServerSettingsConfig();

  std::string ncs_path;
  {
    // If the settings specify a certain NCS path, use it, otherwise, use a
    // default one under root_path.
    auto config_ncs_path =
        server_settings.find("nodes-configuration-file-store-dir");
    if (config_ncs_path != server_settings.end()) {
      ncs_path = config_ncs_path->second;
    } else {
      ncs_path = root_path + "/nc_store";
      mkdir(ncs_path.c_str(), 0777);
    }
  }
  setServerSetting("nodes-configuration-file-store-dir", ncs_path);

  std::vector<ServerAddresses> addrs;
  if (Cluster::pickAddressesForServers(node_ids,
                                       use_tcp_,
                                       tcp_host_,
                                       root_path,
                                       replacement_counters,
                                       addrs) != 0) {
    return nullptr;
  }

  if (nodes_configuration->clusterSize() > 0) {
    // Set the final list of addresses
    NodesConfiguration::Update update{};
    update.service_discovery_update = std::make_unique<
        configuration::nodes::ServiceDiscoveryConfig::Update>();

    for (int i = 0; i < nnodes; ++i) {
      auto sd = nodes_configuration->getNodeServiceDiscovery(node_ids[i]);
      ld_check(sd);
      auto new_sd = *sd;
      addrs[i].toNodeConfig(new_sd, !no_ssl_address_);
      update.service_discovery_update->addNode(
          node_ids[i],
          {configuration::nodes::ServiceDiscoveryConfig::UpdateType::RESET,
           std::make_unique<configuration::nodes::NodeServiceDiscovery>(
               new_sd)});
    }
    nodes_configuration = nodes_configuration->applyUpdate(std::move(update));
    ld_check(nodes_configuration);
  }

  if (!nodes_configuration_sot_.has_value()) {
    // sot setting not provided. randomize the source of truth of NC.
    nodes_configuration_sot_.assign(NodesConfigurationSourceOfTruth::NCM);
  }

  ld_check(nodes_configuration_sot_.has_value());
  ld_info(
      "Using %s as source of truth for NodesConfiguration.",
      nodes_configuration_sot_.value() == NodesConfigurationSourceOfTruth::NCM
          ? "NCM"
          : "SERVER_CONFIG");
  switch (nodes_configuration_sot_.value()) {
    case NodesConfigurationSourceOfTruth::NCM:
      setServerSetting("enable-nodes-configuration-manager", "true");
      setServerSetting(
          "use-nodes-configuration-manager-nodes-configuration", "true");
      break;
    case NodesConfigurationSourceOfTruth::SERVER_CONFIG:
      setServerSetting(
          "use-nodes-configuration-manager-nodes-configuration", "false");
      break;
  }

  {
    // Set NCM seed for clients in the config.
    std::vector<std::string> addrs;
    for (const auto& [_, node] : *nodes_configuration->getServiceDiscovery()) {
      addrs.push_back(node.default_client_data_address.toString());
    }
    auto seed = folly::sformat("data:{}", folly::join(",", addrs));
    setClientSetting("nodes-configuration-seed-servers", seed);
  }

  // Merge the provided server settings with the existing settings
  for (const auto& [key, value] : server_settings_) {
    server_settings.emplace(key, value);
  }

  ServerConfig::SettingsConfig client_settings =
      source_config.serverConfig()->getClientSettingsConfig();
  // Merge the provided client settings with the client settings
  for (const auto& [key, value] : client_settings_) {
    client_settings[key] = value;
  }

  ld_info("Cluster created with data in %s", root_path.c_str());
  std::unique_ptr<Configuration> config =
      std::make_unique<Configuration>(source_config.serverConfig()
                                          ->withServerSettings(server_settings)
                                          ->withClientSettings(client_settings),
                                      source_config.logsConfig(),
                                      std::move(nodes_configuration));

  // Write new config to disk so that logdeviced processes can access it
  std::string config_path = root_path + "/logdevice.conf";
  if (overwriteConfig(config_path.c_str(),
                      config->serverConfig().get(),
                      config->logsConfig().get()) != 0) {
    return nullptr;
  }

  std::unique_ptr<Cluster> cluster(
      new Cluster(root_path,
                  std::move(root_pin),
                  config_path,
                  epoch_store_path,
                  ncs_path,
                  actual_server_binary,
                  actual_admin_server_binary,
                  cluster_name_,
                  enable_logsconfig_manager_,
                  default_log_level_,
                  default_log_colored_,
                  nodes_configuration_sot_.value()));
  if (use_tcp_) {
    cluster->use_tcp_ = true;
  }
  if (!tcp_host_.empty()) {
    cluster->tcp_host_ = tcp_host_;
  }
  if (user_admin_port_ > 0) {
    cluster->user_admin_port_ = user_admin_port_;
  }
  if (no_ssl_address_) {
    cluster->no_ssl_address_ = true;
  }

  cluster->outer_tries_ = outerTries();
  cluster->cmd_param_ = cmd_param_;
  cluster->num_db_shards_ = num_db_shards_;
  cluster->rocksdb_type_ = rocksdb_type_;
  cluster->hash_based_sequencer_assignment_ = hash_based_sequencer_assignment_;
  cluster->setNodeReplacementCounters(std::move(replacement_counters));

  if (cluster->updateNodesConfiguration(*config->getNodesConfiguration()) !=
      0) {
    return nullptr;
  }
  cluster->nodes_configuration_updater_->start();
  wait_until("NodesConfiguration is picked by the updater", [&]() {
    return cluster->getConfig()->getNodesConfiguration() != nullptr;
  });

  // Start Admin Server if enabled
  if (use_standalone_admin_server_) {
    cluster->admin_server_ = cluster->createAdminServer();
  }

  // create Node objects, but don't start the processes
  for (int i = 0; i < nnodes; i++) {
    cluster->nodes_[node_ids[i]] =
        cluster->createNode(node_ids[i], std::move(addrs[i]));
  }

  // if allowed, provision the initial epoch metadata in epoch store,
  // as well as metadata storage nodes
  if (provision_epoch_metadata_) {
    if (cluster->provisionEpochMetaData(
            provision_nodeset_selector_, allow_existing_metadata_) != 0) {
      return nullptr;
    }
  }

  if (!defer_start_ && cluster->start() != 0) {
    return nullptr;
  }

  if (num_logs_config_manager_logs_ > 0) {
    auto log_group = createLogsConfigManagerLogs(cluster);
    if (log_group == nullptr) {
      ld_error("Failed to create the default logs config manager logs.");
    }
  }

  return cluster;
}

std::unique_ptr<client::LogGroup>
ClusterFactory::createLogsConfigManagerLogs(std::unique_ptr<Cluster>& cluster) {
  int num_storage_nodes =
      cluster->getConfig()->getNodesConfiguration()->getStorageNodes().size();
  logsconfig::LogAttributes attrs = log_attributes_.has_value()
      ? log_attributes_.value()
      : createDefaultLogAttributes(num_storage_nodes);

  return cluster->createClient()->makeLogGroupSync(
      "/test_logs",
      logid_range_t(logid_t(1), logid_t(num_logs_config_manager_logs_)),
      attrs);
}

void ClusterFactory::populateDefaultServerSettings() {
  // Poll for config updates more frequently in tests so that they
  // progress faster
  setServerSetting("file-config-update-interval", "100ms");

  setServerSetting("assert-on-data", "true");
  setServerSetting("enable-config-synchronization", "true");
  // Disable rebuilding by default in tests, the test framework
  // (`waitUntilRebuilt' etc) is not ready for it: #14697277
  setServerSetting("disable-rebuilding", "true");
  // Disable the random delay for SHARD_IS_REBUILT messages
  setServerSetting("shard-is-rebuilt-msg-delay", "0s..0s");
  // TODO(T22614431): remove this option once it's been enabled
  // everywhere.
  setServerSetting("allow-conditional-rebuilding-restarts", "true");
  setServerSetting("rebuilding-restarts-grace-period", "1ms");
  setServerSetting("planner-scheduling-delay", "1s");
  // RebuildingTest does not expect this: #14697312
  setServerSetting("enable-self-initiated-rebuilding", "false");
  // disable failure detector because it delays sequencer startup
  setServerSetting("gossip-enabled", "false");
  setServerSetting("ignore-cluster-marker", "true");
  setServerSetting("rocksdb-auto-create-shards", "true");
  setServerSetting("num-workers", "5");
  // always enable NCM
  setServerSetting("enable-nodes-configuration-manager", "true");
  setServerSetting(
      "nodes-configuration-manager-store-polling-interval", "100ms");

  // Small timeout is needed so that appends that happen right after
  // rebuilding, when socket isn't reconnected yet, retry quickly.
  setServerSetting("store-timeout", "10ms..1s");
  // smaller recovery retry timeout for reading seqencer metadata
  setServerSetting("recovery-seq-metadata-timeout", "100ms..500ms");
  // Smaller mutation and cleaning timeout, to make recovery retry faster
  // if a participating node died at the wrong moment.
  // TODO (#54460972): Would be better to make recovery detect such
  // situations by itself, probably using ClusterState.
  setServerSetting("recovery-timeout", "5s");
  // if we fail to store something on a node, we should retry earlier than
  // the default 60s
  setServerSetting("unroutable-retry-interval", "1s");

  // Disable disk space checking by default; tests that want it can
  // override
  setServerSetting("free-disk-space-threshold", "0.000001");
  // Run fewer than the default 4 threads to perform better under load
  setServerSetting("storage-threads-per-shard-slow", "2");
  setServerSetting("rocksdb-allow-fallocate", "false");
  // Reduce memory usage for storage thread queues compared to the
  // default setting
  setServerSetting("max-inflight-storage-tasks", "512");

  if (!no_ssl_address_) {
    setServerSetting(
        "ssl-ca-path", TEST_SSL_FILE("logdevice_test_valid_ca.cert"));
    setServerSetting(
        "ssl-cert-path", TEST_SSL_FILE("logdevice_test_valid.cert"));
    setServerSetting("ssl-key-path", TEST_SSL_FILE("logdevice_test.key"));
  }
}

int Cluster::pickAddressesForServers(
    const std::vector<node_index_t>& indices,
    bool use_tcp,
    std::string tcp_host,
    const std::string& root_path,
    const std::map<node_index_t, node_gen_t>& node_replacement_counters,
    std::vector<ServerAddresses>& out) {
  if (use_tcp) {
    // This test uses TCP. Look for enough free ports for each node.
    std::vector<detail::PortOwner> ports;
    if (detail::find_free_port_set(
            indices.size() * ServerAddresses::COUNT, ports) != 0) {
      ld_error("Not enough free ports on system for %lu nodes", indices.size());
      return -1;
    }

    out.resize(indices.size());
    for (int i = 0; i < indices.size(); ++i) {
      std::vector<detail::PortOwner> node_ports(
          std::make_move_iterator(ports.begin() + i * ServerAddresses::COUNT),
          std::make_move_iterator(ports.begin() +
                                  (i + 1) * ServerAddresses::COUNT));
      out[i] = ServerAddresses::withTCPPorts(std::move(node_ports), tcp_host);
    }
  } else {
    // This test uses unix domain sockets. These will be created in the
    // test directory.
    out.resize(indices.size());
    for (size_t i = 0; i < indices.size(); ++i) {
      out[i] = ServerAddresses::withUnixSockets(Cluster::getNodeDataPath(
          root_path, indices.at(i), node_replacement_counters.at(indices[i])));
    }
  }

  return 0;
}

int Cluster::expandViaAdminServer(thrift::AdminAPIAsyncClient& admin_client,
                                  int nnodes,
                                  bool start_nodes,
                                  int num_racks) {
  std::vector<node_index_t> new_indices;
  node_index_t first = config_->getNodesConfiguration()->getMaxNodeIndex() + 1;
  for (int i = 0; i < nnodes; ++i) {
    new_indices.push_back(first + i);
  }
  return expandViaAdminServer(
      admin_client, std::move(new_indices), start_nodes, num_racks);
}

int Cluster::expandViaAdminServer(thrift::AdminAPIAsyncClient& admin_client,
                                  std::vector<node_index_t> new_indices,
                                  bool start_nodes,
                                  int num_racks) {
  std::sort(new_indices.begin(), new_indices.end());
  if (std::unique(new_indices.begin(), new_indices.end()) !=
      new_indices.end()) {
    ld_error("expandViaAdminServer() called with duplicate indices");
    return -1;
  }

  auto nodes_config = getConfig()->getNodesConfiguration();
  for (node_index_t i : new_indices) {
    if (nodes_config->isNodeInServiceDiscoveryConfig(i) || nodes_.count(i)) {
      ld_error("expandViaAdminServer() called with node index %d that already "
               "exists",
               (int)i);
      return -1;
    }
  }
  ld_info("Expanding with nodes %s", toString(new_indices).c_str());

  configuration::Nodes nodes;
  for (node_index_t idx : new_indices) {
    Configuration::Node node;
    node.name = folly::sformat("server-{}", idx);
    node.generation = 1;
    NodeLocation location;
    location.fromDomainString(std::string(LOC_PREFIX) +
                              std::to_string(idx % num_racks + 1));
    node.location = location;
    setNodeReplacementCounter(idx, 1);

    // Storage only node.
    node.addStorageRole(num_db_shards_);
    nodes[idx] = std::move(node);
  }

  nodes_config = nodes_config->applyUpdate(
      NodesConfigurationTestUtil::addNewNodesUpdate(*nodes_config, nodes));
  ld_check(nodes_config);

  std::vector<ServerAddresses> addrs;
  if (pickAddressesForServers(new_indices,
                              use_tcp_,
                              tcp_host_,
                              root_path_,
                              node_replacement_counters_,
                              addrs) != 0) {
    return -1;
  }

  // Set the addresses
  NodesConfiguration::Update update{};
  update.service_discovery_update =
      std::make_unique<configuration::nodes::ServiceDiscoveryConfig::Update>();

  for (size_t i = 0; i < new_indices.size(); ++i) {
    auto idx = new_indices[i];
    auto sd = nodes_config->getNodeServiceDiscovery(idx);
    ld_check(sd);
    auto new_sd = *sd;
    addrs[i].toNodeConfig(new_sd, !no_ssl_address_);

    update.service_discovery_update->addNode(
        idx,
        {configuration::nodes::ServiceDiscoveryConfig::UpdateType::RESET,
         std::make_unique<configuration::nodes::NodeServiceDiscovery>(new_sd)});
  }
  nodes_config = nodes_config->applyUpdate(std::move(update));
  ld_check(nodes_config);

  // Tests expect the nodes to be enabled. Let's force enable the new nodes.
  std::vector<ShardID> shards;
  for (node_index_t idx : new_indices) {
    shards.emplace_back(idx, -1);
  }

  // Submit the request to Admin Server
  thrift::AddNodesRequest req;
  thrift::AddNodesResponse resp;
  for (node_index_t i : new_indices) {
    thrift::NodeConfig node_cfg;
    fillNodeConfig(node_cfg, i, *nodes_config);
    thrift::AddSingleNodeRequest single;

    ld_info("Adding Node: %s", thriftToJson(node_cfg).c_str());
    single.set_new_config(std::move(node_cfg));
    req.new_node_requests_ref()->push_back(std::move(single));
  }
  try {
    admin_client.sync_addNodes(resp, req);
  } catch (const thrift::ClusterMembershipOperationFailed& exception) {
    ld_error("Failed to expand the cluster with nodes %s: %s (%s)",
             toString(new_indices).c_str(),
             exception.what(),
             thriftToJson(exception).c_str());
    return -1;
  }
  vcs_config_version_t new_config_version(
      static_cast<uint64_t>(resp.get_new_nodes_configuration_version()));
  ld_info("Nodes added via Admin API in new config version %lu",
          new_config_version.val_);

  waitForServersAndClientsToProcessNodesConfiguration(new_config_version);
  for (size_t i = 0; i < new_indices.size(); ++i) {
    node_index_t idx = new_indices[i];
    nodes_[idx] = createNode(idx, std::move(addrs[i]));
  }
  if (!start_nodes) {
    return 0;
  }
  return start(new_indices);
}

int Cluster::expand(std::vector<node_index_t> new_indices, bool start_nodes) {
  std::sort(new_indices.begin(), new_indices.end());
  if (std::unique(new_indices.begin(), new_indices.end()) !=
      new_indices.end()) {
    ld_error("expand() called with duplicate indices");
    return -1;
  }

  auto nodes_config = getConfig()->getNodesConfiguration();
  for (node_index_t i : new_indices) {
    if (nodes_config->isNodeInServiceDiscoveryConfig(i) || nodes_.count(i)) {
      ld_error(
          "expand() called with node index %d that already exists", (int)i);
      return -1;
    }
  }

  configuration::Nodes nodes;
  for (node_index_t idx : new_indices) {
    Configuration::Node node;
    node.name = folly::sformat("server-{}", idx);
    node.generation = 1;
    setNodeReplacementCounter(idx, 1);

    // Storage only node.
    node.addStorageRole(num_db_shards_);
    nodes[idx] = std::move(node);
  }

  nodes_config = nodes_config->applyUpdate(
      NodesConfigurationTestUtil::addNewNodesUpdate(*nodes_config, nodes));
  ld_check(nodes_config);

  std::vector<ServerAddresses> addrs;
  if (pickAddressesForServers(new_indices,
                              use_tcp_,
                              tcp_host_,
                              root_path_,
                              node_replacement_counters_,
                              addrs) != 0) {
    return -1;
  }

  {
    // Set the addresses
    NodesConfiguration::Update update{};
    update.service_discovery_update = std::make_unique<
        configuration::nodes::ServiceDiscoveryConfig::Update>();

    for (size_t i = 0; i < new_indices.size(); ++i) {
      auto idx = new_indices[i];
      auto sd = nodes_config->getNodeServiceDiscovery(idx);
      ld_check(sd);
      auto new_sd = *sd;
      addrs[i].toNodeConfig(new_sd, !no_ssl_address_);
      update.service_discovery_update->addNode(
          idx,
          {configuration::nodes::ServiceDiscoveryConfig::UpdateType::RESET,
           std::make_unique<configuration::nodes::NodeServiceDiscovery>(
               new_sd)});
    }
    nodes_config = nodes_config->applyUpdate(std::move(update));
    ld_check(nodes_config);
  }

  {
    // Tests expect the nodes to be enabled. Let's force enable the new nodes.
    std::vector<ShardID> shards;
    for (node_index_t idx : new_indices) {
      shards.emplace_back(idx, -1);
    }
    nodes_config = nodes_config->applyUpdate(
        NodesConfigurationTestUtil::setStorageMembershipUpdate(
            *nodes_config,
            std::move(shards),
            membership::StorageState::READ_WRITE,
            folly::none));
    ld_check(nodes_config);
  }

  int rv = updateNodesConfiguration(*nodes_config);
  if (rv != 0) {
    return -1;
  }

  if (!start_nodes) {
    return 0;
  }

  for (size_t i = 0; i < new_indices.size(); ++i) {
    node_index_t idx = new_indices[i];
    nodes_[idx] = createNode(idx, std::move(addrs[i]));
  }

  return start(new_indices);
}

int Cluster::expand(int nnodes, bool start) {
  std::vector<node_index_t> new_indices;
  node_index_t first = config_->getNodesConfiguration()->getMaxNodeIndex() + 1;
  for (int i = 0; i < nnodes; ++i) {
    new_indices.push_back(first + i);
  }
  return expand(new_indices, start);
}

int Cluster::shrink(std::vector<node_index_t> indices) {
  if (indices.empty()) {
    ld_error("shrink() called with no nodes");
    return -1;
  }

  std::sort(indices.begin(), indices.end());
  if (std::unique(indices.begin(), indices.end()) != indices.end()) {
    ld_error("shrink() called with duplicate indices");
    return -1;
  }

  // Kill the nodes before we remove them from the cluster.
  for (node_index_t i : indices) {
    if (getNode(i).isRunning()) {
      getNode(i).kill();
    }
  }

  for (node_index_t i : indices) {
    nodes_.erase(i);
  }

  // We need to force set the storage state to EMPTY so that NCM can allow us
  // to shrink them.
  auto nodes_config = getConfig()->getNodesConfiguration();

  std::vector<ShardID> shards;
  std::transform(indices.begin(),
                 indices.end(),
                 std::back_inserter(shards),
                 [](node_index_t idx) { return ShardID(idx, -1); });

  nodes_config = nodes_config->applyUpdate(
      NodesConfigurationTestUtil::setStorageMembershipUpdate(
          *nodes_config,
          shards,
          membership::StorageState::NONE,
          membership::MetaDataStorageState::NONE));

  nodes_config = nodes_config->applyUpdate(
      NodesConfigurationTestUtil::shrinkNodesUpdate(*nodes_config, indices));

  int rv = updateNodesConfiguration(*nodes_config);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

int Cluster::shrink(int nnodes) {
  auto cfg = config_->get();

  // Find nnodes highest node indices.
  std::vector<node_index_t> indices;
  for (auto it = nodes_.crbegin(); it != nodes_.crend() && nnodes > 0;
       ++it, --nnodes) {
    indices.push_back(it->first);
  }
  if (nnodes != 0) {
    ld_error("shrink() called with too many nodes");
    return -1;
  }

  return shrink(indices);
}

int Cluster::shrinkViaAdminServer(thrift::AdminAPIAsyncClient& admin_client,
                                  std::vector<node_index_t> indices) {
  if (indices.empty()) {
    ld_error("shrink() called with no nodes");
    return -1;
  }

  std::sort(indices.begin(), indices.end());
  if (std::unique(indices.begin(), indices.end()) != indices.end()) {
    ld_error("shrink() called with duplicate indices");
    return -1;
  }

  // Kill the nodes before we remove them from the cluster.
  ld_info("Killing nodes (for shrink) %s", toString(indices).c_str());
  for (node_index_t i : indices) {
    if (getNode(i).isRunning()) {
      getNode(i).kill();
    }
  }

  ld_info("Shrinking with nodes %s", toString(indices).c_str());

  // Submit the request to Admin Server
  thrift::RemoveNodesRequest req;
  thrift::RemoveNodesResponse resp;
  for (node_index_t i : indices) {
    thrift::NodesFilter filter;
    thrift::NodeID node;
    node.set_node_index(i);
    filter.set_node(node);
    req.node_filters_ref()->push_back(std::move(filter));
  }

  try {
    admin_client.sync_removeNodes(resp, req);
  } catch (const thrift::ClusterMembershipOperationFailed& exception) {
    ld_error("Failed to shrink the cluster with nodes %s: %s (%s)",
             toString(indices).c_str(),
             exception.what(),
             thriftToJson(exception).c_str());
    return -1;
  }
  vcs_config_version_t new_config_version(
      static_cast<uint64_t>(resp.get_new_nodes_configuration_version()));
  ld_info("Nodes removed via Admin API in new config version %lu",
          new_config_version.val_);

  waitForServersAndClientsToProcessNodesConfiguration(new_config_version);
  // After we have removed the nodes from config.
  for (node_index_t i : indices) {
    nodes_.erase(i);
  }
  return 0;
}

int Cluster::shrinkViaAdminServer(thrift::AdminAPIAsyncClient& admin_client,
                                  int nnodes) {
  auto cfg = config_->get();

  // Find nnodes highest node indices.
  std::vector<node_index_t> indices;
  for (auto it = nodes_.crbegin(); it != nodes_.crend() && nnodes > 0;
       ++it, --nnodes) {
    indices.push_back(it->first);
  }
  if (nnodes != 0) {
    ld_error("shrinkViaAdminServer() called with too many nodes");
    return -1;
  }

  return shrinkViaAdminServer(admin_client, indices);
}

void Cluster::stop() {
  for (auto& it : nodes_) {
    it.second->kill();
  }
}

int Cluster::start(std::vector<node_index_t> indices) {
  // Start admin server if we are configured to start one first.
  if (admin_server_) {
    admin_server_->start();
    admin_server_->waitUntilStarted();
  }
  if (indices.size() == 0) {
    for (auto& it : nodes_) {
      indices.push_back(it.first);
    }
  }

  for (node_index_t i : indices) {
    nodes_.at(i)->start();
  }

  for (node_index_t i : indices) {
    if (nodes_.at(i)->waitUntilStarted() != 0 ||
        nodes_.at(i)->waitUntilAvailable() != 0) {
      return -1;
    }
  }

  maybe_pause_for_gdb(*this, indices);
  return 0;
}

int Cluster::provisionEpochMetaData(std::shared_ptr<NodeSetSelector> selector,
                                    bool allow_existing_metadata) {
  auto meta_provisioner = createMetaDataProvisioner();
  if (selector == nullptr) {
    selector = NodeSetSelectorFactory::create(NodeSetSelectorType::SELECT_ALL);
  }

  int rv = meta_provisioner->provisionEpochMetaData(
      std::move(selector), allow_existing_metadata, true);
  if (rv != 0) {
    ld_error("Failed to provision epoch metadata for the cluster.");
  }
  return rv;
}

int Cluster::updateNodesConfiguration(
    const NodesConfiguration& nodes_configuration) {
  using namespace logdevice::configuration::nodes;
  auto store = buildNodesConfigurationStore();
  if (store == nullptr) {
    return -1;
  }
  auto serialized = NodesConfigurationCodec::serialize(nodes_configuration);
  if (serialized.empty()) {
    return -1;
  }
  store->updateConfigSync(
      std::move(serialized), NodesConfigurationStore::Condition::overwrite());
  waitForServersAndClientsToProcessNodesConfiguration(
      nodes_configuration.getVersion());
  return 0;
}

std::unique_ptr<AdminServer> Cluster::createAdminServer() {
  std::unique_ptr<AdminServer> server = std::make_unique<AdminServer>();
  server->data_path_ = root_path_ + "/admin_server";
  // Create the directory for logs and unix socket
  mkdir(server->data_path_.c_str(), 0777);
  // This test uses TCP. Look for enough free ports for each node.
  Sockaddr admin_address;
  std::vector<detail::PortOwner> port_owners;
  if (use_tcp_) {
    if (user_admin_port_ > 0) {
      auto owner = detail::claim_port(user_admin_port_);
      if (owner.has_value()) {
        port_owners.push_back(std::move(owner.value()));
      } else {
        ld_error("Claim user admin port %d failed", user_admin_port_);
      }
    } else if (detail::find_free_port_set(1, port_owners) != 0) {
      ld_error("No free ports on system for admin server");
      return nullptr;
    }

    admin_address =
        Sockaddr(tcp_host_.empty() ? get_localhost_address_str() : tcp_host_,
                 port_owners[0].port);
  } else {
    // This test uses unix domain sockets. These will be created in the
    // test directory.
    admin_address = Sockaddr(server->data_path_ + "/socket_admin");
  }
  auto protocol_addr_param = admin_address.isUnixAddress()
      ? std::make_pair(
            "--admin-unix-socket", ParamValue{admin_address.getPath()})
      : std::make_pair(
            "--admin-port", ParamValue{std::to_string(admin_address.port())});
  server->address_ = admin_address;
  server->port_owners_ = std::move(port_owners);
  server->admin_server_binary_ = admin_server_binary_;
  server->config_path_ = config_path_;
  server->cmd_args_ = {
      protocol_addr_param,
      {"--config-path", ParamValue{"file:" + server->config_path_}},
      {"--loglevel", ParamValue{loglevelToString(default_log_level_)}},
      {"--logcolored", ParamValue{logcoloredToString(default_log_colored_)}},
      {"--log-file", ParamValue{server->getLogPath()}},
      {"--enable-maintenance-manager", ParamValue{"true"}},
      {"--enable-cluster-maintenance-state-machine", ParamValue{"true"}},
      {"--maintenance-manager-reevaluation-timeout", ParamValue{"5s"}},
      {"--enable-safety-check-periodic-metadata-update", ParamValue{"true"}},
      {"--safety-check-metadata-update-period", ParamValue{"30s"}},
      {"--maintenance-log-snapshotting", ParamValue{"true"}},
  };
  ld_info("Admin Server will be started on address: %s",
          server->address_.toString().c_str());
  return server;
} // namespace IntegrationTestUtils

std::unique_ptr<Node> Cluster::createNode(node_index_t index,
                                          ServerAddresses addrs) const {
  std::unique_ptr<Node> node = std::make_unique<Node>();
  node->node_index_ = index;
  node->name_ = folly::sformat("Node{}", index);
  node->addrs_ = std::move(addrs);
  node->num_db_shards_ = num_db_shards_;
  node->rocksdb_type_ = rocksdb_type_;
  node->server_binary_ = server_binary_;
  node->gossip_enabled_ = isGossipEnabled();

  // Data path will be something like
  // /tmp/logdevice/IntegrationTestUtils.MkkZyS/N0:1/
  node->data_path_ = Cluster::getNodeDataPath(root_path_, index);
  boost::filesystem::create_directories(node->data_path_);
  node->config_path_ = config_path_;

  node->is_storage_node_ =
      config_->getNodesConfiguration()->isStorageNode(index);
  node->is_sequencer_node_ =
      config_->getNodesConfiguration()->isSequencerNode(index);
  node->cmd_args_ = commandArgsForNode(*node);

  ld_info("Node N%d:%d will be started on addresses: protocol:%s, ssl:%s"
          ", gossip:%s, admin:%s (data in %s), server-to-server:%s"
          ", server thrift:%s, client thrift:%s",
          index,
          getNodeReplacementCounter(index),
          node->addrs_.protocol.toString().c_str(),
          node->addrs_.protocol_ssl.toString().c_str(),
          node->addrs_.gossip.toString().c_str(),
          node->addrs_.admin.toString().c_str(),
          node->data_path_.c_str(),
          node->addrs_.server_to_server.toString().c_str(),
          node->addrs_.server_thrift_api.toString().c_str(),
          node->addrs_.client_thrift_api.toString().c_str());

  return node;
}

std::unique_ptr<Node>
Cluster::createSelfRegisteringNode(const std::string& name) const {
  // We need gossip to be enabled to use self registration for the maintenance
  // manager to enable the node.
  ld_check(isGossipEnabled());
  // Self registration only works with the NCM being the source of truth.
  ld_check(nodes_configuration_sot_ == NodesConfigurationSourceOfTruth::NCM);

  std::unique_ptr<Node> node = std::make_unique<Node>();
  node->name_ = name;
  node->num_db_shards_ = num_db_shards_;
  node->rocksdb_type_ = rocksdb_type_;
  node->server_binary_ = server_binary_;
  node->gossip_enabled_ = true;

  // Data path will be something like
  // /tmp/logdevice/IntegrationTestUtils.MkkZyS/<name>/
  node->data_path_ = Cluster::getNodeDataPath(root_path_, name);
  boost::filesystem::create_directories(node->data_path_);
  node->config_path_ = config_path_;

  // Allocate the addresses
  ServerAddresses addrs;
  if (use_tcp_) {
    // This test uses TCP. Look for enough free ports for each node.
    std::vector<detail::PortOwner> ports;
    if (detail::find_free_port_set(ServerAddresses::COUNT, ports) != 0) {
      ld_error("Not enough free ports on system to allocate");
      return nullptr;
    }
    node->addrs_ = ServerAddresses::withTCPPorts(std::move(ports), tcp_host_);
  } else {
    node->addrs_ = ServerAddresses::withUnixSockets(node->data_path_);
  }

  // For now, let's create them always as both sequencer and storage, later on
  // if needed we can change this function to accept the roles.
  node->is_storage_node_ = true;
  node->is_sequencer_node_ = true;

  node->cmd_args_ = commandArgsForNode(*node);

  ld_info("Node %s (with self registration) will be started on addresses: "
          "protocol:%s, ssl: %s, gossip:%s, admin:%s (data in %s), "
          "server-to-server:%s, server thrift api:%s, client thrift api:%s",
          name.c_str(),
          node->addrs_.protocol.toString().c_str(),
          node->addrs_.protocol_ssl.toString().c_str(),
          node->addrs_.gossip.toString().c_str(),
          node->addrs_.admin.toString().c_str(),
          node->data_path_.c_str(),
          node->addrs_.server_to_server.toString().c_str(),
          node->addrs_.server_thrift_api.toString().c_str(),
          node->addrs_.client_thrift_api.toString().c_str());

  return node;
}

ParamMap Cluster::commandArgsForNode(const Node& node) const {
  std::shared_ptr<const Configuration> config = config_->get();

  const auto& p = node.addrs_.protocol;
  auto protocol_addr_param = p.isUnixAddress()
      ? std::make_pair("--unix-socket", ParamValue{p.getPath()})
      : std::make_pair("--port", ParamValue{std::to_string(p.port())});

  const auto& g = node.addrs_.gossip;
  auto gossip_addr_param = g.isUnixAddress()
      ? std::make_pair("--gossip-unix-socket", ParamValue{g.getPath()})
      : std::make_pair("--gossip-port", ParamValue{std::to_string(g.port())});

  const auto& admn = node.addrs_.admin;
  auto admin_addr_param = admn.isUnixAddress()
      ? std::make_pair("--admin-unix-socket", ParamValue{admn.getPath()})
      : std::make_pair("--admin-port", ParamValue{std::to_string(admn.port())});

  const auto& s2s = node.addrs_.server_to_server;
  auto s2s_addr_param = s2s.isUnixAddress()
      ? std::make_pair(
            "--server-to-server-unix-socket", ParamValue{s2s.getPath()})
      : std::make_pair(
            "--server-to-server-port", ParamValue{std::to_string(s2s.port())});

  const auto& server_thrift = node.addrs_.server_thrift_api;
  auto server_thrift_addr_param = server_thrift.isUnixAddress()
      ? std::make_pair("--server-thrift-api-unix-socket",
                       ParamValue{server_thrift.getPath()})
      : std::make_pair("--server-thrift-api-port",
                       ParamValue{std::to_string(server_thrift.port())});

  const auto& client_thrift = node.addrs_.client_thrift_api;
  auto client_thrift_addr_param = client_thrift.isUnixAddress()
      ? std::make_pair("--client-thrift-api-unix-socket",
                       ParamValue{client_thrift.getPath()})
      : std::make_pair("--client-thrift-api-port",
                       ParamValue{std::to_string(client_thrift.port())});

  // TODO: T71290188 add ports per network priority here too

  // clang-format off

  // Construct the default parameters.
  ParamMaps default_param_map = {
    { ParamScope::ALL,
      {
        protocol_addr_param, gossip_addr_param, admin_addr_param, s2s_addr_param,
        server_thrift_addr_param, client_thrift_addr_param,
        {"--name", ParamValue{node.name_}},
        {"--test-mode", ParamValue{"true"}},
        {"--config-path", ParamValue{"file:" + node.config_path_}},
        {"--loglevel", ParamValue{loglevelToString(default_log_level_)}},
        {"--logcolored", ParamValue{logcoloredToString(default_log_colored_)}},
        {"--log-file", ParamValue{node.getLogPath()}},
        {"--server-id", ParamValue{node.server_id_}},
      }
    },
    { ParamScope::SEQUENCER,
      {
        {"--sequencers", ParamValue{"all"}},
      }
    },
    { ParamScope::STORAGE_NODE,
      {
        {"--local-log-store-path", ParamValue{node.getDatabasePath()}},
        {"--num-shards", ParamValue{std::to_string(node.num_db_shards_)}},
      }
    }
  };

  // clang-format on

  // Both `default_param_map' and `cmd_param_' specify params for the 3
  // different ParamScopes (ALL, SEQUENCER, STORAGE_NODE).  Time to flatten
  // based on whether the current node is a sequencer and/or a storage node.

  std::vector<ParamScope> scopes;
  if (node.is_sequencer_node_) {
    scopes.push_back(ParamScope::SEQUENCER);
  }
  if (node.is_storage_node_) {
    scopes.push_back(ParamScope::STORAGE_NODE);
  }
  // ALL comes last so it doesn't overwrite more specific scopes.
  // (unordered_map::insert() keeps existing keys.)
  scopes.push_back(ParamScope::ALL);
  ParamMap defaults_flat, overrides_flat;
  for (ParamScope scope : scopes) {
    defaults_flat.insert(
        default_param_map[scope].cbegin(), default_param_map[scope].cend());
    auto it = cmd_param_.find(scope);
    if (it != cmd_param_.end()) {
      overrides_flat.insert(it->second.cbegin(), it->second.cend());
    }
  }

  // Now we can build the final params map and argv.
  ParamMap final_params;
  // Inserting overrides first so they take precedence over defaults.
  final_params.insert(overrides_flat.cbegin(), overrides_flat.cend());
  final_params.insert(defaults_flat.cbegin(), defaults_flat.cend());

  return final_params;
}

void Cluster::partition(std::vector<std::set<int>> partitions) {
  // for every node in a partition, update the address of nodes outside
  // the partition to a non-existent unix socket. this effectively create a
  // virtual network partition.
  for (auto p : partitions) {
    auto same_parition_nodes = folly::join(",", p);

    for (auto& n : p) {
      nodes_[n]->cmd_args_.emplace(
          "--test-same-partition-nodes", same_parition_nodes);
      nodes_[n]->updateSetting(
          "test-same-partition-nodes", same_parition_nodes);
    }
  }

  updateNodesConfiguration(*getConfig()
                                ->getNodesConfiguration()
                                ->withIncrementedVersionAndTimestamp());
}
bool Cluster::applyInternalMaintenance(Client& client,
                                       node_index_t node_id,
                                       uint32_t shard_idx,
                                       const std::string& reason) {
  maintenance::MaintenanceDelta delta;
  delta.set_apply_maintenances({maintenance::MaintenanceLogWriter::
                                    buildMaintenanceDefinitionForRebuilding(
                                        ShardID(node_id, shard_idx), reason)});
  // write_to_maintenance_log will set err if it returns LSN_INVALID
  ld_info("Applying INTERNAL maintenance on N%u:S%u: %s",
          node_id,
          shard_idx,
          reason.c_str());
  return write_to_maintenance_log(client, delta) != LSN_INVALID;
}

std::string Cluster::applyMaintenance(thrift::AdminAPIAsyncClient& admin_client,
                                      node_index_t node_id,
                                      uint32_t shard_idx,
                                      const std::string& user,
                                      bool drain,
                                      bool force_restore,
                                      const std::string& reason,
                                      bool disable_sequencer) {
  thrift::MaintenanceDefinitionResponse resp;
  thrift::MaintenanceDefinition req;
  req.set_user(user);
  req.set_reason(reason);
  req.set_shard_target_state(
      drain ? thrift::ShardOperationalState::DRAINED
            : thrift::ShardOperationalState::MAY_DISAPPEAR);
  req.set_priority(thrift::MaintenancePriority::IMMINENT);
  if (disable_sequencer) {
    req.set_sequencer_nodes({mkNodeID(node_id)});
    req.set_sequencer_target_state(thrift::SequencingState::DISABLED);
  }
  req.set_force_restore_rebuilding(force_restore);
  req.set_shards({mkShardID(node_id, shard_idx)});
  req.set_force_restore_rebuilding(force_restore);
  admin_client.sync_applyMaintenance(resp, req);
  if (resp.get_maintenances().empty()) {
    throw std::runtime_error("Could not create requested maintenances on N" +
                             std::to_string(node_id) + ":S" +
                             std::to_string(shard_idx));
  }
  return *resp.get_maintenances()[0].get_group_id();
} // namespace IntegrationTestUtils

std::unique_ptr<Cluster>
ClusterFactory::create(const Configuration& source_config) {
  for (int outer_try = 0; outer_try < outerTries(); ++outer_try) {
    std::unique_ptr<Cluster> cluster = createOneTry(source_config);
    if (cluster) {
      return cluster;
    }
    // Cluster failed to start.  This may be because of an actual failure or a
    // port race (someone acquired the port between when we released it and
    // the new process tried to listen on it).  Retry in case it was a port
    // race.
  }

  ld_critical(
      "Failed to start LogDevice test cluster after %d tries", outerTries());
  throw std::runtime_error("Failed to start LogDevice test cluster");
}

Node::Node() {
  const char* alphabet = "0123456789abcdefghijklmnopqrstuvwxyz";
  uint32_t alphabet_size = (uint32_t)strlen(alphabet);
  while (server_id_.size() < 10) {
    server_id_ += alphabet[folly::Random::rand32(alphabet_size)];
  }
}

void Node::start() {
  folly::Subprocess::Options options;
  options.parentDeathSignal(SIGKILL); // kill children if test process dies

  // Make any tcp port that we reserved available to logdeviced.
  addrs_.owners.clear();

  // Without this, calling start() twice would causes a crash because
  // folly::Subprocess::~Subprocess asserts that the process is not running.
  if (isRunning()) {
    // The node is already started.
    return;
  }

  ld_info("Node N%d Command Line: %s",
          node_index_,
          folly::join(" ", commandLine()).c_str());

  ld_info("Starting node %d", node_index_);
  logdeviced_.reset(new folly::Subprocess(commandLine(), options));
  ld_info("Started node %d", node_index_);

  stopped_ = false;
}

void Node::restart(bool graceful, bool wait_until_available) {
  if (graceful) {
    int ret = shutdown();
    ld_check(0 == ret);
  } else {
    kill();
  }
  start();
  if (wait_until_available) {
    waitUntilAvailable();
  }
}

std::vector<std::string> Node::commandLine() const {
  std::vector<std::string> argv = {server_binary_};
  for (const auto& pair : cmd_args_) {
    argv.emplace_back(pair.first);
    if (pair.second.has_value()) {
      argv.emplace_back(pair.second.value());
    }
  }
  return argv;
}

int Node::shutdown() {
  if (isRunning()) {
    sendCommand("quit");
    return waitUntilExited();
  }
  return 0;
}

bool Node::isRunning() const {
  return logdeviced_ && logdeviced_->returnCode().running() &&
      logdeviced_->poll().running();
}

void Node::kill() {
  if (isRunning()) {
    ld_info("Killing node N%hd on %s",
            node_index_,
            addrs_.protocol.toString().c_str());
    logdeviced_->kill();
    logdeviced_->wait();
    ld_info("Killed node N%hd on %s",
            node_index_,
            addrs_.protocol.toString().c_str());
    stopped_ = true;
  }
  logdeviced_.reset();
}

void Node::wipeShard(uint32_t shard) {
  std::string shard_name = "shard" + folly::to<std::string>(shard);
  std::string db_path = getDatabasePath();
  auto shard_path = fs::path(db_path) / shard_name;
  for (fs::directory_iterator end_dir_it, it(shard_path); it != end_dir_it;
       ++it) {
    fs::remove_all(it->path());
  }
}

std::string Node::sendCommand(const std::string& command,
                              std::chrono::milliseconds command_timeout) const {
  auto client = createAdminClient();

  if (client == nullptr) {
    ld_debug("Failed to send admin command %s to node %d, because admin "
             "command client creation failed.",
             command.c_str(),
             node_index_);
    return "";
  }

  apache::thrift::RpcOptions rpc_options;
  rpc_options.setTimeout(command_timeout);

  thrift::AdminCommandRequest req;
  *req.request_ref() = command;

  thrift::AdminCommandResponse resp;
  try {
    client->sync_executeAdminCommand(rpc_options, resp, std::move(req));
  } catch (const folly::AsyncSocketException& e) {
    ld_debug("Failed to send admin command %s to node %d: %s",
             command.c_str(),
             node_index_,
             e.what());
    return "";
  } catch (const apache::thrift::transport::TTransportException& e) {
    ld_debug("Failed to send admin command %s to node %d: %s",
             command.c_str(),
             node_index_,
             e.what());
    return "";
  }
  std::string response = *resp.response_ref();

  // Strip the trailing END
  if (folly::StringPiece(response).endsWith("END\r\n")) {
    response.resize(response.size() - 5);
  }
  ld_debug(
      "Received response to \"%s\": %s", command.c_str(), response.c_str());
  if (response.substr(0, strlen("ERROR")) == "ERROR") {
    ld_warning("Command '%s' on N%d returned an error: %s",
               command.c_str(),
               (int)node_index_,
               response.c_str());
  }
  return response;
}

std::vector<std::map<std::string, std::string>>
Node::sendJsonCommand(const std::string& command) const {
  std::string response = sendCommand(command);
  return parseJsonAdminCommand(response, node_index_, command);
}

folly::SocketAddress Node::getAdminAddress() const {
  return addrs_.admin.getSocketAddress();
}

folly::Optional<test::ServerInfo>
Node::getServerInfo(std::chrono::milliseconds command_timeout) const {
  auto data = sendCommand("info --json", command_timeout);
  if (data.empty()) {
    return folly::Optional<test::ServerInfo>();
  }
  return folly::Optional<test::ServerInfo>(test::ServerInfo::fromJson(data));
}

int Node::waitUntilStarted(std::chrono::steady_clock::time_point deadline) {
  ld_info("Waiting for node %d to start", node_index_);
  bool died = false;

  // If we wait for a long time, dump the server's error log file to stderr to
  // help debug.
  auto t1 = std::chrono::steady_clock::now();
  bool should_dump_log = dbg::currentLevel >= dbg::Level::WARNING;

  auto started = [this, &died, &should_dump_log, t1]() {
    // no need to wait if the process is not even running
    died = !isRunning();
    if (died) {
      return true;
    }
    // To verify if the server has started, send an INFO admin command and see
    // if the server id matches what we expect.  This is to detect races where
    // two tests try to simultaneously claim the same port and hand it over to
    // a child process.
    // Catch any exceptions here and ignore them. eg: socket closed while
    // querying the server. logdeviced will eventually be restarted.
    // Use small timeout to make the above isRunning() check run more often in
    // case logdeviced is dead and some random other process is using its former
    // admin command TCP port.
    try {
      folly::Optional<test::ServerInfo> info =
          getServerInfo(std::chrono::seconds(1));
      if (info) {
        bool match = info->server_id == server_id_;
        if (!match) {
          ld_warning("Server process is running but its --server-id \"%s\" "
                     "does not match the expected \"%s\"",
                     info->server_id.c_str(),
                     server_id_.c_str());
        }
        return match;
      }
    } catch (...) {
    }

    auto t2 = std::chrono::steady_clock::now();
    if (should_dump_log && t2 - t1 > DEFAULT_TEST_TIMEOUT / 3) {
      ld_warning(
          "Server process is taking a long time to start responding to the "
          "'info' command.  Dumping its error log to help debug issues:");
      int rv = dump_file_to_stderr(getLogPath().c_str());
      if (rv == 0) {
        should_dump_log = false;
      }
    }

    return false;
  };
  int rv =
      wait_until(("node " + std::to_string(node_index_) + " starts").c_str(),
                 started,
                 deadline);
  if (died) {
    rv = -1;
  }
  if (rv != 0) {
    ld_info("Node %d failed to start. Dumping its error log", node_index_);
    dump_file_to_stderr(getLogPath().c_str());
  } else {
    ld_info("Node %d started", node_index_);
  }
  return rv;
}

bool Node::waitUntilShardState(
    thrift::AdminAPIAsyncClient& admin_client,
    shard_index_t shard,
    folly::Function<bool(const thrift::ShardState&)> predicate,
    const std::string& reason,
    std::chrono::steady_clock::time_point deadline) {
  int rv = wait_until(
      ("Shard N" + std::to_string(node_index_) + ":" + std::to_string(shard) +
       " matches predicate, " + reason)
          .c_str(),
      [&]() {
        return predicate(*get_shard_state(
            get_nodes_state(admin_client), ShardID(node_index_, shard)));
      },
      deadline);
  if (rv != 0) {
    ld_info(
        "Failed on waiting for shard state to finish for node %d", node_index_);
    return false;
  }
  return true;
}

bool Node::waitUntilInternalMaintenances(
    thrift::AdminAPIAsyncClient& admin_client,
    folly::Function<bool(const std::vector<thrift::MaintenanceDefinition>&)>
        predicate,
    const std::string& reason,
    std::chrono::steady_clock::time_point deadline) {
  thrift::MaintenancesFilter filter;
  thrift::MaintenanceDefinitionResponse resp;
  std::vector<std::string> groups;
  for (shard_index_t s = 0; s < num_db_shards_; ++s) {
    groups.push_back("N" + std::to_string(node_index_) + ":S" +
                     std::to_string(s));
  }
  filter.set_group_ids(groups);
  int rv = wait_until(
      ("Node " + std::to_string(node_index_) + " internal maintenances (" +
       toString(groups) + ") matches predicate, " + reason)
          .c_str(),
      [&]() {
        admin_client.sync_getMaintenances(resp, filter);
        return predicate(resp.get_maintenances());
      },
      deadline);
  if (rv != 0) {
    ld_info(
        "Failed on waiting for internal maintenances to finished for node %d",
        node_index_);
    return false;
  }
  return true;
}

lsn_t Node::waitUntilAllShardsFullyAuthoritative(
    std::shared_ptr<Client> client) {
  std::vector<ShardID> shards;
  for (shard_index_t s = 0; s < num_db_shards_; ++s) {
    shards.push_back(ShardID(node_index_, s));
  }
  return IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client, shards, AuthoritativeStatus::FULLY_AUTHORITATIVE, true);
}

lsn_t Node::waitUntilAllShardsAuthoritativeEmpty(
    std::shared_ptr<Client> client) {
  std::vector<ShardID> shards;
  for (shard_index_t s = 0; s < num_db_shards_; ++s) {
    shards.push_back(ShardID(node_index_, s));
  }
  return IntegrationTestUtils::waitUntilShardsHaveEventLogState(
      client, shards, AuthoritativeStatus::AUTHORITATIVE_EMPTY, true);
}

int Node::waitUntilKnownGossipState(
    node_index_t other_node_index,
    bool alive,
    std::chrono::steady_clock::time_point deadline) {
  if (!gossip_enabled_) {
    return 0;
  }

  const std::string key_expected =
      folly::to<std::string>("N", other_node_index);
  const std::string state_str = alive ? "ALIVE" : "DEAD";
  int rv = wait_until(("node " + std::to_string(node_index_) +
                       " learns through gossip that node " +
                       std::to_string(other_node_index) + " is " + state_str)
                          .c_str(),
                      [&]() { return gossipInfo()[key_expected] == state_str; },
                      deadline);
  if (rv == 0) {
    ld_info("Node %d transitioned to %s according to node %d",
            other_node_index,
            state_str.c_str(),
            node_index_);
  } else {
    ld_info(
        "Timed out waiting for node %d to see that node %d transitioned to %s",
        node_index_,
        other_node_index,
        state_str.c_str());
  }
  return rv;
}

int Node::waitUntilKnownGossipStatus(
    node_index_t other_node_index,
    uint8_t health_status,
    std::chrono::steady_clock::time_point deadline) {
  const std::string key_expected =
      folly::to<std::string>("N", other_node_index);
  const std::string status_str = toString(NodeHealthStatus(health_status));
  int rv = wait_until(
      ("node " + std::to_string(node_index_) +
       " learns through gossip that node " + std::to_string(other_node_index) +
       " is " + status_str)
          .c_str(),
      [&]() { return gossipStatusInfo()[key_expected] == status_str; },
      deadline);
  if (rv == 0) {
    ld_info("Node %d transitioned to %s according to node %d",
            other_node_index,
            status_str.c_str(),
            node_index_);
  } else {
    ld_info(
        "Timed out waiting for node %d to see that node %d transitioned to %s",
        node_index_,
        other_node_index,
        status_str.c_str());
  }
  return rv;
}

int Node::waitUntilAvailable(std::chrono::steady_clock::time_point deadline) {
  return waitUntilKnownGossipState(node_index_, /* alive */ true, deadline);
}

int Node::waitUntilHealthy(std::chrono::steady_clock::time_point deadline) {
  return waitUntilKnownGossipStatus(
      node_index_, NodeHealthStatus::HEALTHY, deadline);
}

void Node::waitUntilKnownDead(node_index_t other_node_index) {
  int rv = waitUntilKnownGossipState(other_node_index, /* alive */ false);
  ld_check(rv == 0);
}

int Node::waitForRecovery(logid_t log,
                          std::chrono::steady_clock::time_point deadline) {
  if (stopped_) {
    return 0;
  }

  // Wait for 'info sequencer' to output either:
  //  - last_released != LSN_INVALID, or
  //  - preempted_by.
  return wait_until(
      ("node " + std::to_string(node_index_) + " finishes recovery of log " +
       std::to_string(log.val_))
          .c_str(),
      [&]() {
        auto seq = sequencerInfo(log);
        if (seq.empty()) {
          // There is no sequencer for this log on that node.
          return true;
        }

        if (seq["State"] == "PREEMPTED") {
          // If sequencer was preempted, consider recovery done.
          return true;
        }

        const std::string last_released = "Last released";
        if (seq[last_released] == "" || seq[last_released] == "0") {
          return false;
        }

        const std::string epoch = "Epoch";
        epoch_t seq_epoch = epoch_t(folly::to<epoch_t::raw_type>(seq[epoch]));
        epoch_t last_release_epoch =
            lsn_to_epoch(folly::to<lsn_t>(seq[last_released]));

        if (seq_epoch != last_release_epoch) {
          return false;
        }

        const std::string meta_last_released = "Meta last released";
        if (seq[meta_last_released] == "" || seq[meta_last_released] == "0") {
          return false;
        }

        return true;
      },
      deadline);
}

int Node::waitUntilAllSequencersQuiescent(
    std::chrono::steady_clock::time_point deadline) {
  if (stopped_) {
    return 0;
  }

  return wait_until(
      folly::sformat(
          "node {} finishes all sequencer activation-related activities",
          node_index_)
          .c_str(),
      [&]() {
        auto s = stats();
        if (s.empty()) {
          // Node didn't reply to admin command. Keep trying.
          return false;
        }
        ld_check(s.count("sequencer_activity_in_progress"));
        return s.at("sequencer_activity_in_progress") == 0;
      },
      deadline);
}

std::unique_ptr<thrift::AdminAPIAsyncClient> Node::createAdminClient() const {
  folly::SocketAddress address = getAdminAddress();
  auto transport = folly::AsyncSocket::newSocket(
      folly::EventBaseManager::get()->getEventBase(), address);
  auto channel =
      apache::thrift::HeaderClientChannel::newChannel(std::move(transport));
  channel->setTimeout(5000);
  if (!channel->good()) {
    ld_debug("Couldn't create a thrift client for the Admin server for node "
             "%d. It might mean that the node is down.",
             node_index_);
    return nullptr;
  }
  return std::make_unique<thrift::AdminAPIAsyncClient>(std::move(channel));
}

int Node::waitUntilNodeStateReady() {
  waitUntilAvailable();
  auto admin_client = createAdminClient();
  return wait_until(
      "LogDevice started but we are waiting for the EventLog to be replayed",
      [&]() {
        try {
          thrift::NodesStateRequest req;
          thrift::NodesStateResponse resp;
          admin_client->sync_getNodesState(resp, req);
          return true;
        } catch (thrift::NodeNotReady& e) {
          ld_info("getNodesState thrown NodeNotReady exception. Node %d is not "
                  "ready yet",
                  node_index_);
          return false;
        } catch (apache::thrift::transport::TTransportException& ex) {
          ld_info("AdminServer is not fully started yet, connections are "
                  "failing to node %d. ex: %s",
                  node_index_,
                  ex.what());
          return false;
        } catch (std::exception& ex) {
          ld_critical("An exception in AdminClient that we didn't expect: %s",
                      ex.what());
          return false;
        }
      });
}

int Node::waitForPurge(logid_t log_id,
                       epoch_t epoch,
                       std::chrono::steady_clock::time_point deadline) {
  if (stopped_) {
    return false;
  }

  ld_info("Waiting for node %d to finish purging of log %lu upto epoch %u.",
          node_index_,
          log_id.val_,
          epoch.val_);
  epoch_t new_lce(0);
  // Wait for 'logstoragestate' to output last_released with its epoch higher
  // or equal than @param epoch
  int rv =
      wait_until(("node " + std::to_string(node_index_) +
                  " finishes purging of log " + std::to_string(log_id.val_) +
                  " upto epoch " + std::to_string(epoch.val_))
                     .c_str(),
                 [&]() {
                   auto log_state = logState(log_id);
                   auto it = log_state.find("Last Released");
                   if (it == log_state.end()) {
                     return false;
                   }
                   const auto& lr_str = it->second;
                   if (lr_str.empty()) {
                     return false;
                   }
                   new_lce = lsn_to_epoch(folly::to<lsn_t>(lr_str));
                   return new_lce >= epoch;
                 },
                 deadline);

  if (rv == 0) {
    ld_info("Node %d finished purging of log %lu to epoch %u",
            node_index_,
            log_id.val_,
            new_lce.val_);
  } else {
    ld_error("Timed out waiting for node %d to finish purging of "
             "log %lu to epoch %u",
             node_index_,
             log_id.val_,
             epoch.val_);
  }
  return rv;
}

int Node::waitUntilRSMSynced(const char* rsm,
                             lsn_t sync_lsn,
                             std::chrono::steady_clock::time_point deadline) {
  if (stopped_) {
    return false;
  }

  const int rv = wait_until(
      folly::sformat("node {} read {} up to {}",
                     node_index_,
                     rsm,
                     lsn_to_string(sync_lsn).c_str())
          .c_str(),
      [&]() {
        auto data = sendJsonCommand(folly::sformat("info {} --json", rsm));
        if (data.empty()) {
          return false;
        }

        // This is not a very nice usage of ld_check: it
        // assumes something about behavior of a different
        // process (logdeviced). The only excuse is that
        // this is only used in tests at the moment.
        ld_check(data[0].count("Propagated read ptr"));

        std::string s = data[0]["Propagated read ptr"];
        ld_check(!s.empty());
        return folly::to<lsn_t>(s) >= sync_lsn;
      },
      deadline);

  return rv;
}

int Node::waitUntilExited() {
  ld_info("Waiting for node %d to exit", node_index_);
  folly::ProcessReturnCode res;
  if (isRunning()) {
    res = logdeviced_->wait();
  } else {
    res = logdeviced_->returnCode();
  }
  ld_check(res.exited() || res.killed());
  if (res.killed()) {
    ld_warning("Node %d did not exit cleanly (signal %d)",
               node_index_,
               res.killSignal());
  } else {
    ld_info("Node %d exited cleanly", node_index_);
  }
  logdeviced_.reset();
  return res.killed() ? 128 + res.killSignal() : res.exitStatus();
}

void Node::suspend() {
  ld_info("Suspending node %d", node_index_);

  // Make sure the node doesn't hold any file locks while stopped.
  std::string response = sendCommand("pause_file_epoch_store");
  if (response.substr(0, 2) != "OK") {
    ld_error("Failed to pause_file_epoch_store on N%d: %s",
             node_index_,
             sanitize_string(response).c_str());
  }

  stopped_ = true;
  this->signal(SIGSTOP);
  // SIGSTOP is not immediate.  Wait until the process has stopped.
  siginfo_t infop;
  int rv = waitid(P_PID, logdeviced_->pid(), &infop, WSTOPPED);
  if (rv != 0) {
    ld_warning("waitid(pid=%d) failed with errno %d (%s)",
               int(logdeviced_->pid()),
               errno,
               strerror(errno));
  }
  ld_info("Suspended node %d", node_index_);
}

void Node::resume() {
  ld_info("Resuming node %d", node_index_);
  this->signal(SIGCONT);
  siginfo_t infop;
  int rv = waitid(P_PID, logdeviced_->pid(), &infop, WCONTINUED);
  if (rv != 0) {
    ld_warning("waitid(pid=%d) failed with errno %d (%s)",
               int(logdeviced_->pid()),
               errno,
               strerror(errno));
  }
  stopped_ = false;

  // Allow the node to use flock() again.
  std::string response = sendCommand("unpause_file_epoch_store");
  if (response.substr(0, 2) != "OK") {
    ld_error("Failed to unpause_file_epoch_store on N%d: %s",
             node_index_,
             sanitize_string(response).c_str());
  }

  ld_info("Resumed node %d", node_index_);
}

std::unique_ptr<ShardedLocalLogStore> Node::createLocalLogStore() {
  RocksDBSettings rocks_settings = create_default_settings<RocksDBSettings>();
  rocks_settings.allow_fallocate = false;
  rocks_settings.auto_create_shards = true;
  rocks_settings.partitioned = rocksdb_type_ == RocksDBType::PARTITIONED;
  // Tell logsdb to not create partitions automatically, in particular to
  // not create lots of partitions if we write a record with very old timestamp.
  rocks_settings.partition_duration_ = std::chrono::seconds(0);

  auto log_store = std::make_unique<ShardedRocksDBLocalLogStore>(
      getDatabasePath(),
      num_db_shards_,
      UpdateableSettings<RocksDBSettings>(rocks_settings),
      std::make_unique<RocksDBCustomiser>());

  log_store->init(create_default_settings<Settings>(),
                  UpdateableSettings<RebuildingSettings>(),
                  nullptr,
                  nullptr);
  return log_store;
}

void Node::corruptShards(std::vector<uint32_t> shards,
                         std::unique_ptr<ShardedLocalLogStore> sharded_store) {
  if (!sharded_store) {
    sharded_store = createLocalLogStore();
  }
  // Collect paths to RocksDB instances
  std::vector<std::string> paths;
  for (auto idx : shards) {
    RocksDBLogStoreBase* store =
        dynamic_cast<RocksDBLogStoreBase*>(sharded_store->getByIndex(idx));
    ld_check(store != nullptr);
    paths.push_back(store->getLocalDBPath().value());
  }
  sharded_store.reset(); // close DBs before corrupting them

  std::mt19937_64 rng(0xff00abcd);
  for (auto path : paths) {
    // Open all files in the RocksDB directory and overwrite them with
    // random data.  This should ensure that RocksDB fails to open the DB.
    using namespace boost::filesystem;
    for (recursive_directory_iterator it(path), end; it != end; ++it) {
      ld_check(!is_directory(*it));
      std::vector<char> junk(file_size(*it));
      std::generate(junk.begin(), junk.end(), rng);
      FILE* fp = fopen(it->path().c_str(), "w");
      ld_check(fp != nullptr);
      SCOPE_EXIT {
        fclose(fp);
      };
      int rv = fwrite(junk.data(), 1, junk.size(), fp);
      ld_check(rv == junk.size());
    }
  }
}

std::map<std::string, int64_t> Node::stats() const {
  return parse<int64_t>(sendCommand("stats2"), "STAT");
}

std::map<std::string, std::string> Node::logState(logid_t log_id) const {
  std::string command =
      "info log_storage_state --json --logid " + std::to_string(log_id.val_);
  auto data = sendJsonCommand(command);
  if (data.empty()) {
    // There is no sequencer for this log.
    return std::map<std::string, std::string>();
  }
  return data[0];
}

/*
 * Sends inject shard fault command to the node:
 * "inject shard_fault "
 *      "<shard#|a|all> "
 *      "<d|data|m|metadata|a|all|n|none> "
 *      "<r|read|w|write|a|all|n|none> "
 *      "<io_error|corruption|latency|none> "
 *      "[--single_shot] "
 *      "[--chance=PERCENT]"
 *      "[--latency=LATENCY_MS]";
 */
bool Node::injectShardFault(std::string shard,
                            std::string data_type,
                            std::string io_type,
                            std::string code,
                            bool single_shot,
                            folly::Optional<double> chance,
                            folly::Optional<uint32_t> latency_ms) {
  std::string cmd =
      folly::format(
          "inject shard_fault {} {} {} {}", shard, data_type, io_type, code)
          .str();
  if (single_shot) {
    cmd += " --single_shot";
  }
  if (chance.has_value()) {
    cmd += folly::format(" --chance={}", chance.value()).str();
  }
  if (latency_ms.has_value()) {
    cmd += folly::format(" --latency={}", latency_ms.value()).str();
  }
  cmd += " --force"; // Within tests, it's fine to inject errors on opt builds.
  auto reply = sendCommand(cmd);
  ld_check(reply.empty());
  return true;
}

void Node::gossipBlacklist(node_index_t node_id) const {
  auto reply = sendCommand("gossip blacklist " + std::to_string(node_id));
  ld_check(reply == "GOSSIP N" + std::to_string(node_id) + " BLACKLISTED\r\n");
}

void Node::gossipWhitelist(node_index_t node_id) const {
  auto reply = sendCommand("gossip whitelist " + std::to_string(node_id));
  ld_check(reply == "GOSSIP N" + std::to_string(node_id) + " WHITELISTED\r\n");
}

void Node::newConnections(bool accept) const {
  auto reply = sendCommand(std::string("newconnections ") +
                           (accept ? "accept" : "reject"));
  ld_check(reply.empty());
}

void Node::startRecovery(logid_t logid) const {
  auto logid_string = std::to_string(logid.val_);
  auto reply = sendCommand("startrecovery " + logid_string);
  ld_check_eq(reply,
              folly::format("Started recovery for logid {}, result success\r\n",
                            logid_string)
                  .str());
}

std::string Node::upDown(const logid_t logid) const {
  return sendCommand("up " + std::to_string(logid.val_));
}

std::map<std::string, std::string> Node::sequencerInfo(logid_t log_id) const {
  const std::string command =
      "info sequencers " + std::to_string(log_id.val_) + " --json";
  auto data = sendJsonCommand(command);
  if (data.empty()) {
    // There is no sequencer for this log.
    return std::map<std::string, std::string>();
  }
  return data[0];
}

std::map<std::string, std::string> Node::eventLogInfo() const {
  const std::string command = "info event_log --json";
  auto data = sendJsonCommand(command);
  if (data.empty()) {
    // This node does not seem to be reading the event log.
    return std::map<std::string, std::string>();
  }
  return data[0];
}

std::map<std::string, std::string> Node::logsConfigInfo() const {
  const std::string command = "info logsconfig_rsm --json";
  auto data = sendJsonCommand(command);
  if (data.empty()) {
    // This node does not seem to be reading the event log.
    return std::map<std::string, std::string>();
  }
  return data[0];
}

std::vector<std::map<std::string, std::string>> Node::socketInfo() const {
  const std::string command = "info sockets --json";
  auto data = sendJsonCommand(command);
  if (data.empty()) {
    return std::vector<std::map<std::string, std::string>>();
  }
  return data;
}

std::vector<std::map<std::string, std::string>> Node::partitionsInfo() const {
  const std::string command = "info partitions --spew --json";
  auto data = sendJsonCommand(command);
  if (data.empty()) {
    return std::vector<std::map<std::string, std::string>>();
  }
  return data;
}

std::map<std::string, std::string> Node::gossipState() const {
  return parseGossipState(sendCommand("info gossip"));
}

std::map<node_index_t, std::string>
Node::getRsmVersions(logid_t log_id, RsmVersionType rsm_type) const {
  std::map<node_index_t, std::string> res;
  std::string column_name;
  if (log_id == configuration::InternalLogs::CONFIG_LOG_DELTAS) {
    column_name = "logsconfig";
  } else if (log_id == configuration::InternalLogs::EVENT_LOG_DELTAS) {
    column_name = "eventlog";
  } else {
    ld_error("Not supported");
    return res;
  }
  if (rsm_type == RsmVersionType::IN_MEMORY) {
    column_name += " in-memory version";
  } else if (rsm_type == RsmVersionType::DURABLE) {
    column_name += " durable version";
  } else {
    ld_error("Invalid type");
    return res;
  }

  std::string command = "info rsm versions --json";
  auto data = sendJsonCommand(command);
  for (const auto& row : data) {
    const auto peer_id = row.find("Peer ID");
    const auto ver = row.find(column_name);
    if (peer_id == row.end() || ver == row.end()) {
      continue;
    }
    auto node_idx = folly::to<node_index_t>(peer_id->second);
    lsn_t ver_lsn = folly::to<lsn_t>(ver->second);
    res.emplace(node_idx, lsn_to_string(ver_lsn));
  }
  return res;
}

std::pair<std::string, std::string>
Node::getTrimmableVersion(logid_t rsm_log) const {
  return parseTrimmableVersion(sendCommand("info rsm get_trimmable_version " +
                                           std::to_string(rsm_log.val_)));
}

std::map<std::string, std::pair<std::string, uint64_t>>
Node::gossipCount() const {
  return parseGossipCount(sendCommand("info gossip"));
}

std::map<std::string, std::string> Node::gossipInfo() const {
  return parse<std::string>(sendCommand("info gossip"), "GOSSIP");
}

std::map<std::string, std::string> Node::gossipStatusInfo() const {
  std::map<std::string, std::string> out;
  auto cmd_result = sendCommand("info gossip --json");
  if (cmd_result == "") {
    return out;
  }
  auto obj = folly::parseJson(cmd_result);
  for (auto& state : obj["states"]) {
    out[state["node_id"].getString()] = state["health_status"].getString();
  }
  return out;
}

std::map<std::string, bool> Node::gossipStarting() const {
  std::map<std::string, bool> out;
  auto cmd_result = sendCommand("info gossip --json");
  if (cmd_result == "") {
    return out;
  }

  auto obj = folly::parseJson(cmd_result);
  for (auto& state : obj["states"]) {
    int is_starting = state["detector"]["starting"].getInt();
    out[state["node_id"].getString()] =
        (state["status"].getString() == "ALIVE" && is_starting);
  }
  return out;
}

std::map<std::string, bool> Node::gossipBoycottState() const {
  auto string_state = parseGossipBoycottState(sendCommand("info gossip"));
  std::map<std::string, bool> bool_state;
  std::transform(string_state.cbegin(),
                 string_state.cend(),
                 std::inserter(bool_state, bool_state.begin()),
                 [](const auto& entry) {
                   return std::make_pair<std::string, bool>(
                       std::string{entry.first}, entry.second == "BOYCOTTED");
                 });
  return bool_state;
}

void Node::resetBoycott(node_index_t node_index) const {
  sendCommand("boycott_reset " + std::to_string(node_index));
}

std::map<std::string, std::string> Node::domainIsolationInfo() const {
  return parse<std::string>(sendCommand("info gossip"), "DOMAIN_ISOLATION");
}

std::vector<std::map<std::string, std::string>>
Node::partitionsInfo(shard_index_t shard, int level) const {
  const std::string command =
      folly::format("info partitions {} --json --level {}", shard, level).str();
  auto data = sendJsonCommand(command);
  if (data.empty()) {
    return std::vector<std::map<std::string, std::string>>();
  }
  return data;
}

std::map<shard_index_t, std::string> Node::rebuildingStateInfo() const {
  auto data = sendJsonCommand("info shards --json");
  ld_check(!data.empty());
  std::map<shard_index_t, std::string> result;
  for (const auto& row : data) {
    const auto shard = row.find("Shard");
    const auto rebuilding_state = row.find("Rebuilding state");
    if (shard == row.end() || rebuilding_state == row.end()) {
      continue;
    }
    result.emplace(std::stoi(shard->second), rebuilding_state->second);
  }
  return result;
}

std::map<shard_index_t, RebuildingRangesMetadata> Node::dirtyShardInfo() const {
  auto data = sendJsonCommand("info shards --json --dirty-as-json");
  ld_check(!data.empty());
  std::map<shard_index_t, RebuildingRangesMetadata> result;
  for (const auto& row : data) {
    const auto shard = row.find("Shard");
    const auto dirty_state = row.find("Dirty State");
    if (shard == row.end() || dirty_state == row.end() ||
        dirty_state->second.empty() || dirty_state->second == "{}" ||
        dirty_state->second == "UNKNOWN") {
      continue;
    }
    try {
      folly::dynamic obj = folly::parseJson(dirty_state->second);
      RebuildingRangesMetadata rrm;
      if (!RebuildingRangesMetadata::fromFollyDynamic(obj, rrm)) {
        ld_check(false);
        continue;
      }
      result.emplace(std::stoi(shard->second), rrm);
    } catch (...) {
      ld_check(false);
    }
  }
  return result;
}

partition_id_t Node::createPartition(uint32_t shard) {
  std::string out = sendCommand("logsdb create " + std::to_string(shard));
  const std::string expected = "Created partition ";
  if (out.substr(0, expected.size()) != expected) {
    ld_error(
        "Failed to create partition on N%d: %s", (int)node_index_, out.c_str());
    return PARTITION_INVALID;
  }
  return std::stol(out.substr(expected.size()));
}

int Node::compact(logid_t logid) const {
  std::string command_str = std::string("compact ") +
      (logid == LOGID_INVALID ? "--all" : std::to_string(logid.val_));
  std::string stdout = sendCommand(command_str);

  std::vector<std::string> lines;
  folly::split("\r\n", stdout, lines, /* ignoreEmpty */ true);

  std::string success_token("Successfully");
  if (!lines.empty() &&
      !lines[0].compare(0, success_token.size(), success_token)) {
    return 0;
  }
  return -1;
}

void Node::updateSetting(std::string name, std::string value) {
  sendCommand(folly::format("set {} {} --ttl max", name, value).str());
  // Assert that the setting was successfully changed
  auto data = sendJsonCommand("info settings --json");
  ld_check(!data.empty());
  for (int i = 1; i < data.size(); i++) {
    auto& row = data[i];
    if (row["Name"] == name) {
      if (row["Current Value"] != value) {
        // Maybe setting didn't update because something is broken.
        // Or maybe you set it to "1,2,3" but 'info settings'
        // printed it as "1, 2, 3".
        // Let's be conservative and crash.
        ld_critical(
            "Unexpected value in \"info settings\" on N%d after updating "
            "setting %s: expected %s, found %s. This is either a bug in "
            "settings or a benign formatting difference. If it's the latter "
            "please change your test to use canonical formatting.",
            (int)node_index_,
            name.c_str(),
            value.c_str(),
            row["Current Value"].c_str());
        std::abort();
      }
      return;
    }
  }
  ld_check(false);
}

void Node::unsetSetting(std::string name) {
  sendCommand("unset " + name);
  // Assert that the setting was successfully changed
  auto data = sendJsonCommand("info settings --json");
  ld_check(!data.empty());
  for (int i = 1; i < data.size(); i++) {
    auto& row = data[i];
    if (row["Name"] == name) {
      ld_check_eq(row["From Admin Cmd"], "");
      return;
    }
  }
  ld_check(false);
}

namespace {

class StaticSequencerLocatorFactory : public SequencerLocatorFactory {
  std::string identifier() const override {
    return "static";
  }

  std::string displayName() const override {
    return "Static sequencer placement";
  }
  std::unique_ptr<SequencerLocator>
  operator()(std::shared_ptr<UpdateableConfig> config) override {
    return std::make_unique<StaticSequencerLocator>(std::move(config));
  }
};

} // namespace

void Cluster::populateClientSettings(std::unique_ptr<ClientSettings>& settings,
                                     bool use_file_based_ncs) const {
  if (!settings) {
    settings.reset(ClientSettings::create());
  }

  // If we're not using the default hash based sequencer placement, we will need
  // to hijack the client plugins and provide a different sequencer locator.
  if (!hash_based_sequencer_assignment_) {
    ClientSettingsImpl* impl_settings =
        static_cast<ClientSettingsImpl*>(settings.get());

    PluginVector seed_plugins = getClientPluginProviders();
    // assume N0 runs sequencers for all logs
    seed_plugins.emplace_back(
        std::make_unique<StaticSequencerLocatorFactory>());

    impl_settings->setPluginRegistry(
        std::make_shared<PluginRegistry>(std::move(seed_plugins)));
  }

  int rv;
  // Instantiate StatsHolder in tests so that counters can be queried
  rv = settings->set("client-test-force-stats", "true");
  ld_check(rv == 0);
  // But disable publishing
  rv = settings->set("stats-collection-interval", "-1s");
  ld_check(rv == 0);
  // We don't need a ton of workers in the test client
  if (!settings->get("num-workers") ||
      settings->get("num-workers").value() == "cores") {
    rv = settings->set("num-workers", "5");
    ld_check(rv == 0);
  }
  if (!settings->isOverridden("node-stats-send-period")) {
    // Make sure node stats would be sent in most tests for better coverage
    rv = settings->set("node-stats-send-period", "100ms");
    ld_check(rv == 0);
  }
  if (!settings->isOverridden("ssl-ca-path") && !no_ssl_address_) {
    // Set CA cert path so the client can verify the server's identity.
    // Most clients will already have this settings set via the config, but
    // we have a couple of tests that load custom configs, so this ensures it's
    // set in those
    rv = settings->set(
        "ssl-ca-path", TEST_SSL_FILE("logdevice_test_valid_ca.cert"));
    ld_check(rv == 0);
  }

  {
    // Enable NCM on clients
    if (!settings->isOverridden("enable-nodes-configuration-manager")) {
      rv = settings->set("enable-nodes-configuration-manager", "true");
      ld_check(rv == 0);
    }

    if (settings->isOverridden("nodes-configuration-seed-servers")) {
      // TODO(mbassem): Remove this limitation when client settings have higher
      // precedence than config.
      ld_error("Due to a limitation in the test frameowrk, you can't override "
               "the nodes configuration seed for now. This is mainly because "
               "the seed is defined in the config and config settings have "
               "higher precedence over client settings as of now.");
      ld_check(false);
    }

    if (use_file_based_ncs) {
      rv = settings->set("admin-client-capabilities", "true");
      ld_check(rv == 0);
      rv = settings->set("nodes-configuration-file-store-dir", getNCSPath());
      ld_check(rv == 0);
    }

    if (!settings->isOverridden(
            "use-nodes-configuration-manager-nodes-configuration")) {
      rv = settings->set(
          "use-nodes-configuration-manager-nodes-configuration",
          nodes_configuration_sot_ == NodesConfigurationSourceOfTruth::NCM
              ? "true"
              : "false");
      ld_check(rv == 0);
    }
  }
}

std::shared_ptr<Client>
Cluster::createClient(std::chrono::milliseconds timeout,
                      std::unique_ptr<ClientSettings> settings,
                      std::string credentials,
                      bool use_file_based_ncs) {
  populateClientSettings(settings, use_file_based_ncs);
  auto client = ClientFactory()
                    .setClusterName(cluster_name_)
                    .setTimeout(timeout)
                    .setClientSettings(std::move(settings))
                    .setCredentials(credentials)
                    .create(config_path_);
  created_clients_.push_back(client);
  return client;
}

std::unique_ptr<EpochStore> Cluster::createEpochStore() {
  static InlineRequestPoster inline_request_poster{};
  return std::make_unique<FileEpochStore>(
      epoch_store_path_,
      RequestExecutor(&inline_request_poster),
      folly::none,
      getConfig()->updateableNodesConfiguration());
}

void Cluster::setStartingEpoch(logid_t log_id,
                               epoch_t epoch,
                               epoch_t last_expected_epoch) {
  auto epoch_store = createEpochStore();
  Semaphore semaphore;

  if (last_expected_epoch == EPOCH_INVALID) {
    // either expecting EPOCH_MIN + 1 or unprovisioned data, depending on the
    // test configuration
    epoch_store->readMetaData(log_id,
                              [&semaphore, &last_expected_epoch](
                                  Status status,
                                  logid_t /*log_id*/,
                                  std::unique_ptr<EpochMetaData> info,
                                  std::unique_ptr<EpochStoreMetaProperties>) {
                                if (status == E::OK) {
                                  ld_assert(info != nullptr);
                                  ld_assert_eq(
                                      EPOCH_MIN.val() + 1, info->h.epoch.val());
                                  last_expected_epoch = EPOCH_MIN;
                                } else {
                                  ld_assert_eq(E::NOTFOUND, status);
                                }
                                semaphore.post();
                              });
    semaphore.wait();
  }

  for (epoch_t e = epoch_t(last_expected_epoch.val_ + 1); e < epoch; ++e.val_) {
    epoch_store->createOrUpdateMetaData(
        log_id,
        std::make_shared<EpochMetaDataUpdateToNextEpoch>(
            EpochMetaData::Updater::Options().setProvisionIfEmpty(),
            getConfig()->get(),
            getConfig()->getNodesConfiguration()),
        [&semaphore, e](Status status,
                        logid_t,
                        std::unique_ptr<EpochMetaData> info,
                        std::unique_ptr<EpochStoreMetaProperties>) {
          ld_assert_eq(E::OK, status);
          ld_assert(info != nullptr);
          ld_assert_eq(e.val() + 1, info->h.epoch.val());
          semaphore.post();
        },
        MetaDataTracer());
    semaphore.wait();
  }
}

std::unique_ptr<MetaDataProvisioner> Cluster::createMetaDataProvisioner() {
  auto fn = [this](node_index_t nid) -> std::shared_ptr<ShardedLocalLogStore> {
    return getNode(nid).createLocalLogStore();
  };
  return std::make_unique<MetaDataProvisioner>(
      createEpochStore(), getConfig(), fn);
}

int Cluster::replaceViaAdminServer(thrift::AdminAPIAsyncClient& admin_client,
                                   node_index_t index,
                                   bool defer_start) {
  ld_info("Replacing node %d", (int)index);
  thrift::NodesFilter filter;
  thrift::NodeID node;
  node.set_node_index(index);
  filter.set_node(node);
  // Kill the existing node and wipe its data.
  for (int outer_try = 0; outer_try < outer_tries_; ++outer_try) {
    auto current_generation =
        getConfig()->getNodesConfiguration()->getNodeGeneration(index);
    nodes_.at(index).reset();
    if (hasStorageRole(index)) {
      ld_check(getNodeReplacementCounter(index) ==
               getConfig()->getNodesConfiguration()->getNodeGeneration(index));
    }
    // Bump the node generation
    {
      thrift::BumpGenerationRequest req;
      req.set_node_filters({filter});
      thrift::BumpGenerationResponse resp;
      admin_client.sync_bumpNodeGeneration(resp, req);
      current_generation++;
      if (resp.bumped_nodes_ref()->size() != 1) {
        ld_error(
            "Failed to find the node %d in the nodes configuration.", index);
        return -1;
      }
      ld_info(
          "Node %d generation is bumped at nodes config version %s",
          index,
          std::to_string(resp.get_new_nodes_configuration_version()).c_str());
      waitForServersAndClientsToProcessNodesConfiguration(
          vcs_config_version_t(resp.get_new_nodes_configuration_version()));
      // bump the internal node replacement counter
      setNodeReplacementCounter(index, current_generation);
    }

    // Update the addresses
    std::vector<ServerAddresses> addrs;
    if (pickAddressesForServers(std::vector<node_index_t>{index},
                                use_tcp_,
                                tcp_host_,
                                root_path_,
                                node_replacement_counters_,
                                addrs) != 0) {
      return -1;
    }

    auto nodes_config = getConfig()->getNodesConfiguration();
    thrift::NodeConfig new_config;

    {
      auto sd = nodes_config->getNodeServiceDiscovery(index);
      ld_check(sd);
      auto new_sd = *sd;
      addrs[0].toNodeConfig(new_sd, !no_ssl_address_);

      nodes_config = nodes_config->applyUpdate(
          NodesConfigurationTestUtil::setNodeAttributesUpdate(
              index, std::move(new_sd), folly::none, folly::none));
      ld_check(nodes_config);

      fillNodeConfig(new_config, index, *nodes_config);
    }
    // Sending the update request
    thrift::UpdateSingleNodeRequest update;
    update.set_node_to_be_updated(mkNodeID(index));
    update.set_new_config(new_config);
    {
      thrift::UpdateNodesRequest req;
      thrift::UpdateNodesResponse resp;
      req.set_node_requests({std::move(update)});
      admin_client.sync_updateNodes(resp, req);
      if (resp.updated_nodes_ref()->size() != 1) {
        ld_error("NodesConfig update failed to find the node %d", index);
        return -1;
      }
      // Wait for new config
      waitForServersAndClientsToProcessNodesConfiguration(
          vcs_config_version_t(resp.get_new_nodes_configuration_version()));
    }
    nodes_[index] = createNode(index, std::move(addrs[0]));
    if (defer_start) {
      return 0;
    }
    if (start({index}) == 0) {
      return 0;
    }
  }
  return -1;
}

int Cluster::replace(node_index_t index, bool defer_start) {
  ld_debug("replacing node %d", (int)index);

  if (hasStorageRole(index)) {
    ld_check(getNodeReplacementCounter(index) ==
             getConfig()->getNodesConfiguration()->getNodeGeneration(index));
  }

  for (int outer_try = 0, gen = getNodeReplacementCounter(index) + 1;
       outer_try < outer_tries_;
       ++outer_try, ++gen) {
    // Kill current node and erase its data
    nodes_.at(index).reset();

    // bump the internal node replacement counter
    setNodeReplacementCounter(index, gen);

    std::vector<ServerAddresses> addrs;
    if (pickAddressesForServers(std::vector<node_index_t>{index},
                                use_tcp_,
                                tcp_host_,
                                root_path_,
                                node_replacement_counters_,
                                addrs) != 0) {
      return -1;
    }

    auto nodes_config = getConfig()->getNodesConfiguration();

    {
      auto sd = nodes_config->getNodeServiceDiscovery(index);
      ld_check(sd);
      auto new_sd = *sd;
      addrs[0].toNodeConfig(new_sd, !no_ssl_address_);

      folly::Optional<configuration::nodes::StorageNodeAttribute>
          new_storage_attrs;

      if (hasStorageRole(index)) {
        // only bump the config generation if the node has storage role

        auto storage_cfg = nodes_config->getNodeStorageAttribute(index);
        ld_check(storage_cfg);

        new_storage_attrs = *storage_cfg;
        new_storage_attrs->generation = gen;
      }

      nodes_config = nodes_config->applyUpdate(
          NodesConfigurationTestUtil::setNodeAttributesUpdate(
              index,
              std::move(new_sd),
              folly::none,
              std::move(new_storage_attrs)));
      ld_check(nodes_config);
    }

    // Update config on disk so that other nodes become aware of the swap as
    // soon as possible
    int rv = updateNodesConfiguration(*nodes_config);
    if (rv != 0) {
      return -1;
    }

    nodes_[index] = createNode(index, std::move(addrs[0]));
    if (defer_start) {
      return 0;
    }
    if (start({index}) == 0) {
      return 0;
    }
  }

  ld_error("Failed to replace");
  return -1;
}

int Cluster::bumpGeneration(thrift::AdminAPIAsyncClient& admin_client,
                            node_index_t index) {
  auto current_generation =
      getConfig()->getNodesConfiguration()->getNodeGeneration(index);
  thrift::NodesFilter filter;
  thrift::NodeID node;
  node.set_node_index(index);
  filter.set_node(node);
  thrift::BumpGenerationRequest req;
  req.set_node_filters({filter});
  thrift::BumpGenerationResponse resp;
  admin_client.sync_bumpNodeGeneration(resp, req);
  current_generation++;
  if (resp.bumped_nodes_ref()->size() != 1) {
    ld_error("Failed to find the node %d in the nodes configuration.", index);
    return -1;
  }
  waitForServersAndClientsToProcessNodesConfiguration(
      vcs_config_version_t(resp.get_new_nodes_configuration_version()));
  // bump the internal node replacement counter
  setNodeReplacementCounter(index, current_generation);
  return 0;
}

int Cluster::updateNodeAttributes(node_index_t index,
                                  configuration::StorageState storage_state,
                                  int sequencer_weight,
                                  folly::Optional<bool> enable_sequencing) {
  static const auto from_legacy_storage_state =
      [](configuration::StorageState storage_state) {
        switch (storage_state) {
          case configuration::StorageState::READ_WRITE:
            return membership::StorageState::READ_WRITE;
          case configuration::StorageState::READ_ONLY:
            return membership::StorageState::READ_ONLY;
          case configuration::StorageState::DISABLED:
            return membership::StorageState::NONE;
        }
        ld_check(false);
        return membership::StorageState::INVALID;
      };
  ld_info("Updating attributes of N%d: storage_state %s, sequencer weight %d, "
          "enable_sequencing %s",
          (int)index,
          storageStateToString(storage_state).c_str(),
          sequencer_weight,
          enable_sequencing.has_value()
              ? enable_sequencing.value() ? "true" : "false"
              : "unchanged");

  auto nodes_config = getConfig()->getNodesConfiguration();

  if (!nodes_config->isNodeInServiceDiscoveryConfig(index)) {
    ld_error("No such node: %d", (int)index);
    ld_check(false);
    return -1;
  }

  if (nodes_config->isSequencerNode(index)) {
    nodes_config = nodes_config->applyUpdate(
        NodesConfigurationTestUtil::setSequencerWeightUpdate(
            *nodes_config, {index}, sequencer_weight));

    if (enable_sequencing.has_value()) {
      nodes_config = nodes_config->applyUpdate(
          NodesConfigurationTestUtil::setSequencerEnabledUpdate(
              *nodes_config, {index}, enable_sequencing.value()));
    }
  }

  if (nodes_config->isStorageNode(index)) {
    nodes_config = nodes_config->applyUpdate(
        NodesConfigurationTestUtil::setStorageMembershipUpdate(
            *nodes_config,
            {ShardID(index, -1)},
            from_legacy_storage_state(storage_state),
            folly::none));
  }

  int rv = updateNodesConfiguration(*nodes_config);
  if (rv != 0) {
    return -1;
  }
  return 0;
}

void Cluster::waitForServersAndClientsToProcessNodesConfiguration(
    membership::MembershipVersion::Type version) {
  auto server_check = [this, version]() {
    for (auto& [_, node] : nodes_) {
      if (node && !node->stopped_ && node->isRunning()) {
        auto stats = node->stats();
        auto version_itr =
            stats.find("nodes_configuration_manager_published_version");
        if (version_itr == stats.end()) {
          return false;
        }
        auto published_version =
            membership::MembershipVersion::Type(version_itr->second);
        if (published_version < version) {
          return false;
        }
      }
    }
    return true;
  };

  auto client_check = [this, version]() {
    for (const auto& client_ptr : created_clients_) {
      auto client = client_ptr.lock();
      if (client == nullptr) {
        continue;
      }
      auto client_impl = dynamic_cast<ClientImpl*>(client.get());
      if (client_impl->getConfig()->getNodesConfiguration()->getVersion() <
          version) {
        return false;
      }
    }
    return true;
  };

  auto config_check = [this, version]() {
    if (config_->getNodesConfiguration() == nullptr) {
      return true;
    }
    return config_->getNodesConfiguration()->getVersion() >= version;
  };

  wait_until(
      folly::sformat("nodes config version procssed >= {}", version.val())
          .c_str(),
      [&]() { return server_check() && client_check() && config_check(); });
}

void Cluster::waitForServersToPartiallyProcessConfigUpdate() {
  auto check = [this]() {
    std::shared_ptr<const Configuration> our_config = config_->get();
    const std::string expected_text = our_config->toString() + "\r\n";
    for (auto& it : nodes_) {
      if (it.second && it.second->logdeviced_ && !it.second->stopped_) {
        std::string node_text = it.second->sendCommand("info config");
        if (node_text != expected_text) {
          ld_info("Waiting for N%d:%d to pick up the most recent config",
                  it.first,
                  getNodeReplacementCounter(it.first));
          return false;
        }
      }
    }
    return true;
  };
  wait_until("config update", check);
}

int Cluster::waitForRecovery(std::chrono::steady_clock::time_point deadline) {
  std::shared_ptr<const Configuration> config = config_->get();
  const auto& logs = config->localLogsConfig();
  ld_debug("Waiting for recovery of %lu data logs.", logs->size());

  for (auto& it : nodes_) {
    node_index_t idx = it.first;
    if (!config->getNodesConfiguration()
             ->getSequencerMembership()
             ->isSequencingEnabled(idx)) {
      continue;
    }

    for (auto it = logs->logsBegin(); it != logs->logsEnd(); ++it) {
      logid_t log(it->first);
      int rv = getNode(idx).waitForRecovery(log, deadline);

      if (rv) {
        return -1;
      }
    }
  }

  return 0;
}

int Cluster::waitUntilAllSequencersQuiescent(
    std::chrono::steady_clock::time_point deadline) {
  std::shared_ptr<const Configuration> config = config_->get();
  for (auto& it : nodes_) {
    node_index_t idx = it.first;
    if (!config->getNodesConfiguration()
             ->getSequencerMembership()
             ->isSequencingEnabled(idx)) {
      continue;
    }

    if (getNode(idx).waitUntilAllSequencersQuiescent(deadline) != 0) {
      return -1;
    }
  }

  return 0;
}

int Cluster::waitUntilAllStartedAndPropagatedInGossip(
    folly::Optional<std::set<node_index_t>> nodes,
    std::chrono::steady_clock::time_point deadline) {
  if (!nodes.has_value()) {
    nodes.emplace();
    for (auto& it : nodes_) {
      if (!it.second->stopped_) {
        nodes->insert(it.first);
      }
    }
  }

  for (auto& it : nodes_) {
    if (!nodes->count(it.first)) {
      continue;
    }

    int rv = wait_until(
        folly::sformat(
            "N{} sees that {} are alive", it.first, toString(nodes.value()))
            .c_str(),
        [&] {
          auto cmd_result = it.second->sendCommand("info gossip --json");
          if (cmd_result == "") {
            return false;
          }
          auto obj = folly::parseJson(cmd_result);
          if (obj["stable_state"] != "true") {
            // The node needs to receive more gossip messages.
            return false;
          }
          for (auto& state : obj["states"]) {
            node_index_t idx =
                folly::to<node_index_t>(state["node_id"].getString().substr(1));
            bool alive = state["status"].getString() == "ALIVE";
            bool starting = state["detector"]["starting"].getInt() == 1;
            // This is a workaround for a quirk in FailureDetector: it may mark
            // the node as ALIVE based on GetClusterState request, then soon
            // mark it as DEAD if we're unlucky enough to not receive enough
            // gossip messages to declare it alive.
            bool gossiped_recently =
                state["detector"]["gossip"].getInt() < 1000;
            bool expected_alive = nodes->count(idx);
            if (expected_alive != alive ||
                (expected_alive && (starting || !gossiped_recently))) {
              return false;
            }
          }
          return true;
        },
        deadline);
    if (rv != 0) {
      return rv;
    }
  }

  return 0;
}

int Cluster::waitUntilAllAvailable(
    std::chrono::steady_clock::time_point deadline) {
  int rv = 0;
  for (auto& it : getNodes()) {
    node_index_t idx = it.first;
    rv |= getNode(idx).waitUntilAvailable(deadline);
  }
  return rv;
}

int Cluster::waitUntilAllHealthy(
    std::chrono::steady_clock::time_point deadline) {
  int rv = 0;
  for (auto& it : getNodes()) {
    node_index_t idx = it.first;
    rv |= getNode(idx).waitUntilHealthy(deadline);
  }
  return rv;
}

int Cluster::waitUntilRSMSynced(
    const char* rsm,
    lsn_t sync_lsn,
    std::vector<node_index_t> nodes,
    std::chrono::steady_clock::time_point deadline) {
  if (nodes.empty()) {
    for (auto& kv : nodes_) {
      nodes.push_back(kv.first);
    }
  }
  for (int n : nodes) {
    int rv = getNode(n).waitUntilRSMSynced(rsm, sync_lsn, deadline);
    if (rv != 0) {
      return -1;
    }
  }

  return 0;
}

int Cluster::waitForMetaDataLogWrites(
    std::chrono::steady_clock::time_point deadline) {
  std::shared_ptr<const Configuration> config = config_->get();
  const auto& logs = config->localLogsConfig();

  auto check = [&]() {
    const std::string metadata_log_written = "Metadata log written";
    for (auto log_it = logs->logsBegin(); log_it != logs->logsEnd(); ++log_it) {
      logid_t log(log_it->first);
      epoch_t last_epoch{LSN_INVALID};
      epoch_t last_written_epoch{LSN_INVALID};
      for (auto& kv : nodes_) {
        node_index_t idx = kv.first;
        if (!config->getNodesConfiguration()
                 ->getSequencerMembership()
                 ->isSequencingEnabled(idx)) {
          continue;
        }
        auto seq = kv.second->sequencerInfo(log);
        epoch_t epoch = epoch_t(atoi(seq["Epoch"].c_str()));
        last_epoch = std::max(last_epoch, epoch);
        if (seq[metadata_log_written] == "1") {
          last_written_epoch = std::max(last_written_epoch, epoch);
        }
      }
      if (last_epoch > last_written_epoch) {
        // The last activated sequencer has unwritten metadata
        return false;
      }
    }
    return true;
  };

  std::string msg = "metadata log records of " +
      folly::to<std::string>(logs->getLogMap().size()) +
      " data logs are written.";
  return wait_until(msg.c_str(), check, deadline);
}

int Cluster::waitUntilGossip(bool alive,
                             uint64_t targetNode,
                             std::set<uint64_t> nodesToSkip,
                             std::chrono::steady_clock::time_point deadline) {
  for (const auto& it : getNodes()) {
    if ((!alive && it.first == targetNode) ||
        nodesToSkip.count(it.first) != 0 || it.second->stopped_) {
      continue;
    }
    int rv = it.second->waitUntilKnownGossipState(targetNode, alive, deadline);
    if (rv != 0) {
      return rv;
    }
  }
  return 0;
}

int Cluster::waitUntilGossipStatus(
    uint8_t health_status,
    uint64_t targetNode,
    std::set<uint64_t> nodesToSkip,
    std::chrono::steady_clock::time_point deadline) {
  for (const auto& it : getNodes()) {
    if (((health_status == 0 || health_status == 3) &&
         it.first == targetNode) ||
        nodesToSkip.count(it.first) != 0 || it.second->stopped_) {
      continue;
    }
    int rv = it.second->waitUntilKnownGossipStatus(
        targetNode, health_status, deadline);
    if (rv != 0) {
      return rv;
    }
  }
  return 0;
}

int Cluster::waitUntilNoOneIsInStartupState(
    folly::Optional<std::set<uint64_t>> nodes,
    std::chrono::steady_clock::time_point deadline) {
  if (!nodes.has_value()) {
    nodes = std::set<uint64_t>();
    for (const auto& [nid, _] : getNodes()) {
      nodes.value().insert(nid);
    }
  }

  for (auto n : nodes.value()) {
    int res = getNode(n).waitUntilAvailable(deadline);
    if (res != 0) {
      return res;
    }
  }

  return wait_until("Nobody is starting", [&]() {
    for (const auto& [n, _] : getNodes()) {
      auto res = getNode(n).gossipStarting();
      for (auto nid : nodes.value()) {
        auto key = folly::to<std::string>("N", nid);
        if (res.find(key) != res.end() && res[key]) {
          return false;
        }
      }
    }
    return true;
  });
}

int Cluster::waitUntilAllClientsPickedConfig(
    const std::string& serialized_config) {
  return wait_until("Config update picked up", [&] {
    for (const auto& client_ptr : created_clients_) {
      auto client = client_ptr.lock();
      if (client == nullptr) {
        continue;
      }
      auto client_impl = dynamic_cast<ClientImpl*>(client.get());
      if (client_impl->getConfig()->get()->toString() != serialized_config) {
        return false;
      }
    }
    return true;
  });
}

bool Cluster::isGossipEnabled() const {
  // Assumes gossip is always set in the config regardless of its value.
  return config_->getServerConfig()->getServerSettingsConfig().at(
             "gossip-enabled") == "true";
}

int Cluster::checkConsistency(argv_t additional_args) {
  folly::Subprocess::Options options;
  options.parentDeathSignal(SIGKILL); // kill children if test process dies

  std::string checker_path = findBinary(CHECKER_PATH);
  if (checker_path.empty()) {
    return -1;
  }

  argv_t argv = {
      checker_path,
      "--config-path",
      config_path_,
      "--loglevel",
      dbg::loglevelToString(dbg::currentLevel),
      "--report-errors",
      "all",
  };

  argv.insert(argv.end(), additional_args.begin(), additional_args.end());

  auto proc = std::make_unique<folly::Subprocess>(argv, options);
  auto status = proc->wait();
  if (!status.exited()) {
    ld_error("checker did not exit properly: %s", status.str().c_str());
    return -1;
  }

  if (status.exitStatus() == 0) {
    return 0;
  } else {
    ld_error("checker exited with error %i", status.exitStatus());
    return -1;
  }
}

Cluster::~Cluster() {
  nodes_.clear();
  config_.reset();

  if (getenv_switch("LOGDEVICE_TEST_LEAVE_DATA")) {
    ld_info("LOGDEVICE_TEST_LEAVE_DATA environment variable was set.  Leaving "
            "data in: %s",
            root_path_.c_str());
  }
}

std::string ClusterFactory::actualServerBinary() const {
  const char* envpath = getenv("LOGDEVICE_TEST_BINARY");
  if (envpath != nullptr) {
    return envpath;
  }
  std::string relative_to_build_out =
      server_binary_.value_or(defaultLogdevicedPath());
  return findBinary(relative_to_build_out);
}

std::string ClusterFactory::actualAdminServerBinary() const {
  const char* envpath = getenv("LOGDEVICE_ADMIN_SERVER_BINARY");
  if (envpath != nullptr) {
    return envpath;
  }
  std::string relative_to_build_out =
      admin_server_binary_.value_or(defaultAdminServerPath());
  return findBinary(relative_to_build_out);
}

void ClusterFactory::setInternalLogAttributes(const std::string& name,
                                              logsconfig::LogAttributes attrs) {
  auto log_group_node = internal_logs_.insert(name, attrs);
  ld_check(log_group_node);
}

static void noop_signal_handler(int) {}

void maybe_pause_for_gdb(Cluster& cluster,
                         const std::vector<node_index_t>& indices) {
  if (!getenv_switch("LOGDEVICE_TEST_PAUSE_FOR_GDB")) {
    return;
  }

  fprintf(stderr,
          "\nLOGDEVICE_TEST_PAUSE_FOR_GDB environment variable was set.  "
          "Pausing to allow human to debug the system.\n\n");
  fprintf(stderr, "Attach GDB to server processes with:\n");
  for (int i : indices) {
    fprintf(stderr,
            "  Node N%d:%d: gdb %s %d\n",
            i,
            cluster.getNodeReplacementCounter(i),
            cluster.getNode(i).server_binary_.c_str(),
            cluster.getNode(i).logdeviced_->pid());
  }

  unsigned alarm_saved = alarm(0);

  fprintf(stderr, "\nResume this process with:\n");
  fprintf(stderr, "  kill -usr2 %d\n\n", getpid());
  struct sigaction act, oldact;
  act.sa_handler = noop_signal_handler;
  sigemptyset(&act.sa_mask);
  act.sa_flags = 0;
  sigaction(SIGUSR2, &act, &oldact);
  pause();
  sigaction(SIGUSR2, &oldact, nullptr);

  alarm(alarm_saved);
}

static lsn_t writeToEventlog(Client& client, EventLogRecord& event) {
  logid_t event_log_id = configuration::InternalLogs::EVENT_LOG_DELTAS;

  ld_info("Writing to event log: %s", event.describe().c_str());

  // Retry for at most 30s to avoid test failures due to transient failures
  // writing to the event log.
  std::chrono::steady_clock::time_point deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds{30};

  const int size = event.toPayload(nullptr, 0);
  ld_check(size > 0);
  void* buf = malloc(size);
  ld_check(buf);
  SCOPE_EXIT {
    free(buf);
  };
  int rv = event.toPayload(buf, size);
  ld_check(rv == size);
  Payload payload(buf, size);

  lsn_t lsn = LSN_INVALID;
  auto clientImpl = dynamic_cast<ClientImpl*>(&client);
  clientImpl->allowWriteInternalLog();
  rv = wait_until(
      "writes to the event log succeed",
      [&]() {
        lsn = clientImpl->appendSync(event_log_id, payload);
        return lsn != LSN_INVALID;
      },
      deadline);

  if (rv != 0) {
    ld_check(lsn == LSN_INVALID);
    ld_error("Could not write record %s in event log(%lu): %s(%s)",
             event.describe().c_str(),
             event_log_id.val_,
             error_name(err),
             error_description(err));
    return false;
  }

  ld_info("Wrote event log record with lsn %s", lsn_to_string(lsn).c_str());
  return lsn;
}

Status getSeqState(Client* client,
                   logid_t log_id,
                   SequencerState& seq_state,
                   bool wait_for_recovery) {
  Status st = E::OK;
  bool callback_called = false;
  auto callback = [&](GetSeqStateRequest::Result res) {
    st = res.status;
    seq_state.node = res.last_seq;
    seq_state.last_released_lsn = res.last_released_lsn;
    seq_state.next_lsn = res.next_lsn;
    callback_called = true;
  };

  GetSeqStateRequest::Options opts;
  GetSeqStateRequest::Context ctx{GetSeqStateRequest::Context::UNKNOWN};
  opts.wait_for_recovery = wait_for_recovery;
  opts.on_complete = callback;

  auto& processor = static_cast<ClientImpl*>(client)->getProcessor();
  std::unique_ptr<Request> req =
      std::make_unique<GetSeqStateRequest>(logid_t(log_id), ctx, opts);
  processor.blockingRequest(req);

  ld_check(callback_called);
  return st;
}

lsn_t requestShardRebuilding(Client& client,
                             node_index_t node,
                             uint32_t shard,
                             SHARD_NEEDS_REBUILD_flags_t flags,
                             RebuildingRangesMetadata* rrm) {
  SHARD_NEEDS_REBUILD_Header hdr(
      node, shard, "unittest", "IntegrationTestUtils", flags);
  SHARD_NEEDS_REBUILD_Event event(hdr, rrm);
  return writeToEventlog(client, event);
}

lsn_t markShardUndrained(Client& client, node_index_t node, uint32_t shard) {
  SHARD_UNDRAIN_Event event(node, shard);
  return writeToEventlog(client, event);
}

lsn_t markShardUnrecoverable(Client& client,
                             node_index_t node,
                             uint32_t shard) {
  SHARD_UNRECOVERABLE_Event event(node, shard);
  return writeToEventlog(client, event);
}

lsn_t waitUntilShardsHaveEventLogState(std::shared_ptr<Client> client,
                                       std::vector<ShardID> shards,
                                       std::set<AuthoritativeStatus> st,
                                       bool wait_for_rebuilding) {
  std::string reason = "shards ";
  for (int i = 0; i < shards.size(); ++i) {
    if (i > 0) {
      reason += ",";
    }
    reason += shards[i].toString();
  }
  reason += " to have their authoritative status changed to ";
  if (st.size() > 1) {
    reason += "{";
  }
  for (auto it = st.begin(); it != st.end(); ++it) {
    if (it != st.begin()) {
      reason += ", ";
    }
    reason += toString(*it);
  }
  if (st.size() > 1) {
    reason += "}";
  }

  ld_info("Waiting for %s", reason.c_str());
  auto start_time = SteadyTimestamp::now();

  lsn_t last_update = LSN_INVALID;

  int rv = EventLogUtils::tailEventLog(
      *client,
      nullptr,
      [&](const EventLogRebuildingSet& set, const EventLogRecord*, lsn_t) {
        for (const ShardID& shard : shards) {
          std::vector<node_index_t> donors_remaining;
          const auto status = set.getShardAuthoritativeStatus(
              shard.node(), shard.shard(), donors_remaining);
          ld_info("Shard N%u:%u has authoritative status %s, expected %s",
                  shard.node(),
                  shard.shard(),
                  toString(status).c_str(),
                  toString(st).c_str());
          if (!st.count(status) ||
              (wait_for_rebuilding && !donors_remaining.empty())) {
            return true;
          }
        }
        last_update = set.getLastUpdate();
        return false;
      });

  auto seconds_waited =
      std::chrono::duration_cast<std::chrono::duration<double>>(
          SteadyTimestamp::now() - start_time);
  ld_info("Finished waiting for %s (%.3fs)",
          reason.c_str(),
          seconds_waited.count());

  ld_check(rv == 0);
  return last_update;
}

lsn_t waitUntilShardsHaveEventLogState(std::shared_ptr<Client> client,
                                       std::vector<ShardID> shards,
                                       AuthoritativeStatus st,
                                       bool wait_for_rebuilding) {
  return waitUntilShardsHaveEventLogState(
      client, shards, std::set<AuthoritativeStatus>({st}), wait_for_rebuilding);
}

lsn_t waitUntilShardHasEventLogState(std::shared_ptr<Client> client,
                                     ShardID shard,
                                     AuthoritativeStatus st,
                                     bool wait_for_rebuilding) {
  return waitUntilShardsHaveEventLogState(
      client, {shard}, st, wait_for_rebuilding);
}

int Cluster::getShardAuthoritativeStatusMap(ShardAuthoritativeStatusMap& map) {
  auto client = createClient();
  return EventLogUtils::getShardAuthoritativeStatusMap(*client, map);
}

int Cluster::shutdownNodes(const std::vector<node_index_t>& nodes) {
  std::vector<node_index_t> to_wait;
  for (auto i : nodes) {
    auto& n = getNode(i);
    if (n.isRunning()) {
      n.sendCommand("stop");
      to_wait.push_back(i);
    }
  }
  int res = 0;
  for (auto i : to_wait) {
    int rv = getNode(i).waitUntilExited();
    if (rv != 0) {
      res = -1;
    } else {
      getNode(i).stopped_ = true;
    }
  }
  return res;
}

std::vector<node_index_t> Cluster::getRunningStorageNodes() const {
  std::vector<node_index_t> r;
  for (const auto& it : nodes_) {
    if (!it.second->stopped_ && it.second->is_storage_node_) {
      r.push_back(it.first);
    }
  }
  return r;
}

int Cluster::getHashAssignedSequencerNodeId(logid_t log_id, Client* client) {
  SequencerState seq_state;
  Status s = getSeqState(client, log_id, seq_state, true);
  return s == E::OK ? seq_state.node.index() : -1;
}

std::string findBinary(const std::string& relative_path) {
#ifdef FB_BUILD_PATHS
  // Inside FB ask the build system for the full path.  Note that we don't use
  // the plugin facility for this because we run most tests with minimal
  // dependencies (default plugin) to speed up build and run times.
  return facebook::files::FbcodePaths::findPathInFbcodeBin(relative_path);
#else
  return findFile(relative_path, /* require_excutable */ true);
#endif
}

bool Cluster::hasStorageRole(node_index_t node) const {
  return getConfig()->getNodesConfiguration()->isStorageNode(node);
}

int Cluster::writeConfig(const ServerConfig* server_cfg,
                         const LogsConfig* logs_cfg,
                         bool wait_for_update) {
  int rv = overwriteConfig(config_path_.c_str(), server_cfg, logs_cfg);
  if (rv != 0) {
    return rv;
  }
  if (!wait_for_update) {
    return 0;
  }
  config_source_->thread()->advisePollingIteration();
  ld_check(server_cfg != nullptr);
  std::string expected_text =
      (server_cfg ? server_cfg : config_->get()->serverConfig().get())
          ->toString(logs_cfg);
  wait_until("Config update picked up",
             [&] { return config_->get()->toString() == expected_text; });
  waitUntilAllClientsPickedConfig(expected_text);
  return 0;
}

int Cluster::writeConfig(const Configuration& cfg, bool wait_for_update) {
  return writeConfig(
      cfg.serverConfig().get(), cfg.logsConfig().get(), wait_for_update);
}

void Cluster::updateSetting(const std::string& name, const std::string& value) {
  // Do it in parallel because this admin command is extremely slow (T56729673).
  std::vector<std::thread> ts;
  for (auto& kv : nodes_) {
    ts.emplace_back([&node = *kv.second, &name, &value] {
      node.updateSetting(name, value);
    });
  }
  for (std::thread& t : ts) {
    t.join();
  }
}

void Cluster::unsetSetting(const std::string& name) {
  std::vector<std::thread> ts;
  for (auto& kv : nodes_) {
    ts.emplace_back([&node = *kv.second, &name] { node.unsetSetting(name); });
  }
  for (std::thread& t : ts) {
    t.join();
  }
}

std::unique_ptr<configuration::nodes::NodesConfigurationStore>
Cluster::buildNodesConfigurationStore() const {
  using namespace logdevice::configuration::nodes;
  NodesConfigurationStoreFactory::Params params;
  params.type = NodesConfigurationStoreFactory::NCSType::File;
  params.file_store_root_dir = ncs_path_;
  params.path = NodesConfigurationStoreFactory::getDefaultConfigStorePath(
      NodesConfigurationStoreFactory::NCSType::File, cluster_name_);

  return NodesConfigurationStoreFactory::create(std::move(params));
}

std::shared_ptr<const NodesConfiguration>
Cluster::readNodesConfigurationFromStore() const {
  using namespace logdevice::configuration::nodes;
  auto store = buildNodesConfigurationStore();
  if (store == nullptr) {
    return nullptr;
  }
  std::string serialized;
  if (auto status = store->getConfigSync(&serialized); status != Status::OK) {
    ld_error("Failed reading the nodes configuration from the store: %s",
             error_name(status));
    return nullptr;
  }

  return NodesConfigurationCodec::deserialize(std::move(serialized));
}

class ManualNodeSetSelector : public NodeSetSelector {
 public:
  ManualNodeSetSelector(std::set<node_index_t> node_indices, size_t num_shards)
      : node_indices_{std::move(node_indices)}, num_db_shards_(num_shards) {}

  Result getStorageSet(
      logid_t log_id,
      const Configuration* cfg,
      const configuration::nodes::NodesConfiguration& nodes_configuration,
      nodeset_size_t /* ignored */,
      uint64_t /* ignored */,
      const EpochMetaData* prev,
      const Options& /* options */
      ) override {
    Result res;
    const LogsConfig::LogGroupNodePtr logcfg =
        cfg->getLogGroupByIDShared(log_id);
    if (!logcfg) {
      res.decision = Decision::FAILED;
      return res;
    }

    for (const auto nid : node_indices_) {
      shard_index_t sidx = getLegacyShardIndexForLog(log_id, num_db_shards_);
      ShardID sid{nid, sidx};
      res.storage_set.push_back(sid);
    }

    std::sort(res.storage_set.begin(), res.storage_set.end());
    res.decision = (prev && prev->shards == res.storage_set)
        ? Decision::KEEP
        : Decision::NEEDS_CHANGE;
    return res;
  }

 private:
  std::set<node_index_t> node_indices_;
  size_t num_db_shards_;
};

int Cluster::provisionEpochMetadataWithShardIDs(
    std::set<node_index_t> node_indices,
    bool allow_existing_metadata) {
  auto selector = std::make_shared<ManualNodeSetSelector>(
      std::move(node_indices), num_db_shards_);
  return provisionEpochMetaData(std::move(selector), allow_existing_metadata);
}

}}} // namespace facebook::logdevice::IntegrationTestUtils
