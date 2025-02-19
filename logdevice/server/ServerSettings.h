/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <array>
#include <chrono>
#include <string>
#include <unordered_map>

#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/StorageTask-enums.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/ZookeeperConfigSource.h"
#include "logdevice/common/configuration/nodes/NodeRole.h"
#include "logdevice/common/configuration/nodes/ServiceDiscoveryConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/if/gen-cpp2/common_types.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/util.h"
#include "logdevice/server/locallogstore/LocalLogStoreSettings.h"

/**
 * @file Mains server settings.
 */

namespace facebook { namespace logdevice {

struct ServerSettings : public SettingsBundle {
  using NodeServiceDiscovery = configuration::nodes::NodeServiceDiscovery;
  using ClientNetworkPriority =
      configuration::nodes::NodeServiceDiscovery::ClientNetworkPriority;
  using NodesConfigTagMapT = std::unordered_map<std::string, std::string>;

  /**
   * Validates and parses a string containing a list of tags (key-value pairs).
   * The list of key-value pairs must be separated by commas. Keys must not
   * contain colons or commas and values can contain anything but commas. Values
   * can be empty, but keys must not. Key-value pairs are specified as
   * "key:value". Example: key_1:value_1,key_2:,key_3:value_3
   */
  static NodesConfigTagMapT parse_tags(const std::string& tags_string);

  static std::map<ClientNetworkPriority, int>
  parse_ports_per_net_priority(const std::string& value);

  static std::map<ClientNetworkPriority, std::string>
  parse_unix_sockets_per_net_priority(const std::string& value);

  struct TaskQueueParams {
    int nthreads = 0;
  };
  using StoragePoolParams =
      std::array<TaskQueueParams, (size_t)StorageTaskThreadType::MAX>;

  const char* getName() const override {
    return "ServerSettings";
  }

  void defineSettings(SettingEasyInit& init) override;

  int port;
  std::string unix_socket;
  bool require_ssl_on_command_port;
  int ssl_command_port;
  bool admin_enabled;
  int command_conn_limit;
  dbg::Level loglevel;
  dbg::LogLevelMap loglevel_overrides;
  dbg::Colored logcolored;
  bool assert_on_data;
  // number of background workers
  int num_background_workers;
  std::string log_file;
  std::string config_path;
  std::string epoch_store_path;
  StoragePoolParams storage_pool_params;
  std::chrono::milliseconds shutdown_timeout;
  // Interval between invoking syncs for delayable storage tasks.
  // Ignored when undelayable task is being enqueued.
  std::chrono::milliseconds storage_thread_delaying_sync_interval;
  std::string server_id;
  int fd_limit;
  bool eagerly_allocate_fdtable;
  int num_reserved_fds;
  bool lock_memory;
  std::string user;
  SequencerOptions sequencer;
  bool unmap_caches;
  bool disable_event_log_trimming;
  bool ignore_cluster_marker;
  // When set represents the file where trim actions will be logged.
  // All changes to Trim points are stored in this log.
  std::string audit_log;

  bool shutdown_on_node_configuration_mismatch;
  bool hard_exit_on_node_configuration_mismatch;

  // (server-only setting) Maximum number of incoming connections that have been
  // accepted by listener (have an open FD) but have not been processed by
  // workers (made logdevice protocol handshake)
  size_t connection_backlog;

  bool test_mode;

  bool wipe_storage_when_storage_state_none;

  // Self Registration Specific attributes
  bool enable_node_self_registration;
  std::string name;
  std::chrono::seconds sleep_secs_after_self_registeration;
  folly::Optional<uint64_t> version;
  // TODO(mbassem): This is the IP, do we need a better name?
  std::string address;
  int ssl_port;
  int server_to_server_port;
  std::string ssl_unix_socket;
  std::string server_to_server_unix_socket;
  int gossip_port;
  std::string gossip_unix_socket;
  configuration::nodes::RoleSet roles;
  NodeLocation location;
  double sequencer_weight;
  double storage_capacity;
  int num_shards;
  // Connection config for client-facing Thrift API
  int client_thrift_api_port;
  std::string client_thrift_api_unix_socket;
  // Connection config for server-to-server Thrift API
  int server_thrift_api_port;
  std::string server_thrift_api_unix_socket;
  NodesConfigTagMapT tags;

  bool use_tls_ticket_seeds;
  std::string tls_ticket_seeds_path;

  bool enable_dscp_reflection;

  std::map<ClientNetworkPriority, std::string>
      unix_addresses_per_network_priority;
  std::map<ClientNetworkPriority, int> ports_per_network_priority;

 private:
  // Only UpdateableSettings can create this bundle to ensure defaults are
  // populated.
  ServerSettings() {}
  friend class UpdateableSettingsRaw<ServerSettings>;

  int command_port;
  std::string command_unix_socket;
};

}} // namespace facebook::logdevice
