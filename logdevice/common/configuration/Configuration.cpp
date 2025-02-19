/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/Configuration.h"

#include <boost/filesystem.hpp>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LocalLogsConfig.h"
#include "logdevice/common/configuration/MetaDataLogsConfig.h"
#include "logdevice/common/configuration/ParsingHelpers.h"
#include "logdevice/common/configuration/ServerConfig.h"

using namespace facebook::logdevice::configuration::parser;
using namespace facebook::logdevice::configuration;

namespace facebook { namespace logdevice {

const std::shared_ptr<facebook::logdevice::configuration::LocalLogsConfig>
Configuration::localLogsConfig() const {
  return std::dynamic_pointer_cast<
      facebook::logdevice::configuration::LocalLogsConfig>(logs_config_);
}

const facebook::logdevice::configuration::LocalLogsConfig&
Configuration::getLocalLogsConfig() const {
  return *localLogsConfig().get();
}

Configuration::Configuration(
    std::shared_ptr<ServerConfig> server_config,
    std::shared_ptr<LogsConfig> logs_config,
    std::shared_ptr<const NodesConfiguration> nodes_configuration,
    std::shared_ptr<facebook::logdevice::configuration::RqliteConfig>
        rqlite_config)
    : server_config_(std::move(server_config)),
      logs_config_(std::move(logs_config)),
      nodes_configuration_(std::move(nodes_configuration)),
      rqlite_config_(std::move(rqlite_config)) {}

LogsConfig::LogGroupNodePtr
Configuration::getLogGroupByIDShared(logid_t id) const {
  if (MetaDataLog::isMetaDataLog(id)) {
    return server_config_->getMetaDataLogGroup();
  } else {
    return logs_config_->getLogGroupByIDShared(id);
  }
}

const LogsConfig::LogGroupInDirectory*
Configuration::getLogGroupInDirectoryByIDRaw(logid_t id) const {
  // raw access is only supported by the local config.
  ld_check(logs_config_->isLocal());
  if (MetaDataLog::isMetaDataLog(id)) {
    return &server_config_->getMetaDataLogGroupInDir();
  } else {
    return localLogsConfig()->getLogGroupInDirectoryByIDRaw(id);
  }
}

void Configuration::getLogGroupByIDAsync(
    logid_t id,
    std::function<void(LogsConfig::LogGroupNodePtr)> cb) const {
  if (MetaDataLog::isMetaDataLog(id)) {
    cb(server_config_->getMetaDataLogGroup());
  } else {
    ld_check(logs_config_);
    logs_config_->getLogGroupByIDAsync(id, cb);
  }
}

folly::Optional<std::string> Configuration::getLogGroupPath(logid_t id) const {
  ld_check(logs_config_->isLocal());
  return localLogsConfig()->getLogGroupPath(id);
}

std::chrono::seconds Configuration::getMaxBacklogDuration() const {
  ld_check(logs_config_->isLocal());
  return localLogsConfig()->getMaxBacklogDuration();
}

folly::dynamic stringToJsonObj(const std::string& json) {
  auto parsed = parseJson(json);
  // Make sure the parsed string is actually an object
  if (!parsed.isObject()) {
    ld_error("configuration must be a map");
    err = E::INVALID_CONFIG;
    return nullptr;
  }
  return parsed;
}

std::unique_ptr<Configuration>
Configuration::fromJson(const std::string& jsonPiece,
                        std::shared_ptr<LogsConfig> alternative_logs_config,
                        const ConfigParserOptions& options) {
  auto parsed = stringToJsonObj(jsonPiece);
  if (parsed == nullptr || jsonPiece.empty()) {
    return nullptr;
  }
  return fromJson(parsed, alternative_logs_config, options);
}

std::unique_ptr<Configuration>
Configuration::fromJson(const folly::dynamic& parsed,
                        std::shared_ptr<LogsConfig> alternative_logs_config,
                        const ConfigParserOptions& options) {
  auto server_config = ServerConfig::fromJson(parsed);
  if (!server_config) {
    // hopefully fromJson will correctly set err to INVALID_CONFIG
    return nullptr;
  }

  // Try to parse the rqlite section, but it is only required on servers
  std::unique_ptr<RqliteConfig> rqlite_config;
  auto iter = parsed.find("rqlite");
  if (iter != parsed.items().end()) {
    const folly::dynamic& rqliteSection = iter->second;
    rqlite_config = RqliteConfig::fromJson(rqliteSection);
  }

  std::shared_ptr<LogsConfig> logs_config = nullptr;
  if (alternative_logs_config) {
    logs_config = alternative_logs_config;
  } else {
    auto local_logs_config =
        LocalLogsConfig::fromJson(parsed, *server_config, options);
    if (!local_logs_config) {
      if (err != E::LOGS_SECTION_MISSING) {
        // we don't want to mangle the err if it's LOGS_SECTION_MISSING because
        // TextConfigUpdater uses this to decide whether to auto enable
        // logsconfig manager or not. Otherwise set the err to INVALID_CONFIG
        err = E::INVALID_CONFIG;
      }
      // We couldn't not parse the logs/defaults section of the config, we will
      // return the nullptr logsconfig.
      return std::make_unique<Configuration>(
          std::move(server_config), nullptr, nullptr, std::move(rqlite_config));
    }
    local_logs_config->setInternalLogsConfig(
        server_config->getInternalLogsConfig());

    // This is a fully loaded config, so we mark it as one.
    local_logs_config->markAsFullyLoaded();
    logs_config = std::move(local_logs_config);
  }
  // Ensure the logs config knows about the namespace delimiter (which is
  // specified in the server config).
  logs_config->setNamespaceDelimiter(server_config->getNamespaceDelimiter());

  return std::make_unique<Configuration>(
      std::move(server_config), logs_config, nullptr, std::move(rqlite_config));
}

std::unique_ptr<Configuration>
Configuration::fromJsonFile(const char* path,
                            std::unique_ptr<LogsConfig> alternative_logs_config,
                            const ConfigParserOptions& options) {
  // Extracting prefix from the path for referencing any files that would be
  // includable from the main config
  boost::filesystem::path path_prefix(path);
  path_prefix.remove_filename();

  std::string json_blob = readFileIntoString(path);
  if (json_blob.size() == 0) {
    return nullptr;
  }
  auto parsed = stringToJsonObj(json_blob);
  return fromJson(parsed, std::move(alternative_logs_config), options);
}

std::unique_ptr<Configuration>
Configuration::loadFromString(const std::string& server,
                              const std::string& logs) {
  std::shared_ptr<ServerConfig> server_config;
  std::shared_ptr<LocalLogsConfig> logs_config;
  std::shared_ptr<RqliteConfig> rqlite_config;

  auto parsed = parseJson(server);
  if (!parsed.isObject()) {
    return nullptr;
  }

  server_config = ServerConfig::fromJson(parsed);
  if (server_config) {
    auto rqlite = parsed.find("rqlite");
    if (rqlite != parsed.items().end()) {
      rqlite_config = RqliteConfig::fromJson(rqlite->second);
      if (!rqlite_config) {
        return nullptr;
      }
    }

    logs_config =
        LocalLogsConfig::fromJson(logs, *server_config, ConfigParserOptions());
    if (logs_config) {
      return std::make_unique<Configuration>(
          server_config, logs_config, nullptr, rqlite_config);
    }
  }
  return nullptr;
}

int Configuration::validateJson(const char* server_config_contents,
                                const char* logs_config_contents) {
  auto config = loadFromString(server_config_contents, logs_config_contents);
  return (config && (bool)config->logsConfig()) ? 0 : -1;
}

std::string Configuration::normalizeJson(const char* server_config_contents,
                                         const char* logs_config_contents) {
  auto config = loadFromString(server_config_contents, logs_config_contents);
  if (config) {
    return config->toString();
  }
  return "";
}
std::string Configuration::toString() const {
  if (server_config_) {
    return server_config_->toString(logs_config_.get(), rqlite_config_.get());
  }
  return "";
}

std::unique_ptr<Configuration> Configuration::withNodesConfiguration(
    std::shared_ptr<const NodesConfiguration> nodes_configuration) const {
  std::shared_ptr<ServerConfig> server_config{
      server_config_ ? server_config_->copy() : nullptr};
  std::shared_ptr<LogsConfig> logs_config{logs_config_ ? logs_config_->copy()
                                                       : nullptr};
  std::shared_ptr<RqliteConfig> rqlite_config{
      rqlite_config_ ? std::make_shared<RqliteConfig>(*rqlite_config_)
                     : nullptr};
  return std::make_unique<Configuration>(std::move(server_config),
                                         std::move(logs_config),
                                         std::move(nodes_configuration),
                                         std::move(rqlite_config));
}

}} // namespace facebook::logdevice
