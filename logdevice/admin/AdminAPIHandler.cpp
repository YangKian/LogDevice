/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/admin/AdminAPIHandler.h"

#include <array>

#include <folly/MoveWrapper.h>
#include <folly/stats/MultiLevelTimeSeries.h>
#include <thrift/lib/cpp/util/EnumUtils.h>

#include "logdevice/admin/Conv.h"
#include "logdevice/admin/SettingOverrideTTLRequest.h"
#include "logdevice/admin/maintenance/ClusterMaintenanceStateMachine.h"
#include "logdevice/admin/safety/SafetyChecker.h"
#include "logdevice/common/BuildInfo.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/logs/LogsConfigManager.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/SettingsUpdater.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/common/util.h"
#include "logdevice/server/LogGroupCustomCounters.h"
#include "logdevice/server/LogGroupThroughput.h"

namespace facebook { namespace logdevice {

using fb_status = facebook::fb303::cpp2::fb_status;

AdminAPIHandler::AdminAPIHandler(
    const std::string& service_name,
    Processor* processor,
    std::shared_ptr<SettingsUpdater> settings_updater,
    UpdateableSettings<ServerSettings> updateable_server_settings,
    UpdateableSettings<AdminServerSettings> updateable_admin_server_settings,
    StatsHolder* stats_holder)
    : AdminAPIHandlerBase(processor,
                          std::move(settings_updater),
                          std::move(updateable_server_settings),
                          std::move(updateable_admin_server_settings),
                          stats_holder),
      LogDeviceThriftHandler(service_name, processor) {
  safety_checker_ = std::make_shared<SafetyChecker>(processor_);
  safety_checker_->useAdminSettings(updateable_admin_server_settings_);
}

void AdminAPIHandler::getLogTreeInfo(thrift::LogTreeInfo& response) {
  auto logsconfig = processor_->config_->getLocalLogsConfig();
  ld_check(logsconfig);
  response.set_version(std::to_string(logsconfig->getVersion()));
  response.set_num_logs(logsconfig->size());
  response.set_max_backlog_seconds(logsconfig->getMaxBacklogDuration().count());
  response.set_is_fully_loaded(logsconfig->isFullyLoaded());
}

fb_status AdminAPIHandler::getStatus() {
  ShardedRocksDBLocalLogStore* sharded_store =
      AdminAPIHandlerBase::sharded_store_;
  if (sharded_store != nullptr) {
    for (int i = 0; i < sharded_store->numShards(); ++i) {
      LocalLogStore* local_log_store = sharded_store->getByIndex(i);
      if (local_log_store != nullptr && local_log_store->inFailSafeMode()) {
        return fb_status::WARNING;
      }
    }
  }
  return LogDeviceThriftHandler::getStatus();
}

void AdminAPIHandler::getReplicationInfo(thrift::ReplicationInfo& response) {
  auto logsconfig = processor_->config_->getLocalLogsConfig();
  ld_check(logsconfig);
  // Tolerable Failure Domain : {RACK: 2} which means that you can take
  // down 2 racks and we _should_ be still read-available. This is only an
  // approximation and is not guaranteed since you might have older data
  // that was replicated with an old replication policy that is more
  // restrictive.
  ld_check(logsconfig != nullptr);

  ReplicationProperty repl = logsconfig->getNarrowestReplication();
  // create the narrowest replication property json
  std::map<thrift::LocationScope, int32_t> narrowest_replication;
  for (auto item : repl.getDistinctReplicationFactors()) {
    narrowest_replication[toThrift<thrift::LocationScope>(item.first)] =
        item.second;
  }
  response.set_narrowest_replication(std::move(narrowest_replication));

  thrift::TolerableFailureDomain tfd;

  const auto biggest_replication_scope = repl.getBiggestReplicationScope();
  tfd.set_domain(toThrift<thrift::LocationScope>(biggest_replication_scope));
  tfd.set_count(repl.getReplication(biggest_replication_scope) - 1);

  response.set_smallest_replication_factor(repl.getReplicationFactor());
  response.set_tolerable_failure_domains(tfd);
  response.set_version(std::to_string(logsconfig->getVersion()));
}

void AdminAPIHandler::getSettings(
    thrift::SettingsResponse& response,
    std::unique_ptr<thrift::SettingsRequest> request) {
  auto requested_settings = request->get_settings();

  for (const auto& setting : settings_updater_->getState()) {
    // Filter settings by name (if provided)
    if (requested_settings != nullptr &&
        requested_settings->find(setting.first) == requested_settings->end()) {
      continue;
    }

    auto get = [&](SettingsUpdater::Source src) {
      return settings_updater_->getValueFromSource(setting.first, src)
          .value_or("");
    };
    thrift::Setting s;
    *s.currentValue_ref() = get(SettingsUpdater::Source::CURRENT);
    *s.defaultValue_ref() =
        folly::join(" ", setting.second.descriptor.default_value);

    std::string cli = get(SettingsUpdater::Source::CLI);
    std::string config = get(SettingsUpdater::Source::CONFIG);
    std::string admin_cmd = get(SettingsUpdater::Source::ADMIN_OVERRIDE);

    if (!cli.empty()) {
      s.sources_ref()[thrift::SettingSource::CLI] = std::move(cli);
    }
    if (!config.empty()) {
      s.sources_ref()[thrift::SettingSource::CONFIG] = std::move(config);
    }
    if (!admin_cmd.empty()) {
      s.sources_ref()[thrift::SettingSource::ADMIN_OVERRIDE] =
          std::move(admin_cmd);
    }

    response.settings_ref()[setting.first] = std::move(s);
  }
}

folly::SemiFuture<folly::Unit>
logdevice::AdminAPIHandler::semifuture_applySettingOverride(
    std::unique_ptr<thrift::ApplySettingOverrideRequest> request) {
  folly::Promise<folly::Unit> p;
  auto future = p.getSemiFuture();

  // Validate request
  if (*request->ttl_seconds_ref() <= 0) {
    p.setException(thrift::InvalidRequest("TTL must be > 0 seconds"));
    return future;
  }

  try {
    // Apply the temporary setting
    settings_updater_->setFromAdminCmd(
        *request->name_ref(), *request->value_ref());

    // Post a request to unset the setting after ttl expires.
    // If the request fails, do nothing
    auto ttl = std::chrono::seconds(*request->ttl_seconds_ref());
    std::unique_ptr<Request> req = std::make_unique<SettingOverrideTTLRequest>(
        ttl, *request->name_ref(), settings_updater_);

    if (processor_->postImportant(req) != 0) {
      ld_error("Failed to post SettingOverrideTTLRequest, error: %s.",
               error_name(err));

      // We have a problem. Roll back the temporary setting since it will
      // otherwise never get removed.
      settings_updater_->unsetFromAdminCmd(*request->name_ref());

      p.setException(thrift::OperationError(
          folly::format("Failed to post SettingOverrideTTLRequest, error: {}",
                        error_name(err))
              .str()));
      return future;
    }

  } catch (const boost::program_options::error& ex) {
    p.setException(
        thrift::InvalidRequest(folly::format("Error: {}", ex.what()).str()));
    return future;
  }

  return folly::makeSemiFuture();
}

folly::SemiFuture<folly::Unit>
AdminAPIHandler::semifuture_removeSettingOverride(
    std::unique_ptr<thrift::RemoveSettingOverrideRequest> request) {
  folly::Promise<folly::Unit> p;
  auto future = p.getSemiFuture();

  try {
    settings_updater_->unsetFromAdminCmd(*request->name_ref());
  } catch (const boost::program_options::error& ex) {
    p.setException(
        thrift::InvalidRequest(folly::format("Error: {}", ex.what()).str()));
    return future;
  }

  return folly::makeSemiFuture();
}

folly::SemiFuture<folly::Unit> AdminAPIHandler::semifuture_takeLogTreeSnapshot(
    thrift::unsigned64 min_version) {
  folly::Promise<folly::Unit> p;
  auto future = p.getSemiFuture();

  // Are we running with LCM?
  if (!processor_->settings()->enable_logsconfig_manager) {
    p.setException(thrift::NotSupported(
        "LogsConfigManager is disabled in settings on this node"));
    return future;
  } else if (!processor_->settings()->logsconfig_snapshotting) {
    p.setException(
        thrift::NotSupported("LogsConfigManager snapshotting is not enabled"));
    return future;
  }

  auto logsconfig_worker_type = LogsConfigManager::workerType(processor_);
  auto logsconfig_owner_worker =
      worker_id_t{LogsConfigManager::getLogsConfigManagerWorkerIdx(
          processor_->getWorkerCount(logsconfig_worker_type))};
  // Because thrift does not support u64, we encode it in a i64.
  uint64_t minimum_version = to_unsigned(min_version);

  auto cb = [minimum_version](folly::Promise<folly::Unit> promise) mutable {
    // This is needed because we want to move this into a lambda, an
    // std::function<> does not allow capturing move-only objects, so here we
    // are!
    // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2013/n3610.html
    folly::MoveWrapper<folly::Promise<folly::Unit>> mpromise(
        std::move(promise));
    Worker* w = Worker::onThisThread(true /* enforce_worker */);
    auto config = w->getConfig();
    ld_check(config);
    auto logsconfig = config->localLogsConfig();
    if (minimum_version > 0 && logsconfig->getVersion() < minimum_version) {
      thrift::StaleVersion error(
          folly::format("LogTree version on this node is {} which is lower "
                        "than the minimum requested {}",
                        logsconfig->getVersion(),
                        minimum_version)
              .str());
      error.set_server_version(static_cast<int64_t>(logsconfig->getVersion()));
      mpromise->setException(std::move(error));
      return;
    }
    // LogsConfigManager must exist on this worker, even if the RSM is not
    // started.
    ld_check(w->logsconfig_manager_);

    if (!w->logsconfig_manager_->isLogsConfigFullyLoaded()) {
      mpromise->setException(
          thrift::NodeNotReady("LogsConfigManager has not fully replayed yet"));
      return;
    } else {
      // LogsConfig is has fully replayed. Let's take a snapshot.
      auto snapshot_cb = [=](Status st) mutable {
        switch (st) {
          case E::OK:
            ld_info("A LogTree snapshot has been taken based on an Admin API "
                    "request");
            mpromise->setValue(folly::Unit());
            break;
          case E::UPTODATE:
            ld_info("A LogTree snapshot already exists at the same version.");
            mpromise->setValue(folly::Unit());
            break;
          default:
            mpromise->setException(thrift::OperationError(
                folly::format("Cannot take a snapshot: {}", error_name(st))
                    .str()));
        };
      };
      ld_check(w->logsconfig_manager_->getStateMachine());
      // Actually take the snapshot, the callback will fulfill the promise.
      w->logsconfig_manager_->getStateMachine()->snapshot(
          std::move(snapshot_cb));
    }
  };
  return fulfill_on_worker<folly::Unit>(
      processor_,
      folly::Optional<worker_id_t>(logsconfig_owner_worker),
      logsconfig_worker_type,
      cb,
      RequestType::ADMIN_CMD_UTIL_INTERNAL);
}

folly::SemiFuture<folly::Unit>
AdminAPIHandler::semifuture_takeMaintenanceLogSnapshot(
    thrift::unsigned64 min_version) {
  auto [p, f] = folly::makePromiseContract<folly::Unit>();
  // Are we running with a cluster maintenance state machine?
  if (!updateable_admin_server_settings_
           ->enable_cluster_maintenance_state_machine) {
    p.setException(thrift::NotSupported(
        "ClusterMaintenanceStateMachine is disabled in settings on this node"));
    return std::move(f);
  } else if (!updateable_admin_server_settings_->maintenance_log_snapshotting) {
    // We don't allow snapshotting on this node.
    p.setException(thrift::NotSupported(
        "ClusterMaintenanceStateMachine snapshotting is disabled enabled"));
    return std::move(f);
  }

  // Figure out where does that RSM live.
  auto maintenance_worker_type =
      maintenance::ClusterMaintenanceStateMachine::workerType(processor_);
  auto maintenance_owner_worker =
      worker_id_t{maintenance::ClusterMaintenanceStateMachine::getWorkerIndex(
          processor_->getWorkerCount(maintenance_worker_type))};
  // Because thrift does not support u64, we encode it in a i64.
  uint64_t minimum_version = to_unsigned(min_version);

  // The callback to be executed on the target worker.
  auto cb = [minimum_version](folly::Promise<folly::Unit> promise) mutable {
    // This is needed because we want to move this into a lambda, an
    // std::function<> does not allow capturing move-only objects, so here we
    // are!
    // http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2013/n3610.html
    folly::MoveWrapper<folly::Promise<folly::Unit>> mpromise(
        std::move(promise));
    Worker* w = Worker::onThisThread(true /* enforce_worker */);
    // ClusterMaintenanceStateMachine must exist on this worker, even if the RSM
    // is not started.
    ld_check(w->cluster_maintenance_state_machine_);

    if (!w->cluster_maintenance_state_machine_->isFullyLoaded()) {
      mpromise->setException(thrift::NodeNotReady(
          "ClusterMaintenanceStateMachine has not fully replayed yet"));
      return;
    } else {
      uint64_t current_version =
          w->cluster_maintenance_state_machine_->getVersion();
      if (minimum_version > 0 && current_version < minimum_version) {
        thrift::StaleVersion error(
            folly::format(
                "Maintenance state version on this node is {} which is lower "
                "than the minimum requested {}",
                current_version,
                minimum_version)
                .str());
        error.set_server_version(static_cast<int64_t>(current_version));
        mpromise->setException(std::move(error));
        return;
      }
      // ClusterMaintenanceStateMachine is has fully replayed. Let's take a
      // snapshot.
      auto snapshot_cb = [=](Status st) mutable {
        if (st == E::OK) {
          ld_info("A Maintenance state snapshot has been taken based on an "
                  "Admin API request");
          mpromise->setValue(folly::Unit());
        } else {
          mpromise->setException(thrift::OperationError(
              folly::format("Cannot take a snapshot: {}", error_name(st))
                  .str()));
        }
      };
      // Actually take the snapshot, the callback will fulfill the promise.
      w->cluster_maintenance_state_machine_->snapshot(std::move(snapshot_cb));
    }
  };
  return fulfill_on_worker<folly::Unit>(
      processor_,
      folly::Optional<worker_id_t>(maintenance_owner_worker),
      maintenance_worker_type,
      cb,
      RequestType::ADMIN_CMD_UTIL_INTERNAL);
}

void setLogGroupCustomCountersResponse(
    std::string log_group_name,
    GroupResults counters,
    thrift::LogGroupCustomCountersResponse& response,
    std::vector<uint16_t> keys_filter) {
  std::vector<thrift::LogGroupCustomCounter> results;
  for (const auto& result : counters) {
    if (!keys_filter.empty()) {
      auto key_it =
          std::find(keys_filter.begin(), keys_filter.end(), result.first);
      if (key_it == keys_filter.end()) {
        continue;
      }
    }
    thrift::LogGroupCustomCounter counter;
    *counter.key_ref() = static_cast<int16_t>(result.first);
    *counter.val_ref() = static_cast<int64_t>(result.second);
    results.push_back(counter);
  }

  response.counters_ref()[log_group_name] = std::move(results);
}

void AdminAPIHandler::getLogGroupCustomCounters(
    thrift::LogGroupCustomCountersResponse& response,
    std::unique_ptr<thrift::LogGroupCustomCountersRequest> request) {
  ld_check(request != nullptr);

  if (!stats_holder_) {
    thrift::NotSupported err;
    err.set_message("This admin server cannot provide stats");
    throw err;
  }

  Duration query_interval = std::chrono::seconds(60);
  if (*request->time_period_ref() != 0) {
    query_interval = std::chrono::seconds(*request->time_period_ref());
  }

  CustomCountersAggregateMap agg =
      doAggregateCustomCounters(stats_holder_, query_interval);

  std::string req_log_group = *request->log_group_path_ref();

  std::vector<u_int16_t> keys_filter;
  for (const uint8_t& key : *request->keys_ref()) {
    if (key > std::numeric_limits<uint8_t>::max() || key < 0) {
      thrift::InvalidRequest err;
      std::ostringstream error_message;
      error_message << "key " << key << " is not within the limits 0-"
                    << std::numeric_limits<uint8_t>::max();

      err.set_message(error_message.str());
      throw err;
    }
    keys_filter.push_back(key);
  }

  if (!req_log_group.empty()) {
    if (agg.find(req_log_group) == agg.end()) {
      return;
    }
    auto log_group = agg[req_log_group];

    setLogGroupCustomCountersResponse(
        req_log_group, log_group, response, keys_filter);
    return;
  }

  for (const auto& entry : agg) {
    setLogGroupCustomCountersResponse(
        entry.first, entry.second, response, keys_filter);
  }
}

void AdminAPIHandler::dumpServerConfigJson(std::string& response) {
  // We capture the shared_ptr here to ensure the lifetime of its components
  // lives long enough to finish this request.
  auto config = processor_->config_->get();
  ld_check(config);
  ld_check(config->serverConfig());
  response = config->serverConfig()->toString(
      /* with_logs = */ nullptr,
      config->rqliteConfig().get(),
      /* compress = */ false);
}

void AdminAPIHandler::getClusterName(std::string& response) {
  response = processor_->config_->getServerConfig()->getClusterName();
}

void AdminAPIHandler::getLogGroupThroughput(
    thrift::LogGroupThroughputResponse& response,
    std::unique_ptr<thrift::LogGroupThroughputRequest> request) {
  ld_check(request != nullptr);

  if (!stats_holder_) {
    thrift::NotSupported err;
    err.set_message(
        "This admin server cannot provide per-log-throughtput stats");
    throw err;
  }
  auto operation = request->operation_ref().value_or(
      thrift::LogGroupOperation(thrift::LogGroupOperation::APPENDS));

  using apache::thrift::util::enumName;
  std::string time_series = lowerCase(enumName(operation));

  std::vector<Duration> query_intervals;
  if (request->time_period_ref().has_value()) {
    auto time_period = request->time_period_ref().value();
    for (const auto t : time_period) {
      query_intervals.push_back(std::chrono::seconds(t));
    }
  }
  if (query_intervals.empty()) {
    query_intervals.push_back(std::chrono::seconds(60));
  }

  std::string msg;
  if (!verifyIntervals(stats_holder_, time_series, query_intervals, msg)) {
    thrift::InvalidRequest err;
    err.set_message(msg);
    throw err;
  }

  AggregateMap agg = doAggregate(stats_holder_,
                                 time_series,
                                 query_intervals,
                                 processor_->config_->getLogsConfig());

  std::string req_log_group = request->log_group_name_ref().value_or("");

  for (const auto& entry : agg) {
    std::string log_group_name = entry.first;
    if (!req_log_group.empty() && log_group_name != req_log_group) {
      continue;
    }

    thrift::LogGroupThroughput lg_throughput;
    *lg_throughput.operation_ref() = operation;

    const OneGroupResults& results = entry.second;
    std::vector<int64_t> log_results;
    for (auto result : results) {
      log_results.push_back(int64_t(result));
    }
    *lg_throughput.results_ref() = std::move(log_results);
    response.throughput_ref()[log_group_name] = std::move(lg_throughput);
  }
}
}} // namespace facebook::logdevice
