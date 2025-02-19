/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/test/TestUtil.h"

#include <chrono>
#include <cstdio>
#include <ctime>
#include <errno.h>
#include <fcntl.h>
#include <fstream>
#include <ifaddrs.h>
#include <memory>
#include <thread>
#include <unistd.h>

#include <folly/FileUtil.h>
#include <folly/Memory.h>
#include <folly/String.h>
#include <folly/dynamic.h>
#include <folly/experimental/TestUtil.h>
#include <folly/json.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>

#include "logdevice/common/NoopTraceLogger.h"
#include "logdevice/common/PermissionChecker.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/ReaderImpl.h"
#include "logdevice/common/SequencerLocator.h"
#include "logdevice/common/Timer.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/ConfigParser.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationCodec.h"
#include "logdevice/common/configuration/nodes/NodesConfigurationManagerFactory.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/plugin/CommonBuiltinPlugins.h"
#include "logdevice/common/protocol/MessageTypeNames.h"
#include "logdevice/common/request_util.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/test/NodesConfigurationTestUtil.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Reader.h"

using facebook::logdevice::configuration::parser::parseAttributes;
namespace fs = boost::filesystem;

namespace facebook { namespace logdevice {

int overwriteConfigFile(const char* path, const std::string& contents) {
  int rv;

  boost::filesystem::path tmp_path;

  // Create a temporary file in the same directory as the file we are
  // overwriting.  The two files need to be on the same filesystem for
  // rename() to succeed.
  {
    using folly::test::TemporaryFile;
    TemporaryFile tmp("LogDeviceTestUtil.overwriteConfigFile",
                      boost::filesystem::path(path).parent_path(),
                      TemporaryFile::Scope::PERMANENT);
    tmp_path = tmp.path();

    rv = folly::writeFull(tmp.fd(), contents.data(), contents.size());
    if (rv < 0) {
      ld_critical("Failed to write config file, errno = %d (%s)",
                  errno,
                  strerror(errno));
      return -1;
    }
  }

  // Calculate the timestamp to be set below.  If the file already exists,
  // we'll bump the timestamp by 2 seconds.  We do this to force
  // FileConfigSourceThread to detect the change to the file even on
  // filesystems with coarse timestamps, without the need for the test to
  // sleep().
  struct stat st;
  time_t mtime_to_set = stat(path, &st) == 0 ? st.st_mtime + 2 : time(nullptr);

  rv = rename(tmp_path.c_str(), path);
  ld_check(rv == 0);

  struct timeval ts = {mtime_to_set, 0};
  struct timeval times[2] = {ts, ts};
  rv = utimes(path, times);
  ld_check(rv == 0);
  return 0;
}

int overwriteConfig(const char* path,
                    const ServerConfig* server_cfg,
                    const LogsConfig* logs_cfg) {
  ld_check(server_cfg);
  return overwriteConfigFile(path, server_cfg->toString(logs_cfg));
}

TemporaryDirectory::TemporaryDirectory(const std::string& name_prefix) {
  const static std::vector<fs::path> prefixes = {
      "/dev/shm/tmp/logdevice", "/tmp/logdevice"};
  for (fs::path prefix : prefixes) {
    try {
      fs::create_directories(prefix);
      fs::path p =
          prefix / fs::unique_path(name_prefix + ".%%%%-%%%%-%%%%-%%%%");
      fs::create_directory(p);
      path_ = p;
      return;
    } catch (const fs::filesystem_error& e) {
      // Failed. Continue with next prefix.
    }
  }
  ld_error("Failed to create directory for test data");
  ld_check(false);
}

TemporaryDirectory::~TemporaryDirectory() {
  if (!path_.has_value()) {
    // Moved out.
    return;
  }
  if (getenv_switch("LOGDEVICE_TEST_LEAVE_DATA") ||
      (getenv_switch("LOGDEVICE_TEST_LEAVE_DATA_IF_FAILED") &&
       testing::Test::HasFailure())) {
    ld_info("Leaving data in %s", path_.value().string().c_str());
    return;
  }
  boost::system::error_code ec;
  fs::remove_all(path_.value(), ec);
  if (ec) {
    ld_error("Failed to delete temporary directory at %s: %s",
             path_.value().string().c_str(),
             toString(ec).c_str());
  }
}

std::shared_ptr<const NodesConfiguration>
createSimpleNodesConfig(size_t nnodes,
                        shard_size_t num_shards,
                        bool all_metadata,
                        int replication_factor) {
  configuration::Nodes nodes;
  for (size_t i = 0; i < nnodes; ++i) {
    nodes[i] = configuration::Node::withTestDefaults(i)
                   .addSequencerRole()
                   .addStorageRole(num_shards)
                   .setIsMetadataNode(all_metadata || i == 0);
  }
  return NodesConfigurationTestUtil::provisionNodes(
      std::move(nodes),
      ReplicationProperty{{NodeLocationScope::NODE, replication_factor}});
}

ServerConfig::MetaDataLogsConfig
createMetaDataLogsConfig(std::vector<node_index_t> positive_weight_nodes,
                         size_t max_replication,
                         NodeLocationScope sync_replication_scope) {
  ServerConfig::MetaDataLogsConfig cfg;
  cfg.metadata_nodes = std::move(positive_weight_nodes);

  int replication = std::min(cfg.metadata_nodes.size(), max_replication);

  // by re-using the actual attribute parsing, we get the default arguments
  // for free
  folly::dynamic attr_map = folly::dynamic::object;
  attr_map[logsconfig::REPLICATION_FACTOR] = replication;
  attr_map[logsconfig::SYNCED_COPIES] = 0;
  attr_map[logsconfig::SYNC_REPLICATION_SCOPE] =
      NodeLocation::scopeNames()[sync_replication_scope];

  folly::Optional<logsconfig::LogAttributes> log_attrs =
      parseAttributes(attr_map,
                      "metadata_logs",
                      /* permissions */ false,
                      /* metadata_logs */ true);

  ld_check(log_attrs.has_value());
  cfg.setMetadataLogGroup(
      logsconfig::LogGroupNode("metadata logs",
                               log_attrs.value(),
                               logid_range_t(LOGID_INVALID, LOGID_INVALID)));

  return cfg;
}

std::shared_ptr<Configuration> createSimpleConfig(size_t nnodes, size_t logs) {
  auto log_attrs = logsconfig::LogAttributes().with_replicationFactor(1);
  auto logs_config = std::make_shared<configuration::LocalLogsConfig>();
  logs_config->insert(
      boost::icl::right_open_interval<logid_t::raw_type>(1, logs + 1),
      "log1",
      log_attrs);

  auto nodes = createSimpleNodesConfig(nnodes);
  auto server_config = ServerConfig::fromDataTest(__FILE__);
  return std::make_shared<Configuration>(
      std::move(server_config), std::move(logs_config), std::move(nodes));
}

int wait_until(const char* reason,
               std::function<bool()> cond,
               std::chrono::steady_clock::time_point deadline) {
  const std::chrono::milliseconds INITIAL_DELAY(10);
  const std::chrono::milliseconds MAX_DELAY(200);
  std::chrono::milliseconds delay(INITIAL_DELAY);

  auto start = std::chrono::steady_clock::now();
  auto last_logged = start;

  if (reason != nullptr) {
    ld_info("\033[0;34mWaiting until:\033[0m %s", reason);
  }

  while (1) {
    auto now = std::chrono::steady_clock::now();
    auto seconds_waited =
        std::chrono::duration_cast<std::chrono::duration<double>>(now - start);

    if (cond()) {
      ld_info("\033[0;32mFinished waiting until:\033[0m %s (%.3fs)",
              reason,
              seconds_waited.count());
      return 0;
    }

    if (now > deadline) {
      ld_info("\033[0;31mTimed out when waiting until:\033[0m %s (%.3fs)",
              reason,
              seconds_waited.count());
      return -1;
    }
    if (now - last_logged >= std::chrono::seconds(5)) {
      if (reason != nullptr) {
        ld_info("\033[0;33mStill waiting until:\033[0m %s (%.3fs)",
                reason,
                seconds_waited.count());
      } else {
        ld_info(
            "\033[0;33mStill waiting (%.3fs)\033[0m", seconds_waited.count());
      }
      last_logged = now;
    }
    sleep_until_safe(std::min(deadline, now + delay));
    delay = std::min(2 * delay, MAX_DELAY);
  }
}

int read_records_swallow_gaps(
    Reader& reader_in,
    size_t nrecords,
    std::vector<std::unique_ptr<DataRecord>>* data_out) {
  std::vector<std::unique_ptr<DataRecord>> data_local;
  if (data_out == nullptr) {
    // The caller did not provide a vector (does not care about the actual
    // data) so use a throwaway
    data_out = &data_local;
  }

  ReaderImpl& reader = checked_downcast<ReaderImpl&>(reader_in);
  auto timeout_stash = reader.getTimeout();

  reader.setTimeout(std::chrono::seconds(1));
  size_t total_read = 0;
  ld_info("Reading %zu records ...", nrecords);
  int ngaps = 0;
  while (reader.isReadingAny()) {
    GapRecord gap;
    int nread = reader.read(nrecords - total_read, data_out, &gap);
    if (nread < 0) {
      ld_check(err == E::GAP);
      ++ngaps;
      continue;
    }
    total_read += nread;
    if (total_read >= nrecords) {
      break;
    }
    ld_info("Read %zu of %zu records ...", total_read, nrecords);
  }
  ld_info("Finished reading");
  ld_check_ge(total_read, nrecords);
  reader.setTimeout(timeout_stash);
  return ngaps;
}

void PrintTo(MessageType type, ::std::ostream* os) {
  *os << messageTypeNames()[type].c_str();
}

void PrintTo(Status st, ::std::ostream* os) {
  *os << error_name(st);
}

bool getenv_switch(const char* name, std::string* value) {
  const char* env = getenv(name);
  // Return false for null, "" and "0", true otherwise.
  bool set = env != nullptr && strlen(env) > 0 && strcmp(env, "0") != 0;
  if (set && value != nullptr) {
    *value = env;
  }
  return set;
}

folly::Optional<dbg::Level> getLogLevelFromEnv() {
  std::string val;
  const char* env = getenv("LOGDEVICE_LOG_LEVEL");
  if (env == nullptr) {
    return folly::none;
  }
  return dbg::tryParseLoglevel(env);
}

folly::Optional<dbg::Colored> getLogColoredFromEnv() {
  std::string val;
  const char* env = getenv("LOGDEVICE_LOG_COLORED");
  if (env == nullptr) {
    return folly::none;
  }
  return dbg::tryParseLogColored(env);
}

std::chrono::milliseconds getDefaultTestTimeout() {
  return getenv_switch("LOGDEVICE_TEST_NO_TIMEOUT")
      ? std::chrono::hours(24 * 365)
      : DEFAULT_TEST_TIMEOUT;
}

std::shared_ptr<PluginRegistry> make_test_plugin_registry() {
  return std::make_shared<PluginRegistry>(
      createAugmentedCommonBuiltinPluginVector<>());
}

std::shared_ptr<Processor>
make_test_processor(const Settings& settings,
                    std::shared_ptr<UpdateableConfig> config,
                    StatsHolder* stats,
                    folly::Optional<NodeID> my_node_id) {
  if (!config) {
    config = UpdateableConfig::createEmpty();
  }
  return Processor::create(config,
                           std::make_shared<NoopTraceLogger>(config),
                           UpdateableSettings<Settings>(settings),
                           stats,
                           make_test_plugin_registry(),
                           "",
                           "",
                           "logdevice",
                           std::move(my_node_id));
}

void gracefully_shutdown_processor(Processor* processor) {
  ld_check(processor != nullptr);
  std::vector<folly::SemiFuture<folly::Unit>> futures =
      fulfill_on_all_workers<folly::Unit>(
          processor,
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
      processor,
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

  ld_info("Stopping Processor");
  processor->waitForWorkers();
}

std::string findFile(const std::string& relative_path,
                     bool require_executable) {
  // Find the path to the currently running program ...
  boost::system::error_code ec;
  fs::path proc_exe_path = fs::read_symlink("/proc/self/exe", ec);
  if (proc_exe_path.empty()) {
    ld_error("Error reading /proc/self/exe: %s", ec.message().c_str());
    return "";
  }

  // Start the search in the same directory, then move up the filesystem
  for (fs::path search_dir = proc_exe_path.parent_path(); !search_dir.empty();
       search_dir = search_dir.parent_path()) {
    fs::path path = search_dir / relative_path;
    if (fs::exists(path)) {
      std::string path_str = path.string();
      if (require_executable && ::access(path_str.c_str(), X_OK) < 0) {
        ld_error("Found \"%s\" but it is not executable!?", path_str.c_str());
        return "";
      }
      return path_str;
    }
  }

  ld_error("Reached top of filesystem without finding \"%s\"",
           relative_path.c_str());
  return "";
}

std::string verifyFileExists(const std::string& filename) {
  if (boost::filesystem::exists(filename)) {
    // Don't search from the binary path if it's available right here
    return filename;
  }
  auto path = findFile(filename);
  if (path.empty()) {
    throw std::runtime_error(
        std::string("File '") + filename +
        "' is required for this test run, but cannot be found. " +
        "Working directory: " + boost::filesystem::current_path().string());
  }
  ld_check(boost::filesystem::exists(path));
  return path;
}

std::string get_localhost_address_str(bool isNonroutable) {
  // Ask the kernel for a list of all network interfaces of the host we are
  // running on.
  struct ifaddrs* ifaddr;
  if (getifaddrs(&ifaddr) != 0) {
    throw std::runtime_error(
        "getifaddrs() failed. errno=" + folly::to<std::string>(errno) + " (" +
        folly::errnoStr(errno) + ")");
  }

  SCOPE_EXIT {
    freeifaddrs(ifaddr);
  };

  // Now compare each returned address to all hosts in the config.
  for (struct ifaddrs* ifa = ifaddr; ifa != nullptr; ifa = ifa->ifa_next) {
    // tun interface address can be null
    if (!ifa->ifa_addr) {
      continue;
    }
    int family = ifa->ifa_addr->sa_family;

    // Only interested in IP addresses
    if (family != AF_INET && family != AF_INET6) {
      continue;
    }

    Sockaddr my_addr(ifa->ifa_addr);

    // Returning the first loopback address found
    if (my_addr.getSocketAddress().isLoopbackAddress()) {
      if (!isNonroutable)
        return my_addr.toStringNoPort();
      if (family == AF_INET)
        return "0.0.0.0";
      if (family == AF_INET6)
        return "::/0";
    }
  }
  throw std::runtime_error("couldn't find any loopback interfaces");
}

std::unique_ptr<folly::test::TemporaryDirectory>
provisionTempNodesConfiguration(const NodesConfiguration& nodes_config) {
  auto temp_dir = std::make_unique<folly::test::TemporaryDirectory>();

  using namespace logdevice::configuration::nodes;
  NodesConfigurationStoreFactory::Params params;
  params.type = NodesConfigurationStoreFactory::NCSType::File;
  params.file_store_root_dir = temp_dir->path().string();
  params.path = NodesConfigurationStoreFactory::getDefaultConfigStorePath(
      NodesConfigurationStoreFactory::NCSType::File, "");

  auto store = NodesConfigurationStoreFactory::create(std::move(params));
  if (store == nullptr) {
    return nullptr;
  }

  auto serialized = NodesConfigurationCodec::serialize(nodes_config);
  if (serialized.empty()) {
    return nullptr;
  }
  store->updateConfigSync(
      std::move(serialized), NodesConfigurationStore::Condition::overwrite());
  return temp_dir;
}

}} // namespace facebook::logdevice
