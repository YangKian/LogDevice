#include "logdevice/server/epoch_store/RqliteEpochStore.h"

#include <algorithm>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <boost/filesystem.hpp>
#include <folly/Memory.h>
#include <folly/small_vector.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/ZookeeperClient.h"
#include "logdevice/common/ZookeeperClientBase.h"
#include "logdevice/common/configuration/ZookeeperConfig.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/settings/Settings.h"
#include "logdevice/common/stats/Stats.h"
#include "logdevice/include/Err.h"
#include "logdevice/server/epoch_store/EpochMetaDataZRQ.h"
#include "logdevice/server/epoch_store/GetLastCleanEpochZRQ.h"
#include "logdevice/server/epoch_store/LogMetaData.h"
#include "logdevice/server/epoch_store/SetLastCleanEpochZRQ.h"
#include "logdevice/server/epoch_store/ZookeeperEpochStoreRequest.h"

namespace facebook { namespace logdevice {

RqliteEpochStore::RqliteEpochStore(
    std::string cluster_name,
    RequestExecutor request_executor,
    std::shared_ptr<RqliteClient> rqclient,
    folly::Optional<NodeID> my_node_id,
    std::shared_ptr<UpdateableNodesConfiguration> nodes_config)
    : cluster_name_(cluster_name),
      request_executor_(std::move(request_executor)),
      rqclient_(rqclient),
      my_node_id_(std::move(my_node_id)),
      nodes_config_(nodes_config) {
  ld_check(!cluster_name.empty() &&
           cluster_name.length() <
               configuration::ZookeeperConfig::MAX_CLUSTER_NAME);
  if (!rqclient_) {
    throw ConstructorFailed();
  }
}

RqliteEpochStore::~RqliteEpochStore() {
  shutting_down_.store(true);
  // close() ensures that no callbacks are invoked after this point. So, we can
  // be sure that no references are held to zkclient_ from any of the callback
  // functions.
  rqclient_->close();
}

std::string RqliteEpochStore::identify() const {
  return "rqlite://" + rqclient_->getUrl() + "/" + tablePrefix();
}

void RqliteEpochStore::provisionLogRows(RequestContext&& context,
                                        std::string value) {
  ld_check(!value.empty());

  std::string table_prefix = tablePrefix();
  std::string logid_str = std::to_string(context.zrq->logid_.val_);

  std::vector<std::string> stmts{
      folly::sformat("create table if not exists {}(logid text unique primary "
                     "key, {} text, version integer) strict",
                     table_prefix + EpochMetaDataZRQ::znodeName,
                     EpochMetaDataZRQ::znodeName),
      folly::sformat("create table if not exists {}(logid text unique primary "
                     "key, {} text, version integer) strict",
                     table_prefix + LastCleanEpochZRQ::znodeNameDataLog,
                     LastCleanEpochZRQ::znodeNameDataLog),
      folly::sformat("create table if not exists {}(logid text unique primary "
                     "key, {} text, version integer) strict",
                     table_prefix + LastCleanEpochZRQ::znodeNameMetaDataLog,
                     LastCleanEpochZRQ::znodeNameMetaDataLog),
      folly::sformat(
          "insert into {} (logid, {}, version) values ('{}', '{}', {}) ",
          table_prefix + EpochMetaDataZRQ::znodeName,
          EpochMetaDataZRQ::znodeName,
          logid_str,
          str2hex(value),
          0),
      folly::sformat(
          "insert into {} (logid, {}, version) values ('{}', '', {}) ",
          table_prefix + LastCleanEpochZRQ::znodeNameDataLog,
          LastCleanEpochZRQ::znodeNameDataLog,
          logid_str,
          0),
      folly::sformat(
          "insert into {} (logid, {}, version) values ('{}', '', {}) ",
          table_prefix + LastCleanEpochZRQ::znodeNameMetaDataLog,
          LastCleanEpochZRQ::znodeNameMetaDataLog,
          logid_str,
          0),
  };

  auto cb = [this, context = std::move(context)](
                RqliteClient::ExecuteResults res) mutable {
    Status st = E::OK;
    for (const auto& r : res.results) {
      if (!r.error.empty()) {
        // TODO: maybe detail errors
        st = E::FAILED;
        break;
      }
    }

    // post completion to do the actual work
    postRequestCompletion(st, std::move(context));
  };

  rqclient_->execute(std::move(stmts), std::move(cb), true);
}

void RqliteEpochStore::writeTable(RequestContext&& context,
                                  std::string value,
                                  uint64_t version) {
  std::string znode_path = context.zrq->getZnodePath("");
  auto [logid, column] = splitZnodePath(znode_path);
  std::string table = tablePrefix() + column;

  std::vector<std::string> stmts{
      folly::sformat(
          "update {} set {} = '{}', version = {} where logid = '{}' and "
          "version = {}",
          table,
          column,
          str2hex(value),
          version + 1,
          logid,
          version),

  };

  // setData() below succeeds only if the current version number of
  // znode at znode_path matches the version that the znode had
  // when we read its value. Zookeeper atomically increments the version
  // number of znode on every write to that znode. If the versions do not
  // match zkSetCf() will be called with status ZBADVERSION. This ensures
  // that if our read-modify-write of znode_path succeeds, it was atomic.
  auto cb = [this, context = std::move(context)](
                RqliteClient::ExecuteResults res) mutable {
    auto logid = context.zrq->logid_;
    Status st;
    if (!res.results[0].error.empty()) {
      st = E::FAILED;
    } else if (res.results[0].rows_affected == 0) {
      // version mismatch
      st = E::AGAIN;
    } else if (res.results[0].rows_affected == 1) {
      st = E::OK;
    } else {
      st = E::UNKNOWN;
    }

    postRequestCompletion(st, std::move(context));
  };

  rqclient_->execute(std::move(stmts), std::move(cb), true);
}

folly::SemiFuture<RqliteEpochStore::RqliteReadResult>
RqliteEpochStore::readTable(const RequestContext& context) {
  auto [promise, future] = folly::makePromiseContract<RqliteReadResult>();
  std::string znode_path = context.zrq->getZnodePath("");
  auto [logid, column] = splitZnodePath(znode_path);
  std::string table = tablePrefix() + column;
  std::vector<std::string> stmts{
      folly::sformat("select {}, version from {} where logid = '{}'",
                     column,
                     table,
                     logid),
  };

  auto cb =
      [p = std::make_shared<folly::Promise<RqliteEpochStore::RqliteReadResult>>(
           std::move(promise))](RqliteClient::QueryResults res) mutable {
        ld_check(res.results.size() == 1);

        std::string error = res.results[0].error;
        Status st;
        std::string value;
        uint64_t version;
        if (error.empty()) {
          if (res.results[0].values.empty()) {
            error = "not found";
            st = E::NOTFOUND;
          } else {
            ld_check(res.results[0].values.size() == 1);
            ld_check(res.results[0].values[0].size() == 2);

            st = E::OK;
            value = hex2str(res.results[0].values[0][0].toString());
            version = res.results[0].values[0][1].toNumber();
          }
        } else {
          std::string no_table("no such table");

          auto cmp =
              std::mismatch(no_table.begin(), no_table.end(), error.begin());
          if (cmp.first == no_table.end()) {
            st = E::NOTFOUND;
          } else {
            st = E::FAILED;
          }
        }
        p->setValue(RqliteReadResult{
            std::move(error), std::move(st), std::move(value), version});
      };

  rqclient_->query(std::move(stmts), std::move(cb));
  return std::move(future);
}

int RqliteEpochStore::runRequest(
    std::shared_ptr<ZookeeperEpochStoreRequest> zrq) {
  ld_check(zrq);
  auto logid = zrq->logid_;
  RequestContext context{
      std::move(zrq),
      LogMetaData::forNewLog(logid),
  };

  auto read_fut = readTable(context);
  folly::Future<RqliteReadResult> fut = std::move(read_fut).toUnsafeFuture();
  std::move(fut).thenValue(
      [this, context = std::move(context)](RqliteReadResult result) mutable {
        onReadTableComplete(std::move(context), std::move(result));
      });

  return 0;
}

void RqliteEpochStore::onReadTableComplete(RequestContext&& context,
                                           RqliteReadResult result) {
  auto& zrq = context.zrq;
  auto& log_metadata = context.log_metadata;
  ZookeeperEpochStoreRequest::NextStep next_step;
  bool do_provision = false;
  ld_check(zrq);

  bool value_existed = result.st == E::OK;
  Status st = result.st;
  if (result.st != E::OK && result.st != E::NOTFOUND) {
    goto err;
  }

  if (value_existed) {
    st = zrq->legacyDeserializeIntoLogMetaData(
        std::move(result.value), log_metadata);
    if (st != Status::OK) {
      goto err;
    }
  }

  next_step = zrq->applyChanges(log_metadata, value_existed);
  switch (next_step) {
    case ZookeeperEpochStoreRequest::NextStep::PROVISION:
      // continue with creation of new znodes
      do_provision = true;
      break;
    case ZookeeperEpochStoreRequest::NextStep::MODIFY:
      // continue the read-modify-write
      ld_check(do_provision == false);
      break;
    case ZookeeperEpochStoreRequest::NextStep::STOP:
      st = err;
      ld_check(
          (dynamic_cast<GetLastCleanEpochZRQ*>(zrq.get()) && st == E::OK) ||
          (dynamic_cast<EpochMetaDataZRQ*>(zrq.get()) && st == E::UPTODATE));
      goto done;
    case ZookeeperEpochStoreRequest::NextStep::FAILED:
      st = err;
      ld_check(st == E::FAILED || st == E::BADMSG || st == E::NOTFOUND ||
               st == E::EMPTY || st == E::EXISTS || st == E::DISABLED ||
               st == E::TOOBIG ||
               ((st == E::INVALID_PARAM || st == E::ABORTED) &&
                dynamic_cast<EpochMetaDataZRQ*>(zrq.get())) ||
               (st == E::STALE &&
                (dynamic_cast<EpochMetaDataZRQ*>(zrq.get()) ||
                 dynamic_cast<SetLastCleanEpochZRQ*>(zrq.get()))));
      goto done;

      // no default to let compiler to check if we exhaust all NextSteps
  }

  // Increment version and timestamp of log metadata.
  log_metadata.touch();

  { // Without this scope gcc throws a "goto cross initialization" error.
    // Using a switch() instead of a goto results in a similar error.
    // Using a single-iteration loop is confusing. Perphas we should start
    // using #define BEGIN_SCOPE { to avoid the extra indentation? --march

    char znode_value[EpochStoreEpochMetaDataFormat::BUFFER_LEN_MAX];
    int znode_value_size =
        zrq->composeZnodeValue(log_metadata, znode_value, sizeof(znode_value));
    if (znode_value_size < 0 || znode_value_size >= sizeof(znode_value)) {
      ld_check(false);
      RATELIMIT_CRITICAL(std::chrono::seconds(1),
                         10,
                         "INTERNAL ERROR: invalid value size %d reported by "
                         "ZookeeperEpochStoreRequest::composeZnodeValue() "
                         "for log %lu",
                         znode_value_size,
                         zrq->logid_.val_);
      st = E::INTERNAL;
      goto err;
    }

    std::string znode_value_str(znode_value, znode_value_size);
    if (do_provision) {
      provisionLogRows(std::move(context), std::move(znode_value_str));
      return;
    } else {
      writeTable(
          std::move(context), std::move(znode_value_str), result.version);
      return;
    }
  }

err:
  ld_check(st != E::OK);

done:
  postRequestCompletion(st, std::move(context));
}

void RqliteEpochStore::postRequestCompletion(Status st,
                                             RequestContext&& context) {
  if (st != E::SHUTDOWN || !shutting_down_.load()) {
    context.zrq->postCompletion(
        st, std::move(context.log_metadata), request_executor_);
  } else {
    // Do not post a CompletionRequest if Zookeeper client is shutting down and
    // the EpochStore is being destroyed. Note that we can get also get an
    // E::SHUTDOWN code if the ZookeeperClient is being destroyed due to
    // zookeeper quorum change, but the EpochStore is still there.
  }
}

int RqliteEpochStore::getLastCleanEpoch(logid_t logid, CompletionLCE cf) {
  return runRequest(std::shared_ptr<ZookeeperEpochStoreRequest>(
      new GetLastCleanEpochZRQ(logid, cf)));
}

int RqliteEpochStore::setLastCleanEpoch(logid_t logid,
                                        epoch_t lce,
                                        const TailRecord& tail_record,
                                        EpochStore::CompletionLCE cf) {
  if (!tail_record.isValid() || tail_record.containOffsetWithinEpoch()) {
    RATELIMIT_CRITICAL(std::chrono::seconds(5),
                       5,
                       "INTERNAL ERROR: attempting to update LCE with invalid "
                       "tail record! log %lu, lce %u, tail record flags: %u",
                       logid.val_,
                       lce.val_,
                       tail_record.header.flags);
    err = E::INVALID_PARAM;
    ld_check(false);
    return -1;
  }

  return runRequest(std::shared_ptr<ZookeeperEpochStoreRequest>(
      new SetLastCleanEpochZRQ(logid, lce, tail_record, cf)));
}

int RqliteEpochStore::createOrUpdateMetaData(
    logid_t logid,
    std::shared_ptr<EpochMetaData::Updater> updater,
    CompletionMetaData cf,
    MetaDataTracer tracer,
    WriteNodeID write_node_id) {
  // do not allow calling this function with metadata logids
  if (logid <= LOGID_INVALID || logid > LOGID_MAX) {
    err = E::INVALID_PARAM;
    return -1;
  }

  return runRequest(std::shared_ptr<ZookeeperEpochStoreRequest>(
      new EpochMetaDataZRQ(logid,
                           cf,
                           std::move(updater),
                           std::move(tracer),
                           write_node_id,
                           nodes_config_->get(),
                           my_node_id_)));
}

std::string RqliteEpochStore::str2hex(const std::string& value) {
  std::stringstream ss;
  for (const auto& item : value) {
    ss << std::hex << std::setfill('0') << std::setw(2)
       << static_cast<unsigned int>(static_cast<unsigned char>(item));
  }
  return ss.str();
}

std::string RqliteEpochStore::hex2str(const std::string& hex) {
  std::string res;
  res.reserve(hex.size() / 2);
  for (int i = 0; i < hex.size(); i += 2) {
    std::istringstream iss(hex.substr(i, 2));
    int byte;
    iss >> std::hex >> byte;
    res.push_back(static_cast<char>(byte));
  }
  return res;
}

std::tuple<std::string, std::string>
RqliteEpochStore::splitZnodePath(const std::string& path) {
  std::stringstream ss(path);
  std::string item;
  std::vector<std::string> elems;
  while (std::getline(ss, item, '/')) {
    elems.push_back(std::move(item));
  }
  ld_check(elems.size() == 3);
  return std::make_tuple(elems[1], elems[2]);
}

}} // namespace facebook::logdevice
