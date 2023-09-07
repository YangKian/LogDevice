#include "logdevice/common/RqliteVersionedConfigStore.h"

#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>

#include <folly/Format.h>
#include <folly/Synchronized.h>
#include <folly/synchronization/Baton.h>

#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

//////// RqliteVersionedConfigStore ////////

void RqliteVersionedConfigStore::getConfig(
    std::string key,
    value_callback_t callback,
    folly::Optional<version_t> base_version) const {
  // key format: table/rowKey
  auto [table, rowKey] = splitKey(key);
  std::vector<std::string> stmts{
      folly::sformat(
          "select key, value, version from {} where key = '{}'", table, rowKey),
  };

  RqliteClient::query_callback_t completion =
      [this,
       cb = std::make_shared<value_callback_t>(std::move(callback)),
       base_version,
       key](RqliteClient::QueryResults query_results) mutable {
        Status status = this->toStatus(query_results.results[0]);
        if (status != Status::OK) {
          (*cb)(status, "");
          return;
        }

        auto value = getValueAndVersion(query_results.results[0]).first;
        if (base_version.has_value()) {
          auto current_version_opt = extract_fn_(value);
          if (!current_version_opt) {
            RATELIMIT_WARNING(std::chrono::seconds(10),
                              5,
                              "Failed to extract version from value read from "
                              "RqliteVersionedConfigurationStore. key: \"%s\"",
                              key.c_str());
            (*cb)(Status::BADMSG, "");
            return;
          }
          if (current_version_opt.value() <= base_version.value()) {
            // zk's config version is not larger than the base version
            (*cb)(Status::UPTODATE, "");
            return;
          }
        }

        (*cb)(Status::OK, std::move(value));
      };
  rqlite_->query(stmts, std::move(completion));
}

void RqliteVersionedConfigStore::getLatestConfig(
    std::string key,
    value_callback_t callback) const {
  getConfig(std::move(key), std::move(callback));
}

void RqliteVersionedConfigStore::readModifyWriteConfig(std::string key,
                                                       mutation_callback_t mcb,
                                                       write_callback_t cb) {
  auto locked_ptr = shutdown_completed_.tryRLock();
  if (shutdownSignaled()) {
    cb(E::SHUTDOWN, version_t{}, "");
    return;
  }
  ld_assert(locked_ptr && !*locked_ptr);

  // naive implementation of read-modify-write
  auto [table, rowKey] = splitKey(key);
  std::vector<std::string> stmts{
      folly::sformat(
          "select key, value, version from {} where key = '{}'", table, rowKey),
  };

  RqliteClient::query_callback_t read_cb =
      [this,
       key,
       mutation_callback =
           std::make_shared<mutation_callback_t>(std::move(mcb)),
       write_callback = std::make_shared<write_callback_t>(std::move(cb))](
          RqliteClient::QueryResults query_results) mutable {
        auto locked_p = this->shutdown_completed_.tryRLock();
        // (1) try acquiring rlock failed || (2) shutdown_completed == true
        if (!locked_p || *locked_p) {
          // (2) should not happen based on our assumption of ZK dtor behavior.
          ld_assert(!*locked_p);
          ld_assert(this->shutdownSignaled());
          (*write_callback)(E::SHUTDOWN, version_t{}, "");
          return;
        }

        Status status = toStatus(query_results.results[0]);
        if (status != E::OK && status != E::NOTFOUND) {
          (*write_callback)(status, version_t{}, "");
          return;
        }
        folly::Optional<version_t> cur_opt = folly::none;
        if (status != E::NOTFOUND) {
          auto current_value =
              getValueAndVersion(query_results.results[0]).first;
          cur_opt = extract_fn_(current_value);
          if (!cur_opt) {
            RATELIMIT_WARNING(std::chrono::seconds(10),
                              5,
                              "Failed to extract version from value read from "
                              "RqliteVersionedConfigurationStore. key: \"%s\"",
                              key.c_str());
          }
        }
        auto status_value = (*mutation_callback)(
            (status == E::NOTFOUND)
                ? folly::none
                : folly::Optional<std::string>(
                      getValueAndVersion(query_results.results[0]).first));
        auto& write_value = status_value.second;

        if (status_value.first != E::OK) {
          (*write_callback)(
              status_value.first, version_t{}, std::move(write_value));
          return;
        }

        folly::Optional<version_t> opt = folly::none;
        opt = extract_fn_(write_value);
        if (!opt) {
          RATELIMIT_WARNING(std::chrono::seconds(10),
                            5,
                            "Failed to extract version from value provided for "
                            " key: \"%s\"",
                            key.c_str());
          err = E::INVALID_PARAM;
          (*write_callback)(E::INVALID_PARAM, version_t{}, "");
          return;
        }
        version_t new_version = opt.value();

        if (status != E::NOTFOUND) {
          if (cur_opt) {
            version_t cur_version = cur_opt.value();
            if (new_version.val() <= cur_version.val()) {
              // TODO: Add stricter enforcement of monotonic increment of
              // version.
              RATELIMIT_WARNING(
                  std::chrono::seconds(10),
                  5,
                  "Config value's version is not monitonically increasing"
                  "key: \"%s\". prev version: \"%lu\". version: \"%lu\"",
                  key.c_str(),
                  cur_version.val(),
                  new_version.val());
            }
          }

          writeModifiedValue(
              std::move(key),
              std::move(write_value),
              new_version,
              getValueAndVersion(query_results.results[0]).second,
              write_callback);
        } else {
          writeModifiedValueNew(std::move(key),
                                std::move(write_value),
                                new_version,
                                write_callback);
        }
      }; // read_cb

  rqlite_->query(stmts, std::move(read_cb));
}

void RqliteVersionedConfigStore::writeModifiedValue(
    std::string key,
    std::string write_value,
    version_t new_version,
    uint64_t row_version,
    std::shared_ptr<write_callback_t> write_callback) {
  auto keep_alive = shutdown_completed_.tryRLock();
  if (shutdownSignaled()) {
    (*write_callback)(E::SHUTDOWN, version_t{}, "");
    return;
  }
  ld_assert(keep_alive);
  ld_assert(!*keep_alive);

  auto [table, rowKey] = splitKey(key);
  std::vector<std::string> stmts{folly::sformat(
      "update {} set value = '{}', version = {} where key = '{}' and "
      "version = {}",
      table,
      str2hex(write_value),
      row_version + 1,
      rowKey,
      row_version)};

  RqliteClient::execute_callback_t completion =
      [this, new_version, write_callback](
          RqliteClient::ExecuteResults execute_results) mutable {
        Status st;
        if (!execute_results.results[0].error.empty()) {
          st = E::FAILED;
        } else if (execute_results.results[0].rows_affected == 0) {
          st = E::VERSION_MISMATCH;
        } else if (execute_results.results[0].rows_affected == 1) {
          st = E::OK;
        } else {
          st = E::INTERNAL;
        }
        (*write_callback)(st, st == E::OK ? new_version : version_t{}, "");
      };

  rqlite_->execute(stmts, std::move(completion));
}

void RqliteVersionedConfigStore::writeModifiedValueNew(
    std::string key,
    std::string write_value,
    version_t new_version,
    std::shared_ptr<write_callback_t> write_callback) {
  // For the createWithAncestors recipe, we must keep zk_ alive until
  // create_callback is called / destroyed. We do so by capturing the
  // SharedLockedPtr (i.e., the shared lock) by value in the callback.
  // auto keep_alive = this->shutdown_completed_.tryRLock();
  auto keep_alive = this->shutdown_completed_.tryRLock();
  if (shutdownSignaled()) {
    (*write_callback)(E::SHUTDOWN, version_t{}, "");
    return;
  }
  ld_assert(keep_alive);
  ld_assert(!*keep_alive);

  // overwrite() when znode does not exist should rarely happen, so
  // ld_info is OK here.
  ld_info(
      "Creating key %s with NC version %lu", key.c_str(), new_version.val());

  auto [table, rowKey] = splitKey(key);
  std::vector<std::string> stmts{
      folly::sformat("create table if not exists {}(key text primary "
                     "key, value text, version integer) strict",
                     table),
      folly::sformat(
          "insert into {} (key, value, version) values ('{}', '{}', {}) ",
          table,
          rowKey,
          str2hex(write_value),
          0),
  };

  RqliteClient::execute_callback_t completion =
      [this,
       key,
       write_value,
       new_version,
       keep_alive =
           std::make_shared<folly::LockedPtr<folly::Synchronized<bool>,
                                             folly::detail::SynchronizedLockPolicyTryShared>>(
               std::move(keep_alive)),
       write_callback](RqliteClient::ExecuteResults execute_results) mutable {
        Status st;
        if (!execute_results.results[1].error.empty()) {
          auto& error = execute_results.results[0].error;
          std::string row_exists_error("UNIQUE constraint failed");
          if (matchError(error, row_exists_error)) {
            // This means that something was written to ZK between our
            // read and write. In this case, we should fail with
            // VERSION_MISMATCH.
            st = E::VERSION_MISMATCH;
          } else {
            st = E::FAILED;
          }
        } else if (execute_results.results[1].rows_affected == 1) {
          st = E::OK;
        } else {
          st = E::INTERNAL;
        }
        (*write_callback)(st, st == E::OK ? new_version : version_t{}, "");
      };

  rqlite_->execute(stmts, std::move(completion));
}

void RqliteVersionedConfigStore::shutdown() {
  shutdown_signaled_.store(true);
  {
    // acquire wlock which will wait for all readers (i.e., in-flight callbacks)
    // to finish, essentially the "join" for ZK-VCS.
    shutdown_completed_.withWLock([this](bool& completed) {
      // let go of the ZookeeperClient instance
      this->rqlite_ = nullptr;
      completed = true;
    });
  }
}

bool RqliteVersionedConfigStore::shutdownSignaled() const {
  return shutdown_signaled_.load();
}

Status RqliteVersionedConfigStore::toStatus(
    const RqliteClient::QueryResults::Result& result) const {
  if (result.error.empty()) {
    if (result.values.empty()) {
      return E::NOTFOUND;
    } else {
      return E::OK;
    }
  } else {
    if (noTable(result.error)) {
      return E::NOTFOUND;
    } else {
      return E::FAILED;
    }
  }
}

std::pair<std::string, uint64_t> RqliteVersionedConfigStore::getValueAndVersion(
    const RqliteClient::QueryResults::Result& res) const {
  // TODO
  ld_check(res.values.size() == 1);
  ld_check(res.values[0].size() == 3);
  auto row = res.values[0];
  auto value = hex2str(row[1].toString());
  auto version = row[2].toNumber();
  return std::make_pair(value, version);
}

bool RqliteVersionedConfigStore::noTable(const std::string& error) const {
  std::string no_table_error("no such table");
  if (matchError(error, no_table_error)) {
    return true;
  } else {
    return false;
  }
}

bool RqliteVersionedConfigStore::matchError(
    const std::string& error,
    const std::string& candidate) const {
  std::string no_table_error("no such table");
  auto res = std::mismatch(candidate.begin(), candidate.end(), error.begin());
  if (res.first == candidate.end()) {
    return true;
  } else {
    return false;
  }
}

std::tuple<std::string, std::string>
RqliteVersionedConfigStore::splitKey(const std::string& key) const {
  std::stringstream ss(key);
  std::string item;
  std::vector<std::string> elems;
  while (std::getline(ss, item, '/')) {
    elems.push_back(std::move(item));
  }
  ld_check(elems.size() == 2);
  return std::make_tuple(elems[0], elems[1]);
}

std::string RqliteVersionedConfigStore::str2hex(const std::string& value) {
  std::stringstream ss;
  for (const auto& item : value) {
    ss << std::hex << std::setfill('0') << std::setw(2)
       << static_cast<unsigned int>(static_cast<unsigned char>(item));
  }
  return ss.str();
}

std::string RqliteVersionedConfigStore::hex2str(const std::string& hex) {
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

}} // namespace facebook::logdevice
