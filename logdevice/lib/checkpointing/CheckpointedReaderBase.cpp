/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/include/CheckpointedReaderBase.h"

#include <chrono>

#include "logdevice/common/ReadStreamAttributes.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

template <typename T>
CheckpointedReaderBase_<T>::CheckpointedReaderBase_(
    const std::string& reader_name,
    T store,
    CheckpointingOptions opts)
    : options_(opts), reader_name_(reader_name), store_(std::move(store)) {}

template <typename T>
Status CheckpointedReaderBase_<T>::syncWriteCheckpoints(
    const std::map<logid_t, lsn_t>& checkpoints) {
  Status return_status = Status::UNKNOWN;
  for (int retries = 0; retries < options_.num_retries; retries++) {
    return_status = store_->updateLSNSync(reader_name_, checkpoints);
    if (return_status == Status::OK) {
      return return_status;
    }
    RATELIMIT_WARNING(std::chrono::seconds(10),
                      1,
                      "Failed to write checkpoints for reader %s, status code "
                      "%s, trial number %d",
                      reader_name_.c_str(),
                      error_name(return_status),
                      retries);
  }
  return return_status;
}

template <typename T>
void CheckpointedReaderBase_<T>::asyncWriteCheckpoints(
    const std::map<logid_t, lsn_t>& checkpoints,
    StatusCallback cb) {
  store_->updateLSN(reader_name_, checkpoints, std::move(cb));
}

template <typename T>
Status CheckpointedReaderBase_<T>::syncRemoveCheckpoints(
    const std::vector<logid_t>& checkpoints) {
  return store_->removeCheckpointsSync(reader_name_, checkpoints);
}

template <typename T>
void CheckpointedReaderBase_<T>::asyncRemoveCheckpoints(
    const std::vector<logid_t>& checkpoints,
    StatusCallback cb) {
  store_->removeCheckpoints(reader_name_, checkpoints, std::move(cb));
}

template <typename T>
Status CheckpointedReaderBase_<T>::syncRemoveAllCheckpoints() {
  return store_->removeAllCheckpointsSync(reader_name_);
}

template <typename T>
void CheckpointedReaderBase_<T>::asyncRemoveAllCheckpoints(StatusCallback cb) {
  store_->removeAllCheckpoints(reader_name_, std::move(cb));
}

template <typename T>
Status CheckpointedReaderBase_<T>::syncWriteCheckpoints(
    const std::vector<logid_t>& logs) {
  auto checkpoints = getNewCheckpoints(logs);
  if (checkpoints.hasError()) {
    return checkpoints.error();
  }
  return syncWriteCheckpoints(std::move(checkpoints).value());
}

template <typename T>
void CheckpointedReaderBase_<T>::asyncWriteCheckpoints(
    StatusCallback cb,
    const std::vector<logid_t>& logs) {
  auto checkpoints = getNewCheckpoints(logs);
  if (checkpoints.hasError()) {
    cb(checkpoints.error());
    return;
  }
  asyncWriteCheckpoints(std::move(checkpoints).value(), std::move(cb));
}

template <typename T>
folly::Expected<std::map<logid_t, lsn_t>, E>
CheckpointedReaderBase_<T>::getNewCheckpoints(
    const std::vector<logid_t>& logs) {
  std::map<logid_t, lsn_t> checkpoints;
  if (logs.empty()) {
    for (auto [log_id, lsn] : last_read_lsn_) {
      checkpoints[log_id] = lsn;
    }
  } else {
    for (auto log : logs) {
      auto it = last_read_lsn_.find(log);
      if (it == last_read_lsn_.end()) {
        return folly::makeUnexpected(E::INVALID_OPERATION);
      }
      checkpoints[log] = it->second;
    }
  }
  return checkpoints;
}

template <typename T>
void CheckpointedReaderBase_<T>::setLastLSNInMap(logid_t log_id, lsn_t lsn) {
  last_read_lsn_.insert_or_assign(
      log_id, std::max(last_read_lsn_[log_id], lsn));
}

template class CheckpointedReaderBase_<std::unique_ptr<CheckpointStore>>;
template class CheckpointedReaderBase_<std::shared_ptr<CheckpointStore>>;

}} // namespace facebook::logdevice
