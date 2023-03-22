/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/lib/checkpointing/SyncCheckpointedReaderImpl.h"

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

template <typename T>
SyncCheckpointedReaderImpl_<T>::SyncCheckpointedReaderImpl_(
    const std::string& reader_name,
    std::unique_ptr<Reader> reader,
    T store,
    typename CheckpointedReaderBase_<T>::CheckpointingOptions opts)
    : SyncCheckpointedReader_<T>(reader_name, std::move(store), opts),
      reader_(std::move(reader)) {
  ld_check(reader_);
}

template <typename T>
int SyncCheckpointedReaderImpl_<T>::startReadingFromCheckpoint(
    logid_t log_id,
    lsn_t start,
    lsn_t until,
    const ReadStreamAttributes* attrs) {
  lsn_t from = LSN_INVALID;
  auto status = this->store_->getLSNSync(this->reader_name_, log_id, &from);
  // We don't want to read the checkpoint twice, so we start from the next
  // record.
  ++from;
  if (status == Status::NOTFOUND) {
    from = start;
    status = Status::OK;
  }
  if (status != Status::OK) {
    err = status;
    return -1;
  }
  return startReading(log_id, from, until, attrs);
}

template <typename T>
int SyncCheckpointedReaderImpl_<T>::startReadingFromCheckpoint(
    logid_t log_id,
    lsn_t until,
    const ReadStreamAttributes* attrs) {
  return startReadingFromCheckpoint(log_id, LSN_INVALID, until, attrs);
}

template <typename T>
int SyncCheckpointedReaderImpl_<T>::startReading(
    logid_t log_id,
    lsn_t from,
    lsn_t until,
    const ReadStreamAttributes* attrs) {
  this->last_read_lsn_.erase(log_id);
  return reader_->startReading(log_id, from, until, attrs);
}

template <typename T>
int SyncCheckpointedReaderImpl_<T>::stopReading(logid_t log_id) {
  return reader_->stopReading(log_id);
}

template <typename T>
bool SyncCheckpointedReaderImpl_<T>::isReading(logid_t log_id) const {
  return reader_->isReading(log_id);
}

template <typename T>
bool SyncCheckpointedReaderImpl_<T>::isReadingAny() const {
  return reader_->isReadingAny();
}

template <typename T>
int SyncCheckpointedReaderImpl_<T>::setTimeout(
    std::chrono::milliseconds timeout) {
  return reader_->setTimeout(timeout);
}

template <typename T>
ssize_t SyncCheckpointedReaderImpl_<T>::read(
    size_t nrecords,
    std::vector<std::unique_ptr<DataRecord>>* data_out,
    GapRecord* gap_out) {
  int nread = reader_->read(nrecords, data_out, gap_out);
  if (nread >= 0) {
    ld_check(data_out);
    for (auto& record_ptr : *data_out) {
      this->setLastLSNInMap(record_ptr->logid, record_ptr->attrs.lsn);
    }
  } else {
    ld_check(gap_out);
    if (gap_out->hi != LSN_MAX) {
      this->setLastLSNInMap(gap_out->logid, gap_out->hi);
    }
  }
  return nread;
}

template <typename T>
void SyncCheckpointedReaderImpl_<T>::waitOnlyWhenNoData() {
  reader_->waitOnlyWhenNoData();
}

template <typename T>
void SyncCheckpointedReaderImpl_<T>::addMonitoringTag(std::string tag) {
  reader_->addMonitoringTag(std::move(tag));
}

template <typename T>
void SyncCheckpointedReaderImpl_<T>::withoutPayload() {
  reader_->withoutPayload();
}

template <typename T>
void SyncCheckpointedReaderImpl_<T>::forceNoSingleCopyDelivery() {
  reader_->forceNoSingleCopyDelivery();
}

template <typename T>
void SyncCheckpointedReaderImpl_<T>::includeByteOffset() {
  reader_->includeByteOffset();
}

template <typename T>
void SyncCheckpointedReaderImpl_<T>::doNotSkipPartiallyTrimmedSections() {
  reader_->doNotSkipPartiallyTrimmedSections();
}

template <typename T>
int SyncCheckpointedReaderImpl_<T>::isConnectionHealthy(logid_t log_id) const {
  return reader_->isConnectionHealthy(log_id);
}

template <typename T>
void SyncCheckpointedReaderImpl_<T>::doNotDecodeBufferedWrites() {
  reader_->doNotDecodeBufferedWrites();
}

template <typename T>
void SyncCheckpointedReaderImpl_<T>::setReaderName(
    const std::string& reader_name) {
  reader_->setReaderName(reader_name);
}

template class SyncCheckpointedReaderImpl_<std::unique_ptr<CheckpointStore>>;
template class SyncCheckpointedReaderImpl_<std::shared_ptr<CheckpointStore>>;

}} // namespace facebook::logdevice
