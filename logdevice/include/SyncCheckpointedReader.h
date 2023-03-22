/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/include/CheckpointedReaderBase.h"
#include "logdevice/include/Reader.h"

namespace facebook { namespace logdevice {

/*
 * @file SyncCheckpointedReader proxies all Reader functions but also
 *   provides checkpointing by inheriting CheckpointedReaderBase class.
 */
template <typename T>
class SyncCheckpointedReader_ : public CheckpointedReaderBase_<T>,
                                public Reader {
 public:
  SyncCheckpointedReader_(
      const std::string& reader_name,
      T store,
      typename CheckpointedReaderBase_<T>::CheckpointingOptions opts)
      : CheckpointedReaderBase_<T>(reader_name, std::move(store), opts) {}

  /*
   * This function is blocking.
   */
  virtual int
  startReadingFromCheckpoint(logid_t log_id,
                             lsn_t until = LSN_MAX,
                             const ReadStreamAttributes* attrs = nullptr) = 0;

  // If we can not find the checkpoint, we will use user provided start lsn
  // instead the LSN_OLDEST.
  virtual int
  startReadingFromCheckpoint(logid_t log_id,
                             lsn_t start,
                             lsn_t until = LSN_MAX,
                             const ReadStreamAttributes* attrs = nullptr) = 0;
};

using SyncCheckpointedReader =
    SyncCheckpointedReader_<std::unique_ptr<CheckpointStore>>;

using SharedSyncCheckpointedReader =
    SyncCheckpointedReader_<std::shared_ptr<CheckpointStore>>;

}} // namespace facebook::logdevice
