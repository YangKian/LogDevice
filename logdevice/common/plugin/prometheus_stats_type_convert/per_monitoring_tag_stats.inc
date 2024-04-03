/**
 * Copyright (c) 2019-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
/* can be included multiple times */

#ifndef EXPORT_COUNTER_TYPE
#error EXPORT_COUNTER_TYPE() macro not defined
#define EXPORT_COUNTER_TYPE(...)
#endif

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */

EXPORT_COUNTER_TYPE(num_read_streams, false, "")
EXPORT_COUNTER_TYPE(read_streams_stuck, false, "")
EXPORT_COUNTER_TYPE(read_streams_stuck_failing_sync_seq_req, false, "")
EXPORT_COUNTER_TYPE(read_streams_lagging, false, "")
EXPORT_COUNTER_TYPE(read_streams_stuck_or_lagging, false, "")

