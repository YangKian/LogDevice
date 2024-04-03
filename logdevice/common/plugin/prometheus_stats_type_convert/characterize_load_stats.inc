/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
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

EXPORT_COUNTER_TYPE(scuba_error, false, "")
EXPORT_COUNTER_TYPE(data_size_failed, false, "")
EXPORT_COUNTER_TYPE(data_size_overflows, false, "")
EXPORT_COUNTER_TYPE(data_size_failed_to_post, false, "")
EXPORT_COUNTER_TYPE(get_tail_LSN_failed, false, "")
EXPORT_COUNTER_TYPE(get_tail_LSN_failed_to_post, false, "")
EXPORT_COUNTER_TYPE(failed_to_start_reading, false, "")
EXPORT_COUNTER_TYPE(found_no_records_log, false, "")
EXPORT_COUNTER_TYPE(found_no_records, false, "")
EXPORT_COUNTER_TYPE(run_started, false, "")
EXPORT_COUNTER_TYPE(run_finished, false, "")
