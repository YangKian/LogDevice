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

// Number of active server read streams
EXPORT_COUNTER_TYPE(read_streams, false, "Number of active server read streams")
// Number of appends that succeeded
EXPORT_COUNTER_TYPE(append_success, true, "Number of appends that succeeded")
// Number of appends that failed (excluding preempted)
EXPORT_COUNTER_TYPE(append_failed, true, "Number of appends that failed (excluding preempted)")
// Number of RECORD messages sent for the log
EXPORT_COUNTER_TYPE(records_sent, true, "Number of RECORD messages sent for the log")
// Number of FINDKEY messages received for the log
EXPORT_COUNTER_TYPE(findkey_received, true, "Number of FINDKEY messages received for the log")
// Number of TRIM messages received for the log
EXPORT_COUNTER_TYPE(trim_received, true, "Number of TRIM messages received for the log")
// Payload bytes sent by clients in APPENDs
EXPORT_COUNTER_TYPE(append_payload_bytes, true, "Payload bytes sent by clients in APPENDs")
// Payload bytes sent to clients in RECORDs
EXPORT_COUNTER_TYPE(record_payload_bytes, true, "Payload bytes sent to clients in RECORDs")
// Number of failures to deliver a metadata log record upon request because of
// empty metadata log or malformed records
EXPORT_COUNTER_TYPE(metadata_log_read_failed, true, "Number of failures to deliver a metadata log record upon request because of empty metadata log or malformed records")
// Number of IS_LOG_EMPTY requests received
EXPORT_COUNTER_TYPE(is_log_empty_received, true, "Number of IS_LOG_EMPTY requests received")
// Number of IS_LOG_EMPTY_REPLY responses sent with TRUE answer
EXPORT_COUNTER_TYPE(is_log_empty_reply_true, true, "Number of IS_LOG_EMPTY_REPLY responses sent with TRUE answer")
// Number of IS_LOG_EMPTY_REPLY responses sent with FALSE answer
EXPORT_COUNTER_TYPE(is_log_empty_reply_true, true, "Number of IS_LOG_EMPTY_REPLY responses sent with FALSE answer")
// Number of IS_LOG_EMPTY_REPLY responses sent with ERROR status
EXPORT_COUNTER_TYPE(is_log_empty_reply_error, true, "Number of IS_LOG_EMPTY_REPLY responses sent with ERROR status")
// Number of DATA_SIZE_REPLY responses sent with a successful answer
EXPORT_COUNTER_TYPE(data_size_reply, true, "Number of DATA_SIZE_REPLY responses sent with a successful answer")
// Number of DATA_SIZE_REPLY responses sent with ERROR status
EXPORT_COUNTER_TYPE(data_size_reply_error, true, "Number of DATA_SIZE_REPLY responses sent with ERROR status")
// Number of GET_TRIM_POINT requests received
EXPORT_COUNTER_TYPE(get_trim_point_received, true, "Number of GET_TRIM_POINT requests received")
// Number of GET_TRIM_POINT response sent with OK status
EXPORT_COUNTER_TYPE(get_trim_point_reply_ok, true, "Number of GET_TRIM_POINT response sent with OK status")
// Number of GET_TRIM_POINT response sent with ERROR status_
EXPORT_COUNTER_TYPE(get_trim_point_reply_error, true, "Number of GET_TRIM_POINT response sent with ERROR status_")
// Number of GET_SEQ_STATE messages sent with UNRELEASED_RECORD context
EXPORT_COUNTER_TYPE(get_seq_state_sent_context_unreleased_record, true, "Number of GET_SEQ_STATE messages sent with UNRELEASED_RECORD context")
// Number of GET_SEQ_STATE messages received with UNRELEASED_RECORD context
EXPORT_COUNTER_TYPE(get_seq_state_received_context_unreleased_record, true, "Number of GET_SEQ_STATE messages received with UNRELEASED_RECORD context")
// Number of DATA_SIZE requests received
EXPORT_COUNTER_TYPE(data_size_received, true, "Number of DATA_SIZE requests received")
