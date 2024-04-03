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

EXPORT_COUNTER_TYPE(workers_started, false, "")

EXPORT_COUNTER_TYPE(readers_assigned, false, "")
EXPORT_COUNTER_TYPE(readers_active, false, "")
EXPORT_COUNTER_TYPE(reader_restarting_to_backlog, false, "")
EXPORT_COUNTER_TYPE(reader_restarting_to_tail, false, "")
EXPORT_COUNTER_TYPE(reader_max_readers_per_log, false, "")
EXPORT_COUNTER_TYPE(reader_restart_backlog_depth_ms_sum, false, "") // milliseconds
EXPORT_COUNTER_TYPE(reader_num_distinct_logs, false, "")
EXPORT_COUNTER_TYPE(reader_records, false, "")
EXPORT_COUNTER_TYPE(reader_bytes, false, "")
EXPORT_COUNTER_TYPE(reader_backlog_bytes, false, "")
EXPORT_COUNTER_TYPE(reader_restarts_done, false, "")
EXPORT_COUNTER_TYPE(reader_restarts_skipped, false, "")
EXPORT_COUNTER_TYPE(reader_records_without_writer_info, false, "")
EXPORT_COUNTER_TYPE(reader_avg_fanout_target_1000x, false, "")
EXPORT_COUNTER_TYPE(reader_avg_backlog_depth_target_ms, false, "")
EXPORT_COUNTER_TYPE(reader_rate_limit_bytes_per_sec_per_worker, false, "")
EXPORT_COUNTER_TYPE(reader_restart_period_target_ms, false, "")
EXPORT_COUNTER_TYPE(reader_target_restart_backlog_probability_1000x, false, "")
EXPORT_COUNTER_TYPE(reader_gap_ACCESS, false, "")
EXPORT_COUNTER_TYPE(reader_gap_DATALOSS, false, "")
EXPORT_COUNTER_TYPE(reader_gap_OTHER, false, "")

EXPORT_COUNTER_TYPE(writer_appends_done, false, "")
EXPORT_COUNTER_TYPE(writer_appends_skipped_latency, false, "")
EXPORT_COUNTER_TYPE(writer_appends_skipped_bytes_in_flight, false, "")
EXPORT_COUNTER_TYPE(writer_appends_skipped_appends_in_flight, false, "")
EXPORT_COUNTER_TYPE(writer_appends_in_flight, false, "")
EXPORT_COUNTER_TYPE(writer_append_bytes_in_flight, false, "")
EXPORT_COUNTER_TYPE(writer_throughput_total_target, false, "")
EXPORT_COUNTER_TYPE(writer_payload_target_avg_size, false, "")
EXPORT_COUNTER_TYPE(writer_highest_target_log_throughput, false, "")
EXPORT_COUNTER_TYPE(writer_largest_log_avg_payload_size, false, "")
EXPORT_COUNTER_TYPE(writer_largest_payload_size, false, "")
EXPORT_COUNTER_TYPE(writer_max_appends_in_flight, false, "")
EXPORT_COUNTER_TYPE(writer_max_append_bytes_in_flight, false, "")
EXPORT_COUNTER_TYPE(writer_target_client_compressibility_1000x, false, "")
EXPORT_COUNTER_TYPE(writer_target_sequencer_compressibility_1000x, false, "")
EXPORT_COUNTER_TYPE(writer_append_timer_slightly_late, false, "")

