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

// Stats shared between server and client

// Number of calls to postRequest
EXPORT_COUNTER_TYPE(post_request_total, true, "Stats shared between server and client Number of calls to postRequest")
// Number of postRequest failures.
EXPORT_COUNTER_TYPE(post_request_failed, true, "Number of postRequest failures.")
// Number of Requests executed on Worker threads.
EXPORT_COUNTER_TYPE(worker_requests_executed, true, "Number of Requests executed on Worker threads.")
// Number of Requests > request_execution_delay_threshold in execute().
EXPORT_COUNTER_TYPE(worker_slow_requests, true, "Number of Requests > request_execution_delay_threshold in execute().")
// Number of Request queue times > request_queue_warning_time_limit in execute().
EXPORT_COUNTER_TYPE(worker_long_queued_requests, true, "Number of Request queue times > request_queue_warning_time_limit in execute().")
// Number of HI_PRI Request queue times > request_queue_warning_time_limit in execute().
EXPORT_COUNTER_TYPE(worker_hi_pri_long_queued_requests, true, "Number of HI_PRI Request queue times > request_queue_warning_time_limit in execute().")
// Number of tasks on background thread that spent > 10 msec executing.
EXPORT_COUNTER_TYPE(background_slow_requests, true, "Number of tasks on background thread that spent > 10 msec executing.")
// TaskQueue stats.
EXPORT_COUNTER_TYPE(worker_enqueued_hi_pri_work, true, "")
EXPORT_COUNTER_TYPE(worker_enqueued_mid_pri_work, true, "")
EXPORT_COUNTER_TYPE(worker_enqueued_lo_pri_work, true, "")
EXPORT_COUNTER_TYPE(worker_executed_hi_pri_work, true, "")
EXPORT_COUNTER_TYPE(worker_executed_mid_pri_work, true, "")
EXPORT_COUNTER_TYPE(worker_executed_lo_pri_work, true, "")

EXPORT_COUNTER_TYPE(ssl_context_created, true, "")
EXPORT_COUNTER_TYPE(ssl_session_resumption_attempt, true, "")
EXPORT_COUNTER_TYPE(ssl_session_resumption_success, true, "")
EXPORT_COUNTER_TYPE(ssl_session_resumption_cached, true, "")

// See OverloadDetector
EXPORT_COUNTER_TYPE(num_workers_tracked_by_overload_detector, true, "")
EXPORT_COUNTER_TYPE(num_overloaded_workers, true, "")

// number of MetaDataLogReaders created
EXPORT_COUNTER_TYPE(metadata_log_readers_created, true, "number of MetaDataLogReaders created")
// number of MetaDataLogReaders started
EXPORT_COUNTER_TYPE(metadata_log_readers_started, true, "number of MetaDataLogReaders started")
// number of MetaDataLogReaders finished
EXPORT_COUNTER_TYPE(metadata_log_readers_finalized, true, "number of MetaDataLogReaders finished")

// Number of failures to deliver a metadata log record upon request.
// _corruption: because of malformed records,
// _dataloss: because of dataloss gap in metadata log
// _other: because metadata log is empty or access is denied.
EXPORT_COUNTER_TYPE(metadata_log_read_failed_corruption, true, "Number of failures to deliver a metadata log record upon request. _corruption: because of malformed records, _dataloss: because of dataloss gap in metadata log _other: because metadata log is empty or access is denied.")
EXPORT_COUNTER_TYPE(metadata_log_read_dataloss, true, "")
EXPORT_COUNTER_TYPE(metadata_log_read_failed_other, true, "")

// Number of CONFIG_CHANGED_Messages received with Action::Reload.
EXPORT_COUNTER_TYPE(config_changed_reload, true, "Number of CONFIG_CHANGED_Messages received with Action::Reload.")

// Number of times nodes configuration polling gets a success result
EXPORT_COUNTER_TYPE(nodes_configuration_polling_success, true, "Number of times nodes configuration polling gets a success result")
// Number of times nodes configuration polling gets a PARTIAL result
EXPORT_COUNTER_TYPE(nodes_configuration_polling_partial, true, "Number of times nodes configuration polling gets a PARTIAL result")
// Number of times nodes configuration polling gets a failure result
EXPORT_COUNTER_TYPE(nodes_configuration_polling_failed, true, "Number of times nodes configuration polling gets a failure result")

// Set to 1 once the node received valid config. It also resets to 0 if a bad
// config is received, so this is really the validity of the most recent config
// received.
EXPORT_COUNTER_TYPE(last_config_invalid, false, "Set to 1 once the node received valid config. It also resets to 0 if a bad config is received, so this is really the validity of the most recent config received.")
// Config update rejected by a hook
EXPORT_COUNTER_TYPE(config_update_invalid, true, "Config update rejected by a hook")

// number of times nodeset finder got metadata from sequencer
EXPORT_COUNTER_TYPE(nodeset_finder_read_from_sequencer, true, "number of times nodeset finder got metadata from sequencer")
// number of times nodeset finder got metadata from metadata log
EXPORT_COUNTER_TYPE(nodeset_finder_read_from_metadata_log, true, "number of times nodeset finder got metadata from metadata log")
// number of times nodeset finder failed to get metadata from sequencer
// and fallback to read from metadata log
EXPORT_COUNTER_TYPE(nodeset_finder_fallback_to_metadata_log, true, "number of times nodeset finder failed to get metadata from sequencer and fallback to read from metadata log")

// LogsConfigManager
// Number of updates sent to UpdateableLogsConfig by LogsConfigManager
EXPORT_COUNTER_TYPE(logsconfig_manager_published_update, true, "LogsConfigManager Number of updates sent to UpdateableLogsConfig by LogsConfigManager")
// Number of times a new LocalLogsConfig was published because
// server config was updated
EXPORT_COUNTER_TYPE(logsconfig_manager_published_server_config_update, true, "Number of times a new LocalLogsConfig was published because server config was updated")
// LogsConfig manager receiving updates from the state machine
EXPORT_COUNTER_TYPE(logsconfig_manager_received_update, true, "LogsConfig manager receiving updates from the state machine")
// The version of the last processed delta/snapshot on this node
EXPORT_COUNTER_TYPE(logsconfig_manager_tree_version, false, "The version of the last processed delta/snapshot on this node")
// LogsConfig Manager Is Started
EXPORT_COUNTER_TYPE(logsconfig_manager_started, false, "LogsConfig Manager Is Started")
// LogsConfig Manager deserialization errors
EXPORT_COUNTER_TYPE(logsconfig_manager_serialization_errors, true, "LogsConfig Manager deserialization errors")
// LogsConfig number of deltas applied
EXPORT_COUNTER_TYPE(logsconfig_manager_delta_applied, true, "LogsConfig number of deltas applied")

// LogsConfig RSM trimming requests
EXPORT_COUNTER_TYPE(logsconfig_manager_trimming_requests, true, "LogsConfig RSM trimming requests")
// LogsConfig RSM snapshotting errors
EXPORT_COUNTER_TYPE(logsconfig_manager_snapshotting_errors, true, "LogsConfig RSM snapshotting errors")
// LogsConfig number of deltas written in LogsConfig state machine
EXPORT_COUNTER_TYPE(logsconfig_manager_delta_written, true, "LogsConfig number of deltas written in LogsConfig state machine")
// How many snapshots has been created
EXPORT_COUNTER_TYPE(logsconfig_manager_snapshot_created, true, "How many snapshots has been created")
// How many snapshots we have requested
EXPORT_COUNTER_TYPE(logsconfig_manager_snapshot_requested, true, "How many snapshots we have requested")

// RSM Snapshot Store stats
EXPORT_COUNTER_TYPE(rsm_legacy_snapshot_store_init, true, "")
EXPORT_COUNTER_TYPE(rsm_log_snapshot_store_init, true, "")
EXPORT_COUNTER_TYPE(rsm_msg_snapshot_store_init, true, "")
EXPORT_COUNTER_TYPE(rsm_local_snapshot_store_init, true, "")

// NodesConfigurationManager
// Number of new nodes configuration published
EXPORT_COUNTER_TYPE(nodes_configuration_manager_config_published, true, "NodesConfigurationManager Number of new nodes configuration published")
// Number of nodes configuration received
EXPORT_COUNTER_TYPE(nodes_configuration_manager_config_received, true, "Number of nodes configuration received")
// The version of the staged nodes configuration
EXPORT_COUNTER_TYPE(nodes_configuration_manager_staged_version, false, "The version of the staged nodes configuration")
// The version of the nodes configuration being processed
EXPORT_COUNTER_TYPE(nodes_configuration_manager_pending_version, false, "The version of the nodes configuration being processed")
// The version of the last published NodesConfig
EXPORT_COUNTER_TYPE(nodes_configuration_manager_published_version, false, "The version of the last published NodesConfig")
// NodesConfigurationManager Is Started
EXPORT_COUNTER_TYPE(nodes_configuration_manager_started, false, "NodesConfigurationManager Is Started")
// Number of time the NCM updates proposed
EXPORT_COUNTER_TYPE(nodes_configuration_manager_updates_requested, true, "Number of time the NCM updates proposed")
// Number of time the NCM overwrites requested
EXPORT_COUNTER_TYPE(nodes_configuration_manager_overwrites_requested, true, "Number of time the NCM overwrites requested")
// NodesConfigurationManager deserialization errors
EXPORT_COUNTER_TYPE(nodes_configuration_manager_serialization_errors, true, "NodesConfigurationManager deserialization errors")
// Read from NodesConfigurationStore failed
EXPORT_COUNTER_TYPE(nodes_configuration_store_read_failed, true, "Read from NodesConfigurationStore failed")
// Proposed advancement out of intermediary states failed
EXPORT_COUNTER_TYPE(nodes_configuration_manager_advance_intermediary_state_failed, true, "Proposed advancement out of intermediary states failed")

// BufferedWriter stats
EXPORT_COUNTER_TYPE(buffered_writer_bytes_in, true, "")
EXPORT_COUNTER_TYPE(buffered_writer_bytes_batched, true, "")
EXPORT_COUNTER_TYPE(buffered_writer_manual_flush, true, "")
EXPORT_COUNTER_TYPE(buffered_writer_max_payload_flush, true, "")
EXPORT_COUNTER_TYPE(buffered_writer_time_trigger_flush, true, "")
EXPORT_COUNTER_TYPE(buffered_writer_size_trigger_flush, true, "")
EXPORT_COUNTER_TYPE(buffered_writer_retries, true, "")
EXPORT_COUNTER_TYPE(buffered_writer_batches_failed, true, "")
EXPORT_COUNTER_TYPE(buffered_writer_batches_succeeded, true, "")
EXPORT_COUNTER_TYPE(buffered_writer_bytes_in_flight, true, "")

// Lifetime of a BufferedWriter append

EXPORT_COUNTER_TYPE(buffered_append_success, true, "")
// Synchronous errors
EXPORT_COUNTER_TYPE(buffered_append_failed_post_request, true, "")
EXPORT_COUNTER_TYPE(buffered_append_failed_memory_limit, true, "")
EXPORT_COUNTER_TYPE(buffered_append_failed_invalid_param, true, "")
// Asynchronous errors
EXPORT_COUNTER_TYPE(buffered_append_failed_dropped_behind_failed_batch, true, "")
EXPORT_COUNTER_TYPE(buffered_append_failed_actual_append, true, "")
// These can be sync or async, but should be rare either way
EXPORT_COUNTER_TYPE(buffered_append_failed_shutdown, true, "")
EXPORT_COUNTER_TYPE(buffered_append_failed_other, true, "")
// This should be equal to the sum of all the stats above
EXPORT_COUNTER_TYPE(buffered_appends, true, "Total number of buffered_appends")

// No. of times a recipient got a Message payload where the checksum
// in the ProtocolHeader does not match the checksum computed on the recipient
EXPORT_COUNTER_TYPE(protocol_checksum_mismatch, true, "No. of times a recipient got a Message payload where the checksum in the ProtocolHeader does not match the checksum computed on the recipient")

// No. of times a recipient got a Message payload with a checksum
// in the ProtocolHeader and it matched the checksum computed on the recipient
EXPORT_COUNTER_TYPE(protocol_checksum_matched, true, "No. of times a recipient got a Message payload with a checksum in the ProtocolHeader and it matched the checksum computed on the recipient")

// GetSeqStateRequest stats
// Number of times a GetSeqStateRequest was created.
// This doesn't count the retries or the reuse of the same request id.
EXPORT_COUNTER_TYPE(get_seq_state_unique, true, "GetSeqStateRequest stats Number of times a GetSeqStateRequest was created. This doesn't count the retries or the reuse of the same request id.")

// Total number of times GetSeqStateRequests were merged.
EXPORT_COUNTER_TYPE(get_seq_state_merged_total, true, "Total number of times GetSeqStateRequests were merged.")

// Number of times a new GetSeqStateRequest was
// merged into an existing one because the options
// matched b/w both requests.
EXPORT_COUNTER_TYPE(get_seq_state_merge_into_existing, true, "Number of times a new GetSeqStateRequest was merged into an existing one because the options matched b/w both requests.")

// Number of times an existing request's options
// matched with the new request, but the new request
// wants to start a new state machine instead of using
// the existing state machine, e.g. rebuilding
EXPORT_COUNTER_TYPE(get_seq_state_merge_into_new, true, "Number of times an existing request's options matched with the new request, but the new request wants to start a new state machine instead of using the existing state machine, e.g. rebuilding")

// Number of times a GetSeqStateRequest was resent
// because sequencer sent E::AGAIN.
EXPORT_COUNTER_TYPE(get_seq_state_resent, true, "Number of times a GetSeqStateRequest was resent because sequencer sent E::AGAIN.")

// Number of times a GetSeqStateRequest timedout
EXPORT_COUNTER_TYPE(get_seq_state_timedout, true, "Number of times a GetSeqStateRequest timedout")

// Total number of attempts that were made to issue GetSeqStateRequest.
// This includes the requests which may not be sent out
// because there already was an outstanding request.
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_unknown, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_catchup_queue, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_findkey_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_islogempty_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_rebuilding_thread, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_rocksdb_cf, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_start_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_store_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_gettrimpoint_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_sealstoragetask, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_get_tail_lsn, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_rebuilding_planner, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_sync_sequencer, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_get_tail_attributes, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_unreleased_record, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_metadata_util, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_historical_metadata, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_get_tail_record, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_reader_monitoring, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_is_log_empty_v2, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_attempts_context_rsm, true, "")

// No. of actual GetSeqStateRequests that were sent out
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_unknown, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_catchup_queue, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_findkey_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_islogempty_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_rebuilding_thread, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_rocksdb_cf, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_start_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_store_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_gettrimpoint_message, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_sealstoragetask, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_get_tail_lsn, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_rebuilding_planner, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_sync_sequencer, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_get_tail_attributes, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_unreleased_record, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_metadata_util, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_historical_metadata, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_get_tail_record, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_reader_monitoring, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_is_log_empty_v2, true, "")
EXPORT_COUNTER_TYPE(get_seq_state_unique_context_rsm, true, "")

// Connection stats.
EXPORT_COUNTER_TYPE(sock_read_events, true, "")
EXPORT_COUNTER_TYPE(sock_time_spent_reading_message, true, "")
EXPORT_COUNTER_TYPE(sock_num_messages_read, true, "")
EXPORT_COUNTER_TYPE(sock_read_event_nobufs, true, "")

EXPORT_COUNTER_TYPE(sock_write_events, true, "")
EXPORT_COUNTER_TYPE(sock_time_spent_to_process_send_done, true, "")
EXPORT_COUNTER_TYPE(sock_num_messages_sent, true, "")

EXPORT_COUNTER_TYPE(sock_misc_socket_events, true, "")
EXPORT_COUNTER_TYPE(sock_connect_event_proc_time, true, "")
EXPORT_COUNTER_TYPE(sock_error_event_proc_time, true, "")
EXPORT_COUNTER_TYPE(sock_peer_closed_event_proc_time, true, "")

EXPORT_COUNTER_TYPE(sock_total_bytes_in_messages_written, true, "")
EXPORT_COUNTER_TYPE(sock_total_time_in_messages_written, true, "")
EXPORT_COUNTER_TYPE(sock_write_sched_delay, true, "")
EXPORT_COUNTER_TYPE(sock_write_sched_size, true, "")
EXPORT_COUNTER_TYPE(sock_write_event_nobufs, true, "")

// Timer Delays
EXPORT_COUNTER_TYPE(wh_timer_sched_delay, true, "Timer Delays")

// Number of connection retries following a timeout.
EXPORT_COUNTER_TYPE(connection_retries, true, "Number of connection retries following a timeout.")
// Number of timeouts trying to connect a tcp socket.
EXPORT_COUNTER_TYPE(connection_timeouts, true, "Number of timeouts trying to connect a tcp socket.")
// Number of handshake timeouts.
EXPORT_COUNTER_TYPE(handshake_timeouts, true, "Number of handshake timeouts.")

// number of attempts to record error messages at severety CRITICAL
EXPORT_COUNTER_TYPE(critical_errors, true, "number of attempts to record error messages at severety CRITICAL")
// number of attempts to record error messages at severety ERROR
EXPORT_COUNTER_TYPE(severe_errors, true, "number of attempts to record error messages at severety ERROR")
// number of attempts to record error messages at severety INFO,NOTE,WARNING
EXPORT_COUNTER_TYPE(production_notices, true, "number of attempts to record error messages at severety INFO,NOTE,WARNING")

// Read path stats
EXPORT_COUNTER_TYPE(records_lost, true, "")
EXPORT_COUNTER_TYPE(records_received, true, "")
EXPORT_COUNTER_TYPE(records_received_while_overloaded, true, "")
EXPORT_COUNTER_TYPE(records_received_wait_for_all, true, "")
EXPORT_COUNTER_TYPE(records_received_scd, true, "")
EXPORT_COUNTER_TYPE(records_received_noscd, true, "")
EXPORT_COUNTER_TYPE(records_delivered, true, "")
EXPORT_COUNTER_TYPE(records_delivered_wait_for_all, true, "")
EXPORT_COUNTER_TYPE(records_delivered_scd, true, "")
EXPORT_COUNTER_TYPE(records_delivered_noscd, true, "")
EXPORT_COUNTER_TYPE(bytes_delivered, true, "")
EXPORT_COUNTER_TYPE(gap_UNKNOWN, true, "")
EXPORT_COUNTER_TYPE(gap_BRIDGE, true, "")
EXPORT_COUNTER_TYPE(gap_HOLE, true, "")
EXPORT_COUNTER_TYPE(gap_DATALOSS, true, "")
EXPORT_COUNTER_TYPE(gap_TRIM, true, "")
EXPORT_COUNTER_TYPE(gap_ACCESS, true, "")
EXPORT_COUNTER_TYPE(gap_NOTINCONFIG, true, "")
EXPORT_COUNTER_TYPE(gap_FILTERED_OUT, true, "")
// Number of read streams currently existing.
// Doesn't include streams that have been destroyed.
EXPORT_COUNTER_TYPE(num_read_streams, false, "Number of read streams currently existing. Doesn't include streams that have been destroyed.")
EXPORT_COUNTER_TYPE(read_streams_healthy, true, "")
EXPORT_COUNTER_TYPE(read_streams_non_authoritative, true, "")
EXPORT_COUNTER_TYPE(read_streams_stalled, true, "")
// ByteOffset requested but not present in records
EXPORT_COUNTER_TYPE(records_missing_byteoffset, true, "ByteOffset requested but not present in records")
// ByteOffset requested but not able to provide
EXPORT_COUNTER_TYPE(records_delivered_without_byteoffset, true, "ByteOffset requested but not able to provide")
EXPORT_COUNTER_TYPE(records_delivered_with_byteoffset, true, "")

// Separate new metrics for read streams that are considered stuck/lagging. Not
// related to read_streams_stalled, read_streams_healthy and
// read_streams_non_authoritative.
EXPORT_COUNTER_TYPE(read_streams_stuck, true, "")
EXPORT_COUNTER_TYPE(read_streams_stuck_failing_sync_seq_req, true, "")
EXPORT_COUNTER_TYPE(read_streams_lagging, true, "")
EXPORT_COUNTER_TYPE(read_streams_stuck_or_lagging, true, "")
EXPORT_COUNTER_TYPE(read_streams_stuck_ignoring_overload, true, "")
EXPORT_COUNTER_TYPE(read_streams_lagging_ignoring_overload, true, "")
EXPORT_COUNTER_TYPE(read_streams_stuck_or_lagging_ignoring_overload, true, "")

// Number of read streams creation events.
// Includes streams that have been destroyed.
EXPORT_COUNTER_TYPE(client_read_streams_created, true, "Number of read streams creation events. Includes streams that have been destroyed.")

EXPORT_COUNTER_TYPE(records_redelivery_attempted, true, "")
EXPORT_COUNTER_TYPE(gaps_redelivery_attempted, true, "")

// Separate reading stats for metadata logs.
EXPORT_COUNTER_TYPE(metadata_log_records_received, true, "")
EXPORT_COUNTER_TYPE(metadata_log_records_received_wait_for_all, true, "")
EXPORT_COUNTER_TYPE(metadata_log_records_delivered, true, "")
EXPORT_COUNTER_TYPE(metadata_log_records_delivered_wait_for_all, true, "")
EXPORT_COUNTER_TYPE(metadata_log_bytes_delivered, true, "")
EXPORT_COUNTER_TYPE(metadata_log_gap_UNKNOWN, true, "")
EXPORT_COUNTER_TYPE(metadata_log_gap_BRIDGE, true, "")
EXPORT_COUNTER_TYPE(metadata_log_gap_HOLE, true, "")
EXPORT_COUNTER_TYPE(metadata_log_gap_DATALOSS, true, "")
EXPORT_COUNTER_TYPE(metadata_log_gap_TRIM, true, "")
EXPORT_COUNTER_TYPE(metadata_log_gap_ACCESS, true, "")
EXPORT_COUNTER_TYPE(metadata_log_gap_NOTINCONFIG, true, "")
EXPORT_COUNTER_TYPE(metadata_log_gap_FILTERED_OUT, true, "")
EXPORT_COUNTER_TYPE(metadata_log_error_NOTFOUND, true, "")

// Stats about client read stream rewinds and single copy delivery.

// Updates to lists of known down nodes.
EXPORT_COUNTER_TYPE(scd_shard_down_added, true, "")
EXPORT_COUNTER_TYPE(scd_shard_slow_added, true, "")
EXPORT_COUNTER_TYPE(scd_shard_underreplicated_region_entered, true, "")
EXPORT_COUNTER_TYPE(scd_shard_underreplicated_region_promoted, true, "")

// Number of rewinds scheduled for various reasons. Note that a scheduled
// rewind may end up not executing, or multiple scheduled rewinds may be merged
// into one, so make sure to check the "rewound_*" counters too.
EXPORT_COUNTER_TYPE(rewind_scheduled, true, "") // all reasons
EXPORT_COUNTER_TYPE(rewind_scheduled_nodeset_or_config_change, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_scd_timeout, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_scd_all_send_all_timeout, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_all_shards_down, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_shard_up, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_outliers_changed, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_rebuilding, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_started_failed, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_checksum_fail, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_under_replicated_region, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_shard_not_authoritative, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_shard_became_authoritative_empty, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_window, true, "")
EXPORT_COUNTER_TYPE(rewind_scheduled_connection_failure, true, "")
// This type of rewind doesn't actually get scheduled, it's executed directly,
// and it's not in RewindReason enum.
// But let's use the same name format as the stats above.
EXPORT_COUNTER_TYPE(rewind_scheduled_when_dataloss, true, "This type of rewind doesn't actually get scheduled, it's executed directly, and it's not in RewindReason enum. But let's use the same name format as the stats above.")

// Number of rewinds actually done.
// "asa" = all-send-all
EXPORT_COUNTER_TYPE(rewind_done, true, "Number of rewinds actually done. 'asa' = all-send-all") // all modes
EXPORT_COUNTER_TYPE(rewound_scd_to_scd, true, "")
EXPORT_COUNTER_TYPE(rewound_scd_to_asa, true, "")
EXPORT_COUNTER_TYPE(rewound_asa_to_scd, true, "")
EXPORT_COUNTER_TYPE(rewound_asa_to_asa, true, "")

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */

/*
 * These stats will not be aggregated for destroyed threads.
 */

// Counts config updates
EXPORT_COUNTER_TYPE(updated_config, true, "Counts config updates")
// Counts config updates ignored because their version is older than the
// running config
EXPORT_COUNTER_TYPE(config_update_old_version, true, "Counts config updates ignored because their version is older than the running config")
// Counts config updates that came in with same version but different hash
EXPORT_COUNTER_TYPE(config_update_hash_mismatch, true, "Counts config updates that came in with same version but different hash")
// Counts config updates ignored because they were identical to running config
EXPORT_COUNTER_TYPE(config_update_same_version, true, "Counts config updates ignored because they were identical to running config")

// SLO
EXPORT_COUNTER_TYPE(durability_failures, true, "SLO")
EXPORT_COUNTER_TYPE(durability_total, true, "")

// Network stats
// Failed to send a message because we are out of send buffer
EXPORT_COUNTER_TYPE(send_failed_nobufs, true, "Network stats Failed to send a message because we are out of send buffer")

// Connection health check stats. Not accumulated for destroyed threads.
EXPORT_COUNTER_TYPE(num_sockets, false, "")
EXPORT_COUNTER_TYPE(sock_active, false, "")
EXPORT_COUNTER_TYPE(sock_stalled, false, "")
EXPORT_COUNTER_TYPE(sock_app_limited, false, "")
EXPORT_COUNTER_TYPE(sock_receiver_throttled, false, "")
EXPORT_COUNTER_TYPE(sock_network_throttled, false, "")
// Socket has been inactive for prolonged period of time
EXPORT_COUNTER_TYPE(sock_idle, false, "")
EXPORT_COUNTER_TYPE(slow_socket_detection_time, false, "")
EXPORT_COUNTER_TYPE(sock_health_unknown, false, "")
EXPORT_COUNTER_TYPE(client_connection_close_backlog, false, "")
EXPORT_COUNTER_TYPE(server_connection_close_backlog, false, "")

