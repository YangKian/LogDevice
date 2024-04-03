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

// Used to count the number of active clients in the cluster. This is bumped
// when the client object is created.
EXPORT_COUNTER_TYPE(client_started, false, "Used to count the number of active clients in the cluster. This is bumped when the client object is created.")

// Count the number of clients failing to initialize for whatever reason
// e.g. failing to fetch the nodes configuration, failing to materialize
// the logs config etc. Bumped when ClientFactory::create() returns a nullptr.
EXPORT_COUNTER_TYPE(client_init_failed, true, "Count the number of clients failing to initialize for whatever reason e.g. failing to fetch the nodes configuration, failing to materialize the logs config etc. Bumped when ClientFactory::create() returns a nullptr.")

// Appends that came back with different common statuses.  These are
// client-level statuses (which we pass to the client-provided callback), not
// internal error codes that we pass over the wire in APPENDED messages.
EXPORT_COUNTER_TYPE(append_success, true, "")
EXPORT_COUNTER_TYPE(append_failed_TIMEDOUT, true, "")
EXPORT_COUNTER_TYPE(append_failed_NOTFOUND, true, "")
EXPORT_COUNTER_TYPE(append_failed_NOTINSERVERCONFIG, true, "")
EXPORT_COUNTER_TYPE(append_failed_NOSEQUENCER, true, "")
EXPORT_COUNTER_TYPE(append_failed_CONNFAILED, true, "")
EXPORT_COUNTER_TYPE(append_failed_PEER_CLOSED, true, "")
EXPORT_COUNTER_TYPE(append_failed_TOOBIG, true, "")
EXPORT_COUNTER_TYPE(append_failed_NOBUFS, true, "")
EXPORT_COUNTER_TYPE(append_failed_SYSLIMIT, true, "")
EXPORT_COUNTER_TYPE(append_failed_SEQNOBUFS, true, "")
EXPORT_COUNTER_TYPE(append_failed_SEQSYSLIMIT, true, "")
EXPORT_COUNTER_TYPE(append_failed_NOSPC, true, "")
EXPORT_COUNTER_TYPE(append_failed_OVERLOADED, true, "")
EXPORT_COUNTER_TYPE(append_failed_DISABLED, true, "")
EXPORT_COUNTER_TYPE(append_failed_ACCESS, true, "")
EXPORT_COUNTER_TYPE(append_failed_INTERNAL, true, "")
EXPORT_COUNTER_TYPE(append_failed_INVALID_PARAM, true, "")
EXPORT_COUNTER_TYPE(append_failed_BADPAYLOAD, true, "")
EXPORT_COUNTER_TYPE(append_failed_ISOLATED, true, "")
EXPORT_COUNTER_TYPE(append_failed_CANCELLED, true, "")
EXPORT_COUNTER_TYPE(append_failed_PEER_UNAVAILABLE, true, "")
EXPORT_COUNTER_TYPE(append_failed_other, true, "")

// Client stats to track effectiveness of REDIRECT_NOT_ALIVE
//
// Number of appends that succeeded after being redirected to
// a dead node with the REDIRECT_NOT_ALIVE flag and forcing reactivation
// of the previous sequencer
EXPORT_COUNTER_TYPE(append_redirected_not_alive_success, true, "Number of appends that succeeded after being redirected to a dead node with the REDIRECT_NOT_ALIVE flag and forcing reactivation of the previous sequencer")
// Number of appends that failed after receiving REDIRECT_NOT_ALIVE flag
EXPORT_COUNTER_TYPE(append_redirected_not_alive_failed, true, "Number of appends that failed after receiving REDIRECT_NOT_ALIVE flag")

// Write path stats

// Every time an append probe is denied by the server, this counter is
// incremented by the size of the payload, indicating how much bandwidth we
// likely avoided wasting (if we had sent the append and it had instafailed).
EXPORT_COUNTER_TYPE(append_probes_bytes_saved, true, "Every time an append probe is denied by the server, this counter is incremented by the size of the payload, indicating how much bandwidth we likely avoided wasting (if we had sent the append and it had instafailed).")
// How many bytes were not sent because APPEND_PROBE message could not be sent
// (stat mainly for tests)
EXPORT_COUNTER_TYPE(append_probes_bytes_unsent_probe_send_error, true, "How many bytes were not sent because APPEND_PROBE message could not be sent (stat mainly for tests)")

// GetClusterStateRequest stats
EXPORT_COUNTER_TYPE(get_cluster_state_started, true, "")
EXPORT_COUNTER_TYPE(get_cluster_state_errors, true, "")
EXPORT_COUNTER_TYPE(get_cluster_state_wave_timeout, true, "")
EXPORT_COUNTER_TYPE(get_cluster_state_timeout, true, "")
EXPORT_COUNTER_TYPE(get_cluster_state_failed, true, "")

// ClusterState stats
EXPORT_COUNTER_TYPE(cluster_state_updates, true, "")

// LogsConfigApiRequest stats
EXPORT_COUNTER_TYPE(logsconfig_api_request_started, true, "")
EXPORT_COUNTER_TYPE(logsconfig_api_request_errors, true, "")
EXPORT_COUNTER_TYPE(logsconfig_api_request_timeout, true, "")
EXPORT_COUNTER_TYPE(logsconfig_api_request_failed, true, "")

// LogsConfig stats
EXPORT_COUNTER_TYPE(logsconfig_start_timeout, true, "")

// Traffic shadowing stats
EXPORT_COUNTER_TYPE(shadow_client_init_failed, true, "")
EXPORT_COUNTER_TYPE(shadow_client_init_retry_failed, true, "")
EXPORT_COUNTER_TYPE(shadow_client_init_retry_success, true, "")
EXPORT_COUNTER_TYPE(shadow_append_attempt, true, "")
EXPORT_COUNTER_TYPE(shadow_append_success, true, "")
EXPORT_COUNTER_TYPE(shadow_append_failed, true, "")
EXPORT_COUNTER_TYPE(shadow_client_not_loaded, true, "")
EXPORT_COUNTER_TYPE(shadow_client_load_retry, true, "")

// API hits stats
//findtime
EXPORT_COUNTER_TYPE(findtime_OK, true, "")
EXPORT_COUNTER_TYPE(findtime_TIMEDOUT, true, "")
EXPORT_COUNTER_TYPE(findtime_INVALID_PARAM, true, "")
EXPORT_COUNTER_TYPE(findtime_ACCESS, true, "")
EXPORT_COUNTER_TYPE(findtime_PARTIAL, true, "")
EXPORT_COUNTER_TYPE(findtime_FAILED, true, "")
EXPORT_COUNTER_TYPE(findtime_SHUTDOWN, true, "")
EXPORT_COUNTER_TYPE(findtime_OTHER, true, "")
EXPORT_COUNTER_TYPE(findtime_NOTFOUND, true, "")
//findkey
EXPORT_COUNTER_TYPE(findkey_OK, true, "")
EXPORT_COUNTER_TYPE(findkey_TIMEDOUT, true, "")
EXPORT_COUNTER_TYPE(findkey_INVALID_PARAM, true, "")
EXPORT_COUNTER_TYPE(findkey_ACCESS, true, "")
EXPORT_COUNTER_TYPE(findkey_PARTIAL, true, "")
EXPORT_COUNTER_TYPE(findkey_FAILED, true, "")
EXPORT_COUNTER_TYPE(findkey_SHUTDOWN, true, "")
EXPORT_COUNTER_TYPE(findkey_OTHER, true, "")
EXPORT_COUNTER_TYPE(findkey_NOTFOUND, true, "")
//get_tail_attributes
EXPORT_COUNTER_TYPE(get_tail_attributes_OK, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_TIMEDOUT, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_CONNFAILED, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_NOSEQUENCER, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_FAILED, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_NOBUFS, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_SHUTDOWN, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_INTERNAL, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_AGAIN, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_OTHER, true, "")
EXPORT_COUNTER_TYPE(get_tail_attributes_NOTFOUND, true, "")
//get_head_attributes
EXPORT_COUNTER_TYPE(get_head_attributes_OK, true, "")
EXPORT_COUNTER_TYPE(get_head_attributes_TIMEDOUT, true, "")
EXPORT_COUNTER_TYPE(get_head_attributes_ACCESS, true, "")
EXPORT_COUNTER_TYPE(get_head_attributes_INVALID_PARAM, true, "")
EXPORT_COUNTER_TYPE(get_head_attributes_SHUTDOWN, true, "")
EXPORT_COUNTER_TYPE(get_head_attributes_FAILED, true, "")
EXPORT_COUNTER_TYPE(get_head_attributes_OTHER, true, "")
EXPORT_COUNTER_TYPE(get_head_attributes_NOTFOUND, true, "")
//get_tail_lsn
EXPORT_COUNTER_TYPE(get_tail_lsn_OK, true, "")
EXPORT_COUNTER_TYPE(get_tail_lsn_TIMEDOUT, true, "")
EXPORT_COUNTER_TYPE(get_tail_lsn_CONNFAILED, true, "")
EXPORT_COUNTER_TYPE(get_tail_lsn_NOSEQUENCER, true, "")
EXPORT_COUNTER_TYPE(get_tail_lsn_FAILED, true, "")
EXPORT_COUNTER_TYPE(get_tail_lsn_NOBUFS, true, "")
EXPORT_COUNTER_TYPE(get_tail_lsn_SHUTDOWN, true, "")
EXPORT_COUNTER_TYPE(get_tail_lsn_INTERNAL, true, "")
EXPORT_COUNTER_TYPE(get_tail_lsn_OTHER, true, "")
EXPORT_COUNTER_TYPE(get_tail_lsn_NOTFOUND, true, "")

//is_log_empty
EXPORT_COUNTER_TYPE(is_log_empty_OK, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_TIMEDOUT, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_INVALID_PARAM, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_PARTIAL, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_ACCESS, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_FAILED, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_NOBUFS, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_SHUTDOWN, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_INTERNAL, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_NOTEMPTY, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_NOTFOUND, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_CONNFAILED, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_OTHER, true, "")
EXPORT_COUNTER_TYPE(is_log_empty_flappy, true, "")
//data_size
EXPORT_COUNTER_TYPE(data_size_OK, true, "")
EXPORT_COUNTER_TYPE(data_size_TIMEDOUT, true, "")
EXPORT_COUNTER_TYPE(data_size_INVALID_PARAM, true, "")
EXPORT_COUNTER_TYPE(data_size_PARTIAL, true, "")
EXPORT_COUNTER_TYPE(data_size_ACCESS, true, "")
EXPORT_COUNTER_TYPE(data_size_FAILED, true, "")
EXPORT_COUNTER_TYPE(data_size_NOBUFS, true, "")
EXPORT_COUNTER_TYPE(data_size_SHUTDOWN, true, "")
EXPORT_COUNTER_TYPE(data_size_INTERNAL, true, "")
EXPORT_COUNTER_TYPE(data_size_OTHER, true, "")
EXPORT_COUNTER_TYPE(data_size_NOTFOUND, true, "")

//trim
EXPORT_COUNTER_TYPE(trim_OK, true, "")
EXPORT_COUNTER_TYPE(trim_TIMEDOUT, true, "")
EXPORT_COUNTER_TYPE(trim_INVALID_PARAM, true, "")
EXPORT_COUNTER_TYPE(trim_FAILED, true, "")
EXPORT_COUNTER_TYPE(trim_PARTIAL, true, "")
EXPORT_COUNTER_TYPE(trim_ACCESS, true, "")
EXPORT_COUNTER_TYPE(trim_NOTFOUND, true, "")
EXPORT_COUNTER_TYPE(trim_OTHER, true, "")

// Client Events
EXPORT_COUNTER_TYPE(critical_events, true, "")
EXPORT_COUNTER_TYPE(error_events, true, "")
EXPORT_COUNTER_TYPE(warning_events, true, "")
EXPORT_COUNTER_TYPE(notice_events, true, "")
EXPORT_COUNTER_TYPE(info_events, true, "")


// Metadata API
EXPORT_COUNTER_TYPE(metadata_api_failures, true, "")
EXPORT_COUNTER_TYPE(metadata_api_total, true, "")

// Appends
EXPORT_COUNTER_TYPE(append_requests, true, "")
EXPORT_COUNTER_TYPE(unexpected_append_failures, true, "")

// Latency
EXPORT_COUNTER_TYPE(append_requests_over_2ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_4ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_8ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_16ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_32ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_64ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_128ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_256ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_512ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_1024ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_2048ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_4096ms, true, "")
EXPORT_COUNTER_TYPE(append_requests_over_8192ms, true, "")
