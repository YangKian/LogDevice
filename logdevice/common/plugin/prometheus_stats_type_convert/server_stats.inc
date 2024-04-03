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

// MS we took to shutdown server
EXPORT_COUNTER_TYPE(shutdown_time_ms, true, "MS we took to shutdown server")
// Total number of read requests
EXPORT_COUNTER_TYPE(read_requests, true, "Total number of read requests")
// Number of read requests that got kicked to storage threads
EXPORT_COUNTER_TYPE(read_requests_to_storage, true, "Number of read requests that got kicked to storage threads")
// Number of epoch offset request that got kicked to storage threads
EXPORT_COUNTER_TYPE(epoch_offset_to_storage, true, "Number of epoch offset request that got kicked to storage threads")
// Number of records not written to RocksDB because their LSN <= trim point
EXPORT_COUNTER_TYPE(skipped_record_lsn_before_trim_point, true, "Number of records not written to RocksDB because their LSN <= trim point")
// Number of write ops of all types submitted to the storage thread pool
EXPORT_COUNTER_TYPE(write_ops, true, "Number of write ops of all types submitted to the storage thread pool")
// Number of multi-write requests passed to the local log store (RocksDB)
// write_ops/write_batches gives the batching factor for group commits
EXPORT_COUNTER_TYPE(write_batches, true, "Number of multi-write requests passed to the local log store (RocksDB) write_ops/write_batches gives the batching factor for group commits")
// Same for rebuilding writes.
EXPORT_COUNTER_TYPE(write_ops_stallable, true, "")
EXPORT_COUNTER_TYPE(write_batches_stallable, true, "")
// Number of write ops queued for WAL sync, but completed immediately
// because they were waiting on a previous sync batch that has completed.
EXPORT_COUNTER_TYPE(write_ops_sync_already_done, true, 
    "Number of write ops queued for WAL sync, but completed immediately because they were waiting on a previous sync batch that has completed.")
// How many times we attempted a non-blocking read
EXPORT_COUNTER_TYPE(non_blocking_reads, true, "How many times we attempted a non-blocking read")
// How many times a non-blocking read did not return any records
EXPORT_COUNTER_TYPE(non_blocking_reads_empty, true, "How many times a non-blocking read did not return any records")
// Number of records removed from local log store by trimming.
// Doesn't include range skips.
EXPORT_COUNTER_TYPE(records_trimmed_removed, true, 
    "Number of records removed from local log store by trimming. Doesn't include range skips")
// Number of metadata log records removed from local log store by trimming
// Doesn't include range skips.
EXPORT_COUNTER_TYPE(metadata_log_records_trimmed_removed, true, 
    "Number of metadata log records removed from local log store by trimming. Doesn't include range skips.")
// Number of per epoch log metadata records removed from local log store
// by trimming
EXPORT_COUNTER_TYPE(per_epoch_log_metadata_trimmed_removed, true, 
    "Number of per epoch log metadata records removed from local log store by trimming.")

// Number of mutable per-epoch log metadata reads for the purpose of reading
// the LNG
EXPORT_COUNTER_TYPE(last_known_good_from_metadata_reads, true, 
    "Number of mutable per-epoch log metadata reads for the purpose of reading the LNG")
// Number of mutable per-epoch log metadata reads that got kicked to storage,
// (i.e., blocking) for the purpose of reading the LNG
EXPORT_COUNTER_TYPE(last_known_good_from_metadata_reads_to_storage, true, 
    "Number of mutable per-epoch log metadata reads that got kicked to storage, (i.e., blocking) for the purpose of reading the LNG")
// Number of data record reads for the purpose of reading the LNG, as fallback
// from failed mutable per-epoch log metadata reads
EXPORT_COUNTER_TYPE(last_known_good_from_record_reads, true, 
    "Number of data record reads for the purpose of reading the LNG, as fallback from failed mutable per-epoch log metadata reads")
// Number of data record reads that got kicked to storage (i.e., blocking), for
// the purpose of reading the LNG, as fallback from failed mutable per-epoch
// log metadata reads
EXPORT_COUNTER_TYPE(last_known_good_from_record_reads_to_storage, true, 
    "Number of data record reads that got kicked to storage (i.e., blocking), for the purpose of reading the LNG, as fallback from failed mutable per-epoch log metadata reads")
// Number of mutable per-epoch log metadata writes (technically merges)
EXPORT_COUNTER_TYPE(mutable_per_epoch_log_metadata_writes, true, "Number of mutable per-epoch log metadata writes (technically merges)")

// Number of successful attempts to start reading past the global last-released
// LSN.
EXPORT_COUNTER_TYPE(read_past_last_released_success, true, 
    "Number of successful attempts to start reading past the global last-released LSN.")
// Number of failed attempts to start reading past the global last-released
// LSN. Failure is not an error, it just means a client attempted to start
// reading beyond the highest safe LSN.
EXPORT_COUNTER_TYPE(read_past_last_released_failed, true, 
    "Number of failed attempts to start reading past the global last-released LSN. Failure is not an error, it just means a client attempted to start reading beyond the highest safe LSN.")

// Number of appends received by the server.  This should be very close to
// `message_received.APPEND' however failed append probes (which come as
// APPEND_PROBE messages not APPEND) are also included.  The main purpose of
// this counter is to serve as a denominator for an "append success/failure
// ratio" graph that reflects what the client observers (timeouts
// notwithstanding).
//
// When sequencer batching is in use, these counters are still in terms of
// incoming APPEND messages, so before batching (and unbatching if incoming
// appends were also batched).  Again the aim is for the counters to line up
// with AppendRequest state machines on the client.
EXPORT_COUNTER_TYPE(append_received, true, "Number of appends received by the server.")     
// Number of appends that succeeded
EXPORT_COUNTER_TYPE(append_success, true, "Number of appends that succeeded")
// Number of appends that were preempted
EXPORT_COUNTER_TYPE(append_preempted, true, "Number of appends that were preempted")
// Number of appends that resulted in a redirect sent to the client
EXPORT_COUNTER_TYPE(append_redirected, true, "Number of appends that resulted in a redirect sent to the client")
// Number of appends that failed (excluding preempted and redirected)
EXPORT_COUNTER_TYPE(append_failed, true, "Number of appends that failed (excluding preempted and redirected)")
// number of APPENDS that failed because runAppender() reported E::SYSLIMIT
// (included in append_failed)
EXPORT_COUNTER_TYPE(append_syslimit, true, 
    "number of APPENDS that failed because runAppender() reported E::SYSLIMIT (included in append_failed)")
// number of appends that end up rediretcing to a dead node (hence with
// REDIRECT_NOT_ALIVE flag)
EXPORT_COUNTER_TYPE(append_redirected_not_alive, true, 
    "number of appends that end up rediretcing to a dead node (hence with REDIRECT_NOT_ALIVE flag)")

// Payload bytes sent by clients in APPENDs
EXPORT_COUNTER_TYPE(append_payload_bytes, true, "Payload bytes sent by clients in APPENDs")
// Payload size in bytes incoming to sequencer batching (if active), after
// unbatching.  (These are the original client bytes if the client is using
// BufferedWriter.)
EXPORT_COUNTER_TYPE(append_bytes_seq_batching_in, true, 
    "Payload size in bytes incoming to sequencer batching (if active), after unbatching.  (These are the original client bytes if the client is using BufferedWriter.)")
// Payload size in bytes of the batched & possibly compressed record that's
// actually stored.  append_bytes_seq_batching_in /
// append_bytes_seq_batching_out gives the compression ratio, including any
// overheads involved for delimiters between the original records.  But not
// overhead for storing the record itself; it just compares Payloads vs
// Payloads.
EXPORT_COUNTER_TYPE(append_bytes_seq_batching_out, true, 
    "Payload size in bytes of the batched & possibly compressed record that's actually stored.")
// How many of the bytes in the '_in' and '_out' stats were just passed
// through by sequencer batching (record already large enough, avoiding a
// compression cycle)
EXPORT_COUNTER_TYPE(append_bytes_seq_batching_passthru, true, 
    "How many of the bytes in the '_in' and '_out' stats were just passed through by sequencer batching (record already large enough, avoiding a compression cycle)")
// Payload bytes incoming to sequencer batching and sent to a BufferedWriter
// shard for uncompression and re-batching.
EXPORT_COUNTER_TYPE(append_bytes_seq_batching_buffer_submitted, true, 
    "Payload bytes incoming to sequencer batching and sent to a BufferedWriter shard for uncompression and re-batching.")
// Incoming payload bytes freed in BufferedWriter after they have been
// re-batched.
EXPORT_COUNTER_TYPE(append_bytes_seq_batching_buffer_freed, true, 
    "Incoming payload bytes freed in BufferedWriter after they have been re-batched.")
// APPEND_PROBE messages that we replied to with E::OK
EXPORT_COUNTER_TYPE(append_probes_passed, true, "APPEND_PROBE messages that we replied to with E::OK")
// APPEND_PROBE messages that we responded to with an error, which instruct
// the client not to follow with a real APPEND message
EXPORT_COUNTER_TYPE(append_probes_denied, true, 
    "APPEND_PROBE messages that we responded to with an error, which instruct the client not to follow with a real APPEND message")
// Number of STORE messages demanding the write to be synced
EXPORT_COUNTER_TYPE(store_synced, true, 
    "Number of STORE messages demanding the write to be synced")
// Number of STORE messages that were amends (had the AMEND flag)
EXPORT_COUNTER_TYPE(store_received_amend, true, "Number of STORE messages that were amends (had the AMEND flag)")
// Number of StoreStorageTasks that timedout (i.e could not be
// executed before task_deadline_)
EXPORT_COUNTER_TYPE(store_storage_task_timedout, true, 
    "Number of StoreStorageTasks that timedout (i.e could not be executed before task_deadline_)")

// Number of redirected appends that recevied LSNs from previous sequencer, and
// were subsequently replicated & released during recovery.
EXPORT_COUNTER_TYPE(append_redirected_previously_stored, true, 
    "Number of redirected appends that recevied LSNs from previous sequencer, and were subsequently replicated & released during recovery.")
// Number of redirected appends that recevied LSNs from previous sequencer, 
// but recovery didn't find it and inserted a hole plug.
EXPORT_COUNTER_TYPE(append_redirected_newly_stored, true, 
    "Number of redirected appends that recevied LSNs from previous sequencer, but recovery didn't find it and inserted a hole plug. ")
// Number of redirected appends that recevied LSNs from previous sequencer, but
// recovery couldn't tell whether or not it was stored.
EXPORT_COUNTER_TYPE(append_redirected_maybe_stored, true, 
    "Number of redirected appends that recevied LSNs from previous sequencer, but recovery couldn't tell whether or not it was stored.")

// Number of streams inserted to CatchupQueue to be processed immediately
EXPORT_COUNTER_TYPE(catchup_queue_push_immediate, true, "Number of streams inserted to CatchupQueue to be processed immediately")
// Number of streams inserted to CatchupQueue to be processed when it's
// ready to read newly released records
EXPORT_COUNTER_TYPE(catchup_queue_push_delayed, true, 
    "Number of streams inserted to CatchupQueue to be processed when it's ready to read newly released records")

// Read stream ordering rules violations
EXPORT_COUNTER_TYPE(read_stream_start_violations, true, "")
EXPORT_COUNTER_TYPE(read_stream_started_violations, true, "")
EXPORT_COUNTER_TYPE(read_stream_gap_violations, true, "")
EXPORT_COUNTER_TYPE(read_stream_record_violations, true, "")

// Read stream byte offset computation
// Number of instances where byte offset is missing in the record payload
EXPORT_COUNTER_TYPE(read_stream_missing_record_offset, true, 
    "Number of instances where byte offset is missing in the record payload")
// Number of instances where epoch offset was unavailable
EXPORT_COUNTER_TYPE(read_stream_missing_epoch_offset, true, 
    "Number of instances where epoch offset was unavailable")
// Number of instances where byte offset was delivered as part of read request
EXPORT_COUNTER_TYPE(read_stream_included_byte_offset, true, 
    "Number of instances where byte offset was delivered as part of read request")

// Read stream error computing epoch offset
// Epoch offset unavailable due to store uninitialized
EXPORT_COUNTER_TYPE(epoch_offset_store_uninitialized, true, "Epoch offset unavailable due to store uninitialized")
// Epoch offset unavailable due to error getting log metadata for earlier epochs
EXPORT_COUNTER_TYPE(epoch_offset_logmetadata_previous_error, true, 
    "Epoch offset unavailable due to error getting log metadata for earlier epochs")
// Epoch offset unavailable due to log metadata unavailable in current epoch
EXPORT_COUNTER_TYPE(epoch_offset_logmetadata_current_notfound, true, 
    "Epoch offset unavailable due to log metadata unavailable in current epoch")
// Epoch offset unavailable due to error in getting log metadata in current epoch
EXPORT_COUNTER_TYPE(epoch_offset_logmetadata_current_error, true, 
    "Epoch offset unavailable due to error in getting log metadata in current epoch")
// Request to retrieve epoch offset through log state
EXPORT_COUNTER_TYPE(epoch_offset_recover_logstate, true, "Request to retrieve epoch offset through log state")

// How many times did we consult Read Throttling framework
EXPORT_COUNTER_TYPE(read_throttling_num_throttle_checks, true, "How many times did we consult Read Throttling framework")
// How many times a read attempt was throttled because of
// throttling limits set in the config
EXPORT_COUNTER_TYPE(read_throttling_num_streams_throttled, true, 
    "How many times a read attempt was throttled because of throttling limits set in the config")
// For how many read streams ReadIoShapingCallback was called
EXPORT_COUNTER_TYPE(read_throttling_num_streams_unthrottled, true, 
    "For how many read streams ReadIoShapingCallback was called")
// How many read streams closed before being unthrottled
EXPORT_COUNTER_TYPE(read_throttling_num_streams_closed_in_throttle, true, "How many read streams closed before being unthrottled")
// How many bytes were read off ReadStorageTask that were administered by
// Read Throttling Framework
EXPORT_COUNTER_TYPE(read_throttling_num_bytes_read, true, "How many bytes were read off ReadStorageTask that were administered by Read Throttling Framework")
EXPORT_COUNTER_TYPE(read_throttling_excess_bytes_debited_from_meter, true, "")
EXPORT_COUNTER_TYPE(read_throttling_excess_bytes_read_from_rocksdb, true, "")

// track outstanding drain() v/s onReadTaskDone
EXPORT_COUNTER_TYPE(read_throttling_num_reads_throttled, true, "")
EXPORT_COUNTER_TYPE(read_throttling_num_reads_allowed, true, "")
EXPORT_COUNTER_TYPE(read_throttling_num_exact_debits, true, "")
EXPORT_COUNTER_TYPE(read_throttling_overflow_bytes, true, "")

// How many bytes were read off ReadStorageTask irrespective of wheter
// throttling is enabled/disabled
EXPORT_COUNTER_TYPE(num_bytes_read_via_read_task, true, 
    "How many bytes were read off ReadStorageTask irrespective of wheter throttling is enabled/disabled")
// How many read storage tasks are issued when Read Throttling is on
EXPORT_COUNTER_TYPE(read_throttling_num_storage_tasks_issued, true, 
    "How many read storage tasks are issued when Read Throttling is on")

// Age in milliseconds of the oldest running log recovery.
EXPORT_COUNTER_TYPE(oldest_recovery_request, false, "Age in milliseconds of the oldest running log recovery.")
// Number of scheduled log recovery requests. Includes queued requests and
// retries for failed recoveries.
EXPORT_COUNTER_TYPE(recovery_scheduled, true, 
    "Number of scheduled log recovery requests. Includes queued requests and retries for failed recoveries.")
// Number of completed log recoveries, regardless of the success of the
// operation.
EXPORT_COUNTER_TYPE(recovery_completed, true, 
    "Number of completed log recoveries, regardless of the success of the operation.")
// Number of log recoveries that are successfully completed
EXPORT_COUNTER_TYPE(recovery_success, true, "Number of log recoveries that are successfully completed")
// Log recovery requests that failed because a later sequencer sealed the log
// through a higher epoch number.
EXPORT_COUNTER_TYPE(recovery_preempted, true, 
    "Log recovery requests that failed because a later sequencer sealed the log through a higher epoch number.")
// Number of failed log recovery requests.
EXPORT_COUNTER_TYPE(recovery_failed, true, "Number of failed log recovery requests.")
// Number of holes in the log identified during the epoch recovery, excluding
// bridge records
EXPORT_COUNTER_TYPE(num_hole_plugs, true, 
    "Number of holes in the log identified during the epoch recovery, excluding bridge records")
// Number of bridge records mutated during epoch recovery
EXPORT_COUNTER_TYPE(num_bridge_records, true, "Number of bridge records mutated during epoch recovery")

// Number of holes or bridge records that recovery didn't plug because it had
// no authoritative digest.
EXPORT_COUNTER_TYPE(num_holes_not_plugged, true, 
    "Number of holes or bridge records that recovery didn't plug because it had no authoritative digest.")

// Number of times epoch recovery received a digest record with checksum error
EXPORT_COUNTER_TYPE(epoch_recovery_digest_checksum_fail, true, 
    "Number of times epoch recovery received a digest record with checksum error")

// number of times the tail record failed to appear in the recovery digest.
// Indicates dataloss, a log being trimmed before it was recovered, or a bug.
EXPORT_COUNTER_TYPE(epoch_recovery_tail_record_not_in_digest, true, 
    "number of times the tail record failed to appear in the recovery digest. Indicates dataloss, a log being trimmed before it was recovered, or a bug.")

// number of times tail record in digest is a hole plug due to bugs or data
// corruption
EXPORT_COUNTER_TYPE(epoch_recovery_tail_record_hole_plug, true, 
    "number of times tail record in digest is a hole plug due to bugs or data corruption")

// number of times that epoch recovery updates its consensus LNG from STARTED
// replies
EXPORT_COUNTER_TYPE(lng_update_digest_started, true, 
    "number of times that epoch recovery updates its consensus LNG from STARTED replies")

// number of data log records mutated by epoch recovery as the best effort but
// not satisfying the replication requirement
EXPORT_COUNTER_TYPE(epoch_recovery_record_underreplication_datalog, true, 
    "number of data log records mutated by epoch recovery as the best effort but not satisfying the replication requirement")

// number of metadatadata log records mutated by epoch recovery as the best
// effort but not satisfying the replication requirement
EXPORT_COUNTER_TYPE(epoch_recovery_record_underreplication_metadatalog, true, 
    "number of metadatadata log records mutated by epoch recovery as the best effort but not satisfying the replication requirement")

// number of Mutators started by epoch recovery
EXPORT_COUNTER_TYPE(mutation_started, true, "number of Mutators started by epoch recovery")
// Number of epochs that were recovered non-authoritatively.
EXPORT_COUNTER_TYPE(non_auth_recovery_epochs, true, 
    "Number of epochs that were recovered non-authoritatively.")
// Number of times an EpochRecovery times out during mutation and cleaning.
EXPORT_COUNTER_TYPE(recovery_mutation_and_cleaning_timeouts, true, "Number of times an EpochRecovery times out during mutation and cleaning.")
// Number of times the EpochRecovery is restarted due to recovery node
// failure during mutation and cleaning stage.
EXPORT_COUNTER_TYPE(epoch_recovery_restarts_on_node_failure, true, "Number of times the EpochRecovery is restarted due to recovery node failure during mutation and cleaning stage.")
// Number of times an EpochRecovery successfully finishes
EXPORT_COUNTER_TYPE(epoch_recovery_success, true, "Number of times an EpochRecovery successfully finishes")

// attempts to activate sequencers
EXPORT_COUNTER_TYPE(sequencer_activations, true, "attempts to activate sequencers")

// Integration tests often want to wait for all sequencers to become quiescent,
// i.e. finish all recoveries, matadata log writes, background reactivations,
// etc, such that the test can be certain that the sequencer is not going to
// suddenly reactivate itself.
//
// Currently the sequencer activation flow is complicated, making quiescence
// difficult to detect reliably. This stat does that. If this stat is zero, it
// is guaranteed that the existing sequencers, if not provoked (e.g. by a node
// going down), won't reactivate or start recovery, will be releasing all
// written records (even if there's no f-majority of available nodes or metadata
// logs are unwritable), and will have historical metadata available (even if
// metadata logs are unreadable). If you do some appends, they'll get
// consecutive LSNs and readers will see them.
//
// Most of the complications come from the fact that the state of sequencer
// activation/recovery/metadata-log-write/background-reactivation-check is
// scattered across lots of obscure places: in-flight EpochStore requests,
// in-flight WriteMetaDataLogRequest-s, Sequencer::state_, in-flight
// LogRecoveryRequest-s, SequencerBackgroundActivator's queue, in-flight
// SequencerBackgroundActivatorRequest-s, and more. If there's any of that in
// flight, we can't be sure that the sequencer won't reactivate soon. This stat
// herds the cats and counts *all* in-flight stuff associated with sequencer
// activations. We aggregate everything into a single stat, instead
// of e.g. polling 'info sequencers' and other commands, to avoid race
// conditions.
//
// Full list of things counted by this stat:
//  - EpochMetaDataUpdateToNextEpoch epoch store requests created by
//    AllSequencers,
//  - LogRecoveryRequest objects,
//  - Sequencer::EpochSequencers::draining != nullptr,
//  - GetHistoricalMetaDataRequest in IMMEDIATE mode,
//  - WriteMetaDataLogRequest,
//  - StartMetaDataLogRecoveryRequest (the actual metadata log recovery, after
//    it's started by this request, is covered by LogRecoveryRequest),
//  - SequencerBackgroundActivatorRequest in flight, for schedule() and
//    notifyCompletion(),
//  - SequencerBackgroundActivator::queue_ and budget_
//  + does *not* count
//    SequencerBackgroundActivator::LogState::reactivation_delay_timer;
//    your sequencers may get reactivated ~60s after config update unless
//    you set sequencer-reactivation-delay-secs to 0.
//
// TODO (#54730759): This can probably be simplified a great deal if we get rid
//                   of the unnecessary sequencer reactivations after metadata
//                   log writes.
EXPORT_COUNTER_TYPE(sequencer_activity_in_progress, false, "")

// sequencer gets activated with epoch metadata incompatible with the
// current configuration.
EXPORT_COUNTER_TYPE(sequencer_activations_incompatible_metadata, true, "sequencer gets activated with epoch metadata incompatible with the current configuration.")

// sequencer is brought up on-demand since the node is the correct node
// to run sequencer
EXPORT_COUNTER_TYPE(sequencer_activations_no_seq_correct_node, true, "sequencer is brought up on-demand since the node is the correct node to run sequencer")
// sequencer is brought up not because the node is the correct node, but
// because the no-redirect flag is set by the client
EXPORT_COUNTER_TYPE(sequencer_activations_no_seq_flag_no_redirect, true, "sequencer is brought up not because the node is the correct node, but because the no-redirect flag is set by the client")
// sequencer is reactivated because it is preempted by itself
EXPORT_COUNTER_TYPE(sequencer_activations_preempted_self, true, "sequencer is reactivated because it is preempted by itself")
// sequencer is preempted by another node, but it needs to be reactivated
// because the client sets the REACTIVATE_IF_PREEMPTED flag
EXPORT_COUNTER_TYPE(sequencer_activations_preempted_flag_reactivate, true, "sequencer is preempted by another node, but it needs to be reactivated because the client sets the REACTIVATE_IF_PREEMPTED flag")
// sequencer is preempted by another node, but it needs to be reactivated
// because the prempted node is considered dead
EXPORT_COUNTER_TYPE(sequencer_activations_preempted_dead, true, "sequencer is preempted by another node, but it needs to be reactivated because the prempted node is considered dead")
// sequencer is reactivated after a record is written in the metadata log
EXPORT_COUNTER_TYPE(sequencer_activations_metadata_record_written, true, "sequencer is reactivated after a record is written in the metadata log")

// how many attempts to activate a sequencer have failed
EXPORT_COUNTER_TYPE(sequencer_activation_failures, true, "how many attempts to activate a sequencer have failed")
// how many attempts to activate a sequencer failed because epoch store was
// empty, but timed out reading metadata log to see if it is empty too
EXPORT_COUNTER_TYPE(sequencer_metadata_log_check_timeouts, true, "how many attempts to activate a sequencer failed because epoch store was empty, but timed out reading metadata log to see if it is empty too")
// how many attempts to activate a sequencer failed because epoch store was
// empty for the given log, but the metadata log was not
EXPORT_COUNTER_TYPE(sequencer_activation_failed_metadata_inconsistency, true, "how many attempts to activate a sequencer failed because epoch store was empty for the given log, but the metadata log was not")

// sequencer gets activated with an epoch metadata that is inconsistent with the
// existing historical metadata maintained by the sequencer
EXPORT_COUNTER_TYPE(sequencer_got_inconsistent_metadata, true, "sequencer gets activated with an epoch metadata that is inconsistent with the existing historical metadata maintained by the sequencer")

// how many times that a sequencer become unavailable because the logid is removed
// from the config
EXPORT_COUNTER_TYPE(sequencer_unavailable_log_removed_from_config, true, "how many times that a sequencer become unavailable because the logid is removed from the config")

// how many times a sequencer became unavailable because the node was isolated
EXPORT_COUNTER_TYPE(sequencer_unavailable_node_isolated, true, "how many times a sequencer became unavailable because the node was isolated")

// how many times a sequencer became unavailable because admin command deactivated it
EXPORT_COUNTER_TYPE(sequencer_unavailable_admin_deactivated, true, "how many times a sequencer became unavailable because admin command deactivated it")

// how many times a sequencer became unavailable because the node stopped being
// a sequencer node
EXPORT_COUNTER_TYPE(sequencer_unavailable_not_sequencer_node, true, "how many times a sequencer became unavailable because the node stopped being a sequencer node")

// how many times sequencer activation gets a result of GRACEFUL_DEFERRED
EXPORT_COUNTER_TYPE(graceful_reactivation_result_deferred, true, "how many times sequencer activation gets a result of GRACEFUL_DEFERRED")
// how many graceful reactivation completion procedure is started
EXPORT_COUNTER_TYPE(graceful_reactivation_completion_started, true, "how many graceful reactivation completion procedure is started")

// The check for sequencer reactivation resulted in no operations
// necessary
EXPORT_COUNTER_TYPE(sequencer_reactivations_noop, true, "The check for sequencer reactivation resulted in no operations necessary")

// Number of times that sequencer reactivations were delayed because there were
// non-substantial reconfigurations.
EXPORT_COUNTER_TYPE(sequencer_reactivations_delayed, true, "Number of times that sequencer reactivations were delayed because there were non-substantial reconfigurations.")
// Number of times that delayed reactivations completed after the delay. This
// should match the previous stat.
EXPORT_COUNTER_TYPE(sequencer_reactivations_delay_completed, true, "Number of times that delayed reactivations completed after the delay. This should match the previous stat.")

// Number of times a sequencer reactivation check recommended delaying
// but there was an already active timer that could be re-used.
EXPORT_COUNTER_TYPE(sequencer_reactivations_delay_timer_reused, true, "Number of times a sequencer reactivation check recommended delaying but there was an already active timer that could be re-used.")

// How many sequencers we reactivated in order to update epoch metadata
// (nodeset, replication factor etc).
EXPORT_COUNTER_TYPE(sequencer_reactivations_for_metadata_update, true, "How many sequencers we reactivated in order to update epoch metadata (nodeset, replication factor etc).")
// How many times we updated epoch metadata without reactivating sequencer.
// This happens when nodes config changes in a way that might change the nodeset
// but actually doesn't.
EXPORT_COUNTER_TYPE(metadata_updates_without_sequencer_reactivation, true, "How many times we updated epoch metadata without reactivating sequencer. This happens when nodes config changes in a way that might change the nodeset but actually doesn't.")

// How many times SequencerBackgroundActivator changed nodeset size based on
// throughput. May be overestimated because the adjustment transaction can fail
// if current sequencer is active but stale.
EXPORT_COUNTER_TYPE(nodeset_adjustments_done, true, "How many times SequencerBackgroundActivator changed nodeset size based on throughput. May be overestimated because the adjustment transaction can fail if current sequencer is active but stale.")
// How many times SequencerBackgroundActivator considered changing nodeset size
// based on throughput but decided against it.
EXPORT_COUNTER_TYPE(nodeset_adjustments_skipped, true, "How many times SequencerBackgroundActivator considered changing nodeset size based on throughput but decided against it.")
// How many times SequencerBackgroundActivator randomized nodeset seed.
// May be overestimated.
EXPORT_COUNTER_TYPE(nodeset_randomizations_done, true, "How many times SequencerBackgroundActivator randomized nodeset seed. May be overestimated.")

//// Per-epoch sequencers

// number of times that sequencer did not finish draining an epoch within the
// configured time
EXPORT_COUNTER_TYPE(sequencer_draining_timedout, true, "Per-epoch sequencers number of times that sequencer did not finish draining an epoch within the configured time")

// how many times approximate findKey tried to be executed on worker thread
EXPORT_COUNTER_TYPE(approximate_find_key_try_non_blocking, true, "how many times approximate findKey tried to be executed on worker thread")
// how many times approximate findKey tried to be executed on worker thread
// but end up doing seeks on disk
EXPORT_COUNTER_TYPE(approximate_find_key_would_block, true, "how many times approximate findKey tried to be executed on worker thread but end up doing seeks on disk")

// how many times strict findKey tried to be executed on worker thread
EXPORT_COUNTER_TYPE(strict_find_key_try_non_blocking, true, "how many times strict findKey tried to be executed on worker thread")
// how many times strict findKey tried to be executed on worker thread
// but end up doing seeks on disk
EXPORT_COUNTER_TYPE(strict_find_key_would_block, true, "how many times strict findKey tried to be executed on worker thread but end up doing seeks on disk")

// How many times findKey storage task timed out during execution
EXPORT_COUNTER_TYPE(findkey_timedout_during_run, true, "How many times findKey storage task timed out during execution")
// How many times findKey storage task timed out before execution
EXPORT_COUNTER_TYPE(findkey_timedout_before_run, true, "How many times findKey storage task timed out before execution")

// The total number of Appenders successfully inserted into appender buffers
EXPORT_COUNTER_TYPE(appenderbuffer_appender_buffered, true, "The total number of Appenders successfully inserted into appender buffers")
// The total number of Appenders that get requeued when processing the buffer
EXPORT_COUNTER_TYPE(appenderbuffer_appender_requeued, true, "The total number of Appenders that get requeued when processing the buffer")
// The number of APPEND requests that failed because an appender had to be
// inserted into a buffer queue, but the queue was full
EXPORT_COUNTER_TYPE(appenderbuffer_appender_failed_queue_full, true, "The number of APPEND requests that failed because an appender had to be inserted into a buffer queue, but the queue was full")
// The number of pending Appenders that have been successfully buffered
// but failed again when it was processed for the second time
EXPORT_COUNTER_TYPE(appenderbuffer_appender_failed_retry, true, "The number of pending Appenders that have been successfully buffered but failed again when it was processed for the second time")
// The number of pending Appenders that have been successfully buffered
// but had to be failed since sequencer activation failed.
EXPORT_COUNTER_TYPE(appenderbuffer_appender_failed_sequencer_activation, true, "The number of pending Appenders that have been successfully buffered but had to be failed since sequencer activation failed.")
// The number of APPEND requests that failed because an appender had to be
// inserted into a buffer queue, but the total size of appenders was above limit
EXPORT_COUNTER_TYPE(appenderbuffer_appender_failed_size_limit, true, "The number of APPEND requests that failed because an appender had to be inserted into a buffer queue, but the total size of appenders was above limit")
// the total number of Appenders removed from appender buffers and successfully
// released to Sequenceres
EXPORT_COUNTER_TYPE(appenderbuffer_appender_released, true, "the total number of Appenders removed from appender buffers and successfully released to Sequenceres")
// the subset of appenderbuffer_appender_released that were previoulsly stored,
// and therefore replied OK and delete the appender.
EXPORT_COUNTER_TYPE(appenderbuffer_appender_deleted, true, "the subset of appenderbuffer_appender_released that were previoulsly stored, and therefore replied OK and delete the appender.")

// (zookeeper epoch store only) number of times zookeeper epoch store encounters
// an internal consistency error
EXPORT_COUNTER_TYPE(zookeeper_epoch_store_internal_inconsistency_error, true, "(zookeeper epoch store only) number of times zookeeper epoch store encounters an internal consistency error")

// PurgeUncleanEpochs instances created and started
EXPORT_COUNTER_TYPE(purging_started, true, "PurgeUncleanEpochs instances created and started")
// PurgeUncleanEpochs instances that started deleting
EXPORT_COUNTER_TYPE(purging_delete_started, true, "PurgeUncleanEpochs instances that started deleting")
// PurgeUncleanEpochs instances that finished deleting
EXPORT_COUNTER_TYPE(purging_delete_done, true, "PurgeUncleanEpochs instances that finished deleting")
// PurgeUncleanEpochs instances that completed
EXPORT_COUNTER_TYPE(purging_done, true, "PurgeUncleanEpochs instances that completed")

// PurgeUncleanEpochs instances that finished successfully
EXPORT_COUNTER_TYPE(purging_success, true, "PurgeUncleanEpochs instances that finished successfully")
// PurgeUncleanEpochs instances that finished with failure
EXPORT_COUNTER_TYPE(purging_failed, true, "PurgeUncleanEpochs instances that finished with failure")

// number of purge jobs created by release enqueued because the limit
// of concurrent purges are reached
EXPORT_COUNTER_TYPE(purging_for_release_enqueued, true, "number of purge jobs created by release enqueued because the limit of concurrent purges are reached")
// number of purge jobs created by release dequeued
EXPORT_COUNTER_TYPE(purging_for_release_dequeued, true, "number of purge jobs created by release dequeued")

// number of PurgeSingleEpoch state machine started
EXPORT_COUNTER_TYPE(purging_v2_purge_epoch_started, true, "number of PurgeSingleEpoch state machine started")
// number of PurgeSingleEpoch state machine completed
EXPORT_COUNTER_TYPE(purging_v2_purge_epoch_completed, true, "number of PurgeSingleEpoch state machine completed")

// number of times PurgeSingleEpoch state machine is completed with the epoch
// considered empty and purged because all fully authoritative nodes have
// responded including at least one of them with E::EMPTY
EXPORT_COUNTER_TYPE(purging_v2_purge_epoch_empty_all_responsed, true, "number of times PurgeSingleEpoch state machine is completed with the epoch considered empty and purged because all fully authoritative nodes have responded including at least one of them with E::EMPTY")

// number of times PurgeSingleEpoch state machine not deleting records
// for the epoch because the epoch is found empty but has a non-zero local
// LNG
EXPORT_COUNTER_TYPE(purge_epoch_skipped_empty_positive_local_lng, true, "number of times PurgeSingleEpoch state machine not deleting records for the epoch because the epoch is found empty but has a non-zero local LNG")

// number of times purge delete records by their key range
EXPORT_COUNTER_TYPE(purging_v2_delete_by_keys, true, "number of times purge delete records by their key range")
// number of times purge delete records by first reading the records
EXPORT_COUNTER_TYPE(purging_v2_delete_by_reading_data, true, "number of times purge delete records by first reading the records")

EXPORT_COUNTER_TYPE(purging_task_dropped, true, "")
// Number of storage tasks queued
EXPORT_COUNTER_TYPE(storage_tasks_queued, true, "Number of storage tasks queued")
// Number of storage tasks dropped
EXPORT_COUNTER_TYPE(storage_tasks_dropped_fast_time_sensitive, true, "Number of storage tasks dropped")
EXPORT_COUNTER_TYPE(storage_tasks_dropped_fast_stallable, true, "")
EXPORT_COUNTER_TYPE(storage_tasks_dropped_slow, true, "")
EXPORT_COUNTER_TYPE(storage_tasks_dropped_default, true, "")
// Number of storage tasks picked up by storage threads
EXPORT_COUNTER_TYPE(storage_tasks_dequeued_fast_time_sensitive, true, "Number of storage tasks picked up by storage threads")
EXPORT_COUNTER_TYPE(storage_tasks_dequeued_fast_stallable, true, "")
EXPORT_COUNTER_TYPE(storage_tasks_dequeued_slow, true, "")
EXPORT_COUNTER_TYPE(storage_tasks_dequeued_default, true, "")

// Number of failures forwarding a message in the delivery chain
EXPORT_COUNTER_TYPE(store_forwarding_failed, true, "Number of failures forwarding a message in the delivery chain")

// Number of times some read iterator was invalidated due to inactivity
EXPORT_COUNTER_TYPE(iterator_invalidations, true, "Number of times some read iterator was invalidated due to inactivity")

// number of waves of STORE messages appenders tried to send through chain
EXPORT_COUNTER_TYPE(appender_wave_chain, true, "number of waves of STORE messages appenders tried to send through chain")
// number of waves appenders tried to send directly to all nodes
EXPORT_COUNTER_TYPE(appender_wave_direct, true, "number of waves appenders tried to send directly to all nodes")
// Appender waves that hit a STORE timeout (and probably sent another wave)
EXPORT_COUNTER_TYPE(appender_wave_timedout, true, "Appender waves that hit a STORE timeout (and probably sent another wave)")
// Appender store timer was reset (because the sync replication scope
// came out of isolation)
EXPORT_COUNTER_TYPE(appender_store_timer_reset, true, "Appender store timer was reset (because the sync replication scope came out of isolation)")
// number of times that an Appender cannot find enough nodes to store copies
// for a record
EXPORT_COUNTER_TYPE(appender_unable_pick_copyset, true, "number of times that an Appender cannot find enough nodes to store copies for a record")
// number of times that an Appender for draining fails to store on a store node
// because the store is preempted by soft seals only
EXPORT_COUNTER_TYPE(appender_draining_soft_preempted, true, "number of times that an Appender for draining fails to store on a store node because the store is preempted by soft seals only")
// number of times that an Appender is started at the beginning of its epoch
// with EPOCH_BEGIN flag in its STORE header
EXPORT_COUNTER_TYPE(appender_epoch_begin, true, "number of times that an Appender is started at the beginning of its epoch with EPOCH_BEGIN flag in its STORE header")
// number of messages that were rejected with write stream unknown error message
EXPORT_COUNTER_TYPE(appender_write_stream_unknown, true, "number of messages that were rejected with write stream unknown error message")
// number of messages that were rejected with write stream broken error message
EXPORT_COUNTER_TYPE(appender_write_stream_broken, true, "number of messages that were rejected with write stream broken error message")
// number of messages that were rejected with write stream ignored error message
EXPORT_COUNTER_TYPE(appender_write_stream_ignored, true, "number of messages that were rejected with write stream ignored error message")

// two important stats above, but exclusively for metadata log appends
EXPORT_COUNTER_TYPE(metadata_log_appender_wave_timedout, true, "")
EXPORT_COUNTER_TYPE(metadata_log_appender_unable_pick_copyset, true, "")

// number of appenders are forcefully aborted because EpochSequencer for its
// epoch is dying
EXPORT_COUNTER_TYPE(appender_aborted_epoch, true, "number of appenders are forcefully aborted because EpochSequencer for its epoch is dying")

// Stats from WeightedCopySetSelector.
EXPORT_COUNTER_TYPE(copyset_selected, true, "Stats from WeightedCopySetSelector.")
EXPORT_COUNTER_TYPE(copyset_biased, true, "Stats from WeightedCopySetSelector.")
EXPORT_COUNTER_TYPE(copyset_selection_failed, true, "Stats from WeightedCopySetSelector.")
EXPORT_COUNTER_TYPE(copyset_selection_attempts, true, "Stats from WeightedCopySetSelector.")
EXPORT_COUNTER_TYPE(copyset_selected_rebuilding, true, "Stats from WeightedCopySetSelector.")
EXPORT_COUNTER_TYPE(copyset_biased_rebuilding, true, "Stats from WeightedCopySetSelector.")
EXPORT_COUNTER_TYPE(copyset_selection_failed_rebuilding, true, "Stats from WeightedCopySetSelector.")
EXPORT_COUNTER_TYPE(copyset_selection_attempts_rebuilding, true, "Stats from WeightedCopySetSelector.")

// Raw bytes and compressed bytes for a given block if the block was selected
// for compressio sampling. The fast stat is based on using a fast compression
// algorithm like lz4 and the slow stat is based on something like zstd.
EXPORT_COUNTER_TYPE(sampled_blocks_raw_bytes_fast, true, "Raw bytes and compressed bytes for a given block if the block was selected for compressio sampling. The fast stat is based on using a fast compression algorithm like lz4 and the slow stat is based on something like zstd.")
EXPORT_COUNTER_TYPE(sampled_blocks_raw_bytes_slow, true, "")
EXPORT_COUNTER_TYPE(sampled_blocks_compressed_bytes_fast, true, "")
EXPORT_COUNTER_TYPE(sampled_blocks_compressed_bytes_slow, true, "")

// A set of stats on distribution of rocksdb block sizes.
EXPORT_COUNTER_TYPE(num_sst_blocks_GT_1024KB, true, "")
EXPORT_COUNTER_TYPE(num_sst_blocks_GT_512KB, true, "")
EXPORT_COUNTER_TYPE(num_sst_blocks_GT_256KB, true, "")
EXPORT_COUNTER_TYPE(num_sst_blocks_GT_128KB, true, "")
EXPORT_COUNTER_TYPE(num_sst_blocks_GT_64KB, true, "")
EXPORT_COUNTER_TYPE(num_sst_blocks_GT_32KB, true, "")
EXPORT_COUNTER_TYPE(num_sst_blocks_LT_32KB, true, "")

// number of times that a storage node replied in STORED header
// with a status code other than E::OK
EXPORT_COUNTER_TYPE(node_stored_unsuccessful_total, true, "number of times that a storage node replied in STORED header with a status code other than E::OK")
// number of times that a storage node replied with E::PREEMPTED
// in STORED header
EXPORT_COUNTER_TYPE(node_stored_preempted_sent, true, "number of times that a storage node replied with E::PREEMPTED in STORED header")
// number of times that a storage node replied with E::DISABLED
// in STORED header
EXPORT_COUNTER_TYPE(node_stored_disabled_sent, true, "number of times that a storage node replied with E::DISABLED in STORED header")
// number of times that a storage node replied with E::DROPPED
// in STORED header
EXPORT_COUNTER_TYPE(node_stored_dropped_sent, true, "number of times that a storage node replied with E::DROPPED in STORED header")
// number of times that a storage node replied with E::NOSPC
// in STORED header
EXPORT_COUNTER_TYPE(node_stored_out_of_space_sent, true, "number of times that a storage node replied with E::NOSPC in STORED header")
// number of times that a storage node replied with E::LOW_ON_SPC
// in STORED header
EXPORT_COUNTER_TYPE(node_stored_low_on_space_sent, true, "number of times that a storage node replied with E::LOW_ON_SPC in STORED header")
// number of times a sequencer received the notification from a
// storage node, that it has crossed lower watermark of free disk space.
EXPORT_COUNTER_TYPE(node_low_on_space_received, true, "number of times a sequencer received the notification from a storage node, that it has crossed lower watermark of free disk space.")
// number of times that a storage node replied with E::NOTSTORAGE
// in STORED header
EXPORT_COUNTER_TYPE(node_stored_not_storage_sent, true, "number of times that a storage node replied with E::NOTSTORAGE in STORED header")
// number of times that a storage node replied with E::REBUILDING
// in STORED header because the copyset contains a node in rebuilding
EXPORT_COUNTER_TYPE(node_stored_rebuilding_sent, true, "number of times that a storage node replied with E::REBUILDING in STORED header because the copyset contains a node in rebuilding")
// number of times that the sequencer received the report that a storage node
// is out of free disk space
EXPORT_COUNTER_TYPE(node_out_of_space_received, true, "number of times that the sequencer received the report that a storage node is out of free disk space")
// number of times that a storage node replied with overloaded flag set
// in STORED header
EXPORT_COUNTER_TYPE(node_overloaded_sent, true, "number of times that a storage node replied with overloaded flag set in STORED header")
// number of times that the sequencer received the report that
// a storage node is overloaded
EXPORT_COUNTER_TYPE(node_overloaded_received, true, "number of times that the sequencer received the report that a storage node is overloaded")
// number of times that the sequencer got E::UNROUTABLE when trying to
// connect to a storage node
EXPORT_COUNTER_TYPE(node_unroutable_received, true, "number of times that the sequencer got E::UNROUTABLE when trying to connect to a storage node")
// number of times that the sequencer got E::DROPPED in STORED header
EXPORT_COUNTER_TYPE(node_dropped_received, true, "number of times that the sequencer got E::DROPPED in STORED header")
// number of times that the sequencer got E::DISABLED in STORED header
EXPORT_COUNTER_TYPE(node_disabled_received, true, "number of times that the sequencer got E::DISABLED in STORED header")
// number of times that the sequencer got E::REBUILDING in STORED header
EXPORT_COUNTER_TYPE(node_rebuilding_received, true, "number of times that the sequencer got E::REBUILDING in STORED header")
// number of times that payloads were corrupted by either a sequencer or
// storage node. Bumped by storage nodes; see log to find which sequencer the
// store was from, then see its log to figure out which node caused it.
// Expected cause is bad hardware
EXPORT_COUNTER_TYPE(payload_corruption, true, "number of times that payloads were corrupted by either a sequencer or storage node. Bumped by storage nodes; see log to find which sequencer the store was from, then see its log to figure out which node caused it. Expected cause is bad hardware")
// number of times a storage node reported corruption to a sequencer node, and
// the sequencer node ignored it. This is either because it is not verifying
// checksums that the client provides - meaning it is most likely the client's
// fault - or the sequencer was shown to corrupt records, but there are too
// many dead nodes to crash it.
EXPORT_COUNTER_TYPE(payload_corruption_ignored, true, "number of times a storage node reported corruption to a sequencer node, and the sequencer node ignored it. This is either because it is not verifying checksums that the client provides - meaning it is most likely the client's fault - or the sequencer was shown to corrupt records, but there are too many dead nodes to crash it.")

// number of times a storage node does not store per-epoch recovery
// metadata since the epoch is empty according to the recovering sequencer
EXPORT_COUNTER_TYPE(epoch_recovery_metadata_not_stored_epoch_empty, true, "number of times a storage node does not store per-epoch recovery metadata since the epoch is empty according to the recovering sequencer")
// number of times a storage node stores or updates per-epoch recovery
// metadata
EXPORT_COUNTER_TYPE(epoch_recovery_metadata_stored, true, "number of times a storage node stores or updates per-epoch recovery metadata")

// number of times a storage node failed to send SEALED reply to a sequencer
// node running log recovery
EXPORT_COUNTER_TYPE(sealed_reply_failed_to_send, true, "number of times a storage node failed to send SEALED reply to a sequencer node running log recovery")

//// stats for record cache
EXPORT_COUNTER_TYPE(record_cache_store_cached, true, "")
EXPORT_COUNTER_TYPE(record_cache_store_not_cached, true, "")
EXPORT_COUNTER_TYPE(record_cache_records_evicted, true, "")
EXPORT_COUNTER_TYPE(record_cache_bytes_cached_estimate, true, "")
EXPORT_COUNTER_TYPE(record_cache_epoch_created, true, "")
EXPORT_COUNTER_TYPE(record_cache_epoch_evicted, true, "")
EXPORT_COUNTER_TYPE(record_cache_epoch_evicted_by_reset, true, "")

// number of times that record cache received a record that cannot be stored
// with the current capacity
EXPORT_COUNTER_TYPE(record_cache_record_out_of_capacity, true, "number of times that record cache received a record that cannot be stored with the current capacity")

// number of times that record cache monitor thead performs size-based
// eviction because the number of bytes cached exceed the limit
EXPORT_COUNTER_TYPE(record_cache_eviction_performed_by_monitor, true, "number of times that record cache monitor thead performs size-based eviction because the number of bytes cached exceed the limit")
// estimate number of payload bytes evicted by the eviction monitor thread
EXPORT_COUNTER_TYPE(record_cache_bytes_evicted_by_monitor, true, "estimate number of payload bytes evicted by the eviction monitor thread")


// for calculating cache hit rate
EXPORT_COUNTER_TYPE(epoch_recovery_digest_received, true, "")
EXPORT_COUNTER_TYPE(record_cache_digest_hit, true, "")
EXPORT_COUNTER_TYPE(record_cache_digest_miss, true, "")
EXPORT_COUNTER_TYPE(log_recovery_seal_received, true, "")
EXPORT_COUNTER_TYPE(record_cache_seal_hit, true, "")
EXPORT_COUNTER_TYPE(record_cache_seal_miss, true, "")

EXPORT_COUNTER_TYPE(record_cache_digest_hit_datalog, true, "")
EXPORT_COUNTER_TYPE(record_cache_digest_miss_datalog, true, "")
EXPORT_COUNTER_TYPE(record_cache_seal_hit_datalog, true, "")
EXPORT_COUNTER_TYPE(record_cache_seal_miss_datalog, true, "")

// on the read side
EXPORT_COUNTER_TYPE(record_cache_digest_created, true, "")
EXPORT_COUNTER_TYPE(record_cache_digest_completed, true, "")
EXPORT_COUNTER_TYPE(record_cache_digest_active, true, "")

EXPORT_COUNTER_TYPE(record_cache_digest_record_sent, true, "")
EXPORT_COUNTER_TYPE(record_cache_digest_payload_bytes_sent, true, "")

// Number of records written to storage with the sticky copyset bit set
EXPORT_COUNTER_TYPE(csi_entry_writes, true, "Number of records written to storage with the sticky copyset bit set")

// Number of records written to storage for the index
EXPORT_COUNTER_TYPE(index_entry_writes, true, "Number of records written to storage for the index")

// number of rejected APPENDS because the logid is not in the config of the
// sequencer node
EXPORT_COUNTER_TYPE(append_rejected_not_in_server_config, true, "number of rejected APPENDS because the logid is not in the config of the sequencer node")
// number of rejected APPENDS because there are too many out-of-space nodes
EXPORT_COUNTER_TYPE(append_rejected_nospace, true, "number of rejected APPENDS because there are too many out-of-space nodes")
// number of rejected APPENDS because there are too many overloaded nodes
EXPORT_COUNTER_TYPE(append_rejected_overloaded, true, "number of rejected APPENDS because there are too many overloaded nodes")
// number of rejected APPENDS because there are too many unroutable nodes
EXPORT_COUNTER_TYPE(append_rejected_unroutable, true, "number of rejected APPENDS because there are too many unroutable nodes")
// number of rejected APPENDS because too many nodes are rebuilding
EXPORT_COUNTER_TYPE(append_rejected_rebuilding, true, "number of rejected APPENDS because too many nodes are rebuilding")
// number of rejected APPENDS because too many nodes have persistent error
EXPORT_COUNTER_TYPE(append_rejected_disabled, true, "number of rejected APPENDS because too many nodes have persistent error")
// number of rejected APPENDS because sequencer was not found
EXPORT_COUNTER_TYPE(append_rejected_nosequencer, true, "number of rejected APPENDS because sequencer was not found")
// number of rejected APPENDS because the sliding window of Appenders was full
EXPORT_COUNTER_TYPE(append_rejected_window_full, true, "number of rejected APPENDS because the sliding window of Appenders was full")
// number of rejected APPENDS because of Settings::max_total_appenders_size_hard
EXPORT_COUNTER_TYPE(append_rejected_size_limit, true, "number of rejected APPENDS because of Settings::max_total_appenders_size_hard")
// number of rejected APPENDS because of too many pending appenders
EXPORT_COUNTER_TYPE(append_rejected_pending_full, true, "number of rejected APPENDS because of too many pending appenders")
// number of rejected APPENDS because the server was shutting down
EXPORT_COUNTER_TYPE(append_rejected_shutdown, true, "number of rejected APPENDS because the server was shutting down")
// number of APPENDS that were rejected because they require sequencer to be
// reactivated, but the limit has been exceeded
EXPORT_COUNTER_TYPE(append_rejected_reactivation_limit, true, "number of APPENDS that were rejected because they require sequencer to be reactivated, but the limit has been exceeded")
// number of appends that were rejected because the node itself is marked
// as unavailable by the failure detector
EXPORT_COUNTER_TYPE(append_rejected_not_ready, true, "number of appends that were rejected because the node itself is marked as unavailable by the failure detector")
// number of appends that were rejected because the LogsConfig is not fully
// loaded yet. Client should retry.
EXPORT_COUNTER_TYPE(append_rejected_logsconfig_not_ready, true, "number of appends that were rejected because the LogsConfig is not fully loaded yet. Client should retry.")
// number of rejected APPENDS becasue of invalid permissions
EXPORT_COUNTER_TYPE(append_rejected_permission_denied, true, "number of rejected APPENDS becasue of invalid permissions")
// number of appends that were rejected due to the seuqencer being isolated
// meaning that more than half of the nodes appear to be dead from a failure
// detector point of view.
EXPORT_COUNTER_TYPE(append_rejected_isolated, true, "number of appends that were rejected due to the seuqencer being isolated meaning that more than half of the nodes appear to be dead from a failure detector point of view.")
// number of APPENDS rejected because they were cancelled at some point.  The
// append may or may not have succeeded.
EXPORT_COUNTER_TYPE(append_rejected_cancelled, true, "number of APPENDS rejected because they were cancelled at some point.  The append may or may not have succeeded.")

// number of rocksdb manual compaction performed
EXPORT_COUNTER_TYPE(manual_compactions, true, "number of rocksdb manual compaction performed")

// How many times we reloaded the TLS ticket seeds
EXPORT_COUNTER_TYPE(tls_ticket_seeds_reloaded, true, "How many times we reloaded the TLS ticket seeds")

EXPORT_COUNTER_TYPE(server_read_streams_created, true, "")

// Total number of records read by LocalLogStoreReader for all read streams.
EXPORT_COUNTER_TYPE(read_streams_num_records_read, true, "Total number of records read by LocalLogStoreReader for all read streams.")
EXPORT_COUNTER_TYPE(read_streams_num_bytes_read, true, "")
EXPORT_COUNTER_TYPE(read_streams_num_record_bytes_read, true, "")
EXPORT_COUNTER_TYPE(read_streams_num_csi_bytes_read, true, "")
// Total size of rocksdb blocks read from disk by LocalLogStoreReader.
EXPORT_COUNTER_TYPE(read_streams_block_bytes_read, true, "Total size of rocksdb blocks read from disk by LocalLogStoreReader.")

// Total number of records filtered by LocalLogStoreReader for all read streams.
EXPORT_COUNTER_TYPE(read_streams_num_records_filtered, true, "Total number of records filtered by LocalLogStoreReader for all read streams.")
// Total number of bytes filtered by LocalLogStoreReader for all read streams.
EXPORT_COUNTER_TYPE(read_streams_num_bytes_filtered, true, "Total number of bytes filtered by LocalLogStoreReader for all read streams.")

// Number of records filtered out by RebuildingReadStorageTask.
EXPORT_COUNTER_TYPE(rebuilding_num_records_filtered, true, "Number of records filtered out by RebuildingReadStorageTask.")
// Number of bytes filtered out by RebuildingReadStorageTask.
EXPORT_COUNTER_TYPE(rebuilding_num_bytes_filtered, true, "Number of bytes filtered out by RebuildingReadStorageTask.")
// Number of records filtered out by RebuildingReadStorageTask which required
// information only available after the full record was read (as opposed to
// information from the copyset index).
EXPORT_COUNTER_TYPE(rebuilding_num_records_late_filtered, true, "Number of records filtered out by RebuildingReadStorageTask which required information only available after the full record was read (as opposed to information from the copyset index).")

// The number of copyset index entries that LocalLogStoreReader read.
EXPORT_COUNTER_TYPE(read_streams_num_csi_entries_read, true, "The number of copyset index entries that LocalLogStoreReader read.")
// The number of copyset index entries that were filtered out by ReadFilter
// in LocalLogStoreReader
EXPORT_COUNTER_TYPE(read_streams_num_csi_entries_filtered, true, "The number of copyset index entries that were filtered out by ReadFilter in LocalLogStoreReader")
// The number of copyset index entries that passed the ReadFilter
// in LocalLogStoreReader
EXPORT_COUNTER_TYPE(read_streams_num_csi_entries_sent, true, "The number of copyset index entries that passed the ReadFilter in LocalLogStoreReader")
// The number of rocksdb::Iterators created on the copyset index
EXPORT_COUNTER_TYPE(read_streams_num_csi_iterators_created, true, "The number of rocksdb::Iterators created on the copyset index")
// The number of rocksdb::Iterators on the copyset index that were destroyed
// when CopySetIndexIterator got destroyed
EXPORT_COUNTER_TYPE(read_streams_num_csi_iterators_destroyed, true, "The number of rocksdb::Iterators on the copyset index that were destroyed when CopySetIndexIterator got destroyed")

// When considering real time reads, the number of cached records we dropped
// because they were from a different epoch than our current read pointer.
EXPORT_COUNTER_TYPE(real_time_records_from_wrong_epoch, true, "When considering real time reads, the number of cached records we dropped because they were from a different epoch than our current read pointer.")

// The approximate number of records evicted because our cache was too big,
// summed over all streams.
EXPORT_COUNTER_TYPE(real_time_record_buffer_eviction, true, "The approximate number of records evicted because our cache was too big, summed over all streams.")

// Number of sent records that came real time, i.e. on release were sent from
// the writer to the reader, and never read from RocksDB.
EXPORT_COUNTER_TYPE(read_streams_records_real_time, true, "Number of sent records that came real time, i.e. on release were sent from the writer to the reader, and never read from RocksDB.")
// Number of sent records that were read from RocksDB using non blocking reads.
EXPORT_COUNTER_TYPE(read_streams_records_non_blocking, true, "Number of sent records that were read from RocksDB using non blocking reads.")
// Number of sent records that came from blocking reads of RocksDB.
EXPORT_COUNTER_TYPE(read_streams_records_blocking, true, "Number of sent records that came from blocking reads of RocksDB.")
// Same but in bytes.
EXPORT_COUNTER_TYPE(read_streams_bytes_real_time, true, "Number of sent bytes that came real time, i.e. on release were sent from the writer to the reader, and never read from RocksDB.")
EXPORT_COUNTER_TYPE(read_streams_bytes_non_blocking, true, "Number of sent bytes that were read from RocksDB using non blocking reads.")
EXPORT_COUNTER_TYPE(read_streams_bytes_blocking, true, "Number of sent bytes that came from blocking reads of RocksDB.")

// Number of times the previous record sent did NOT come from the real time
// buffer, and the current record is from it.
EXPORT_COUNTER_TYPE(real_time_switched_to_real_time, true, "Number of times the previous record sent did NOT come from the real time buffer, and the current record is from it.")
// Number of times the previous record sent was read from RocksDB NOT using non
// blocking I/O, and the current record DID read from RocksDB using non blocking
// I/O.
EXPORT_COUNTER_TYPE(real_time_switched_to_non_blocking, true, "Number of times the previous record sent was read from RocksDB NOT using non blocking I/O, and the current record DID read from RocksDB using non blocking I/O.")
// Number of times the previous record sent was read from RocksDB NOT using
// blocking I/O, and the current record DID use blocking I/O.
EXPORT_COUNTER_TYPE(real_time_switched_to_blocking, true, "Number of times the previous record sent was read from RocksDB NOT using blocking I/O, and the current record DID use blocking I/O.")

// Number of times, when we went to get records to send, that there were no real
// time records.
EXPORT_COUNTER_TYPE(real_time_no_released_metadata, true, "Number of times, when we went to get records to send, that there were no real time records.")
EXPORT_COUNTER_TYPE(real_time_no_released_regular, true, "Number of times, when we went to get records to send, that there were no real time records.")

// Number of times we got released records out-of-order.
EXPORT_COUNTER_TYPE(real_time_out_of_order_metadata, true, "Number of times we got released records out-of-order.")
EXPORT_COUNTER_TYPE(real_time_out_of_order_regular, true, "Number of times we got released records out-of-order.")

// Number of times the recently released records were newer than we needed.
EXPORT_COUNTER_TYPE(real_time_too_new_metadata, true, "Number of times the recently released records were newer than we needed.")
EXPORT_COUNTER_TYPE(real_time_too_new_regular, true, "Number of times the recently released records were newer than we needed.")

//////////////////////////RocksDB LocalLogStore stats///////////////////////////

#define ITERATOR_EXPORT_OP_STATS(op) \
EXPORT_COUNTER_TYPE(read_streams_rocksdb_locallogstore_ ## op ## _reads, true, "") \
EXPORT_COUNTER_TYPE(read_streams_rocksdb_locallogstore_ ## op ## _reads_from_disk, true, "") \
EXPORT_COUNTER_TYPE(read_streams_rocksdb_locallogstore_ ## op ## _block_bytes_read_from_disk, true, "") \
EXPORT_COUNTER_TYPE(read_streams_rocksdb_locallogstore_ ## op ## _blocks_read_from_disk, true, "") \
EXPORT_COUNTER_TYPE(read_streams_rocksdb_locallogstore_ ## op ## _reads_from_block_cache, true, "") \
EXPORT_COUNTER_TYPE(read_streams_rocksdb_locallogstore_ ## op ## _blocks_read_from_block_cache, true, "") \

// Stats for Seek() calls on the copyset index rocksdb::Iterator
ITERATOR_EXPORT_OP_STATS(csi_seek)
// Stats for Next() calls on the copyset index rocksdb::Iterator
ITERATOR_EXPORT_OP_STATS(csi_next)
// Stats for Prev() calls on the copyset index rocksdb::Iterator
ITERATOR_EXPORT_OP_STATS(csi_prev)

// Stats for Seek() calls on the record store rocksdb::Iterator
ITERATOR_EXPORT_OP_STATS(record_seek)
// Stats for Next() calls on the record store rocksdb::Iterator
ITERATOR_EXPORT_OP_STATS(record_next)
// Stats for Prev() calls on the record store index rocksdb::Iterator
ITERATOR_EXPORT_OP_STATS(record_prev)
////////////////////////////////////////////////////////////////////////////////

// TODO (#10357210): remove
EXPORT_COUNTER_TYPE(data_key_format_migration_steps, false, "")
EXPORT_COUNTER_TYPE(data_key_format_migration_merges, false, "")

// Number of calls to LocalLogStoreReader::read()
EXPORT_COUNTER_TYPE(read_streams_num_ops, true, "Number of calls to LocalLogStoreReader::read()")
// Number of times we encounter a transient errors while serving reads
EXPORT_COUNTER_TYPE(read_streams_transient_errors, true, "Number of times we encounter a transient errors while serving reads")

// Number of bytes we enqueued to a reader while a storage task (for a different
// read stream) is outstanding.
EXPORT_COUNTER_TYPE(bytes_queued_during_storage_task, true, "Number of bytes we enqueued to a reader while a storage task (for a different read stream) is outstanding.")

// Total number of successfully started WriteMetaDataRecord state machines
EXPORT_COUNTER_TYPE(write_metadata_record_started, true, "Total number of successfully started WriteMetaDataRecord state machines")
// Total number of successfully finished WriteMetaDataRecord state machines
EXPORT_COUNTER_TYPE(write_metadata_record_finished, true, "Total number of successfully finished WriteMetaDataRecord state machines")

// Total number of failed attempts to write metadata log
EXPORT_COUNTER_TYPE(write_metadata_request_failed, true, "Total number of failed attempts to write metadata log")
// Total number of failed attempts to update EpochStore after writing metadata
// entry to log
EXPORT_COUNTER_TYPE(write_metadata_epoch_store_failed, true, "Total number of failed attempts to update EpochStore after writing metadata entry to log")

// Number of times a partition was created, dropped and compacted
EXPORT_COUNTER_TYPE(partitions_created, true, "")
EXPORT_COUNTER_TYPE(partitions_prepended, true, "")
EXPORT_COUNTER_TYPE(partitions_dropped, true, "")
EXPORT_COUNTER_TYPE(partitions_compacted, true, "")
// Total number of milliseconds spent compacting partitions.
EXPORT_COUNTER_TYPE(partitions_compaction_time, true, "Total number of milliseconds spent compacting partitions.")
// Total number of milliseconds spent cleaning up directory after compaction.
EXPORT_COUNTER_TYPE(partitions_compaction_cleanup_time, true, "Total number of milliseconds spent cleaning up directory after compaction.")
// Total number of milliseconds spent partially compacting partitions.
EXPORT_COUNTER_TYPE(partitions_partial_compaction_time, true, "Total number of milliseconds spent partially compacting partitions.")
// Total number of files partially compacted
EXPORT_COUNTER_TYPE(partitions_partial_compaction_files, true, "")
EXPORT_COUNTER_TYPE(partition_proactive_compactions, true, "")
EXPORT_COUNTER_TYPE(partition_manual_compactions, true, "")
EXPORT_COUNTER_TYPE(partition_partial_compactions, true, "")
// Partition Dirty State Tracking
EXPORT_COUNTER_TYPE(partition_cleaner_scans, true, "")
EXPORT_COUNTER_TYPE(partition_marked_clean, true, "")
EXPORT_COUNTER_TYPE(partition_marked_dirty, true, "")
EXPORT_COUNTER_TYPE(partition_dirty_data_updated, true, "")
EXPORT_COUNTER_TYPE(partition_sync_write_promotion_for_timstamp, true, "")
EXPORT_COUNTER_TYPE(triggered_manual_memtable_flush, true, "")

// A MEMORY or ASYNC_WRITE was promoted to a SYNC write due
// to lacking a record timestamp.
EXPORT_COUNTER_TYPE(sync_write_promotion_no_timestamp, true, "A MEMORY or ASYNC_WRITE was promoted to a SYNC write due to lacking a record timestamp.")
// A MEMORY or ASYNC_WRITE was promoted to a SYNC write due
// to lacking coordinator (the node that issued the write) information.
EXPORT_COUNTER_TYPE(sync_write_promotion_no_coordinator, true, "A MEMORY or ASYNC_WRITE was promoted to a SYNC write due to lacking coordinator (the node that issued the write) information.")

// Number of records that were stored in some partition other than the two most
// recent ones. Expect high values during rebuilding.
EXPORT_COUNTER_TYPE(logsdb_writes_to_old_partitions, true, "Number of records that were stored in some partition other than the two most recent ones. Expect high values during rebuilding.")
EXPORT_COUNTER_TYPE(logsdb_iterator_dir_seek_needed, true, "")
// Various relatively rare conditions in LogsDB.
EXPORT_COUNTER_TYPE(logsdb_partition_pick_retries, true, "")
EXPORT_COUNTER_TYPE(logsdb_directory_premature_flushes, true, "")
EXPORT_COUNTER_TYPE(logsdb_writes_dir_key_decrease, true, "")
EXPORT_COUNTER_TYPE(logsdb_writes_dir_key_add, true, "")
EXPORT_COUNTER_TYPE(logsdb_skipped_writes, true, "")
EXPORT_COUNTER_TYPE(logsdb_target_partition_clamped, true, "")
EXPORT_COUNTER_TYPE(logsdb_iterator_dir_reseek_needed, true, "")
EXPORT_COUNTER_TYPE(logsdb_iterator_partition_dropped, true, "")

// Number of append messages processed due to the NO_REDIRECT flag
EXPORT_COUNTER_TYPE(append_no_redirect, true, "Number of append messages processed due to the NO_REDIRECT flag")
// Number of append messages processed due to the REACTIVATE_IF_PREEMPTED flag
EXPORT_COUNTER_TYPE(append_reactivate_if_preempted, true, "Number of append messages processed due to the REACTIVATE_IF_PREEMPTED flag")
// Number of append messages processed due to the preemptor node being dead
EXPORT_COUNTER_TYPE(append_preempted_dead, true, "Number of append messages processed due to the preemptor node being dead")
// Number of redirects sent to nodes that are not in the current config
EXPORT_COUNTER_TYPE(append_redir_not_in_config, true, "Number of redirects sent to nodes that are not in the current config")

// How many times a GET_SEQ_STATE message is received for a log the node is not
// running a sequencer for.
EXPORT_COUNTER_TYPE(get_seq_state_nosequencer, true, "How many times a GET_SEQ_STATE message is received for a log the node is not running a sequencer for.")
// Number of times redirect was sent as a reply to GET_SEQ_STATE
EXPORT_COUNTER_TYPE(get_seq_state_redirect, true, "Number of times redirect was sent as a reply to GET_SEQ_STATE")
// Number of GET_SEQ_STATE messages received with NO_REDIRECT
EXPORT_COUNTER_TYPE(get_seq_state_no_redirect, true, "Number of GET_SEQ_STATE messages received with NO_REDIRECT")
// Number of GET_SEQ_STATE messages received with REACTIVATE_IF_PREEMPTED
EXPORT_COUNTER_TYPE(get_seq_state_reactivate_if_preempted, true, "Number of GET_SEQ_STATE messages received with REACTIVATE_IF_PREEMPTED")
// Number of times GET_SEQ_STATE message reactivated a sequencer because
// its preemptor is dead
EXPORT_COUNTER_TYPE(get_seq_state_reactivate_preemptor_dead, true, "Number of times GET_SEQ_STATE message reactivated a sequencer because its preemptor is dead")
// Number of times GET_SEQ_STATE message reactivated a sequencer because
// its preemptor is boycotted
EXPORT_COUNTER_TYPE(get_seq_state_reactivate_preemptor_boycotted, true, "Number of times GET_SEQ_STATE message reactivated a sequencer because its preemptor is boycotted")

// Number of times a GET_SEQ_STATE would have returned result from
// a stale sequencer(older epoch), had it not sent out CheckSealRequest
// to determine if another sequencer is activated with a higher epoch
// somewhere else.
EXPORT_COUNTER_TYPE(get_seq_state_stale_sequencer, true, "Number of times a GET_SEQ_STATE would have returned result from a stale sequencer(older epoch), had it not sent out CheckSealRequest to determine if another sequencer is activated with a higher epoch somewhere else.")

// Number of times a sequencer was found INACTIVE,
// but preempted_epoch_ was not set.
//
// Due to sending a CHECK_SEAL message after receiving a
// GET_SEQ_STATE, the preempted_epoch_ is now set.
EXPORT_COUNTER_TYPE(get_seq_state_set_preempted_epoch, true, "Number of times a sequencer was found INACTIVE, but preempted_epoch_ was not set.  Due to sending a CHECK_SEAL message after receiving a GET_SEQ_STATE, the preempted_epoch_ is now set.")

// Number of times a CheckSealRequest timedout
EXPORT_COUNTER_TYPE(check_seal_req_timedout, true, "Number of times a CheckSealRequest timedout")

// Number of times a CHECK_SEAL_Message had to recover
// LogStorageState from RocksdDB
EXPORT_COUNTER_TYPE(check_seal_req_recover_seal, true, "Number of times a CHECK_SEAL_Message had to recover LogStorageState from RocksdDB")

// How many times a GET_SEQ_STATE has to be retried
// since copyset manager is not available.
EXPORT_COUNTER_TYPE(check_seal_req_copysetmanager_invalid, true, "How many times a GET_SEQ_STATE has to be retried since copyset manager is not available.")

// Number of reactivations, meant to prevent out-of-order appends, caused by
// clients notifying us that a sequencer with a higher epoch exists (or
// existed), i.e. with seen_epoch higher than sequencer's current epoch
EXPORT_COUNTER_TYPE(stale_reactivation, true, "Number of reactivations, meant to prevent out-of-order appends, caused by clients notifying us that a sequencer with a higher epoch exists (or existed), i.e. with seen_epoch higher than sequencer's current epoch")
// Number of reactivations due to exhastion of the ESN space within the current
// epoch.
EXPORT_COUNTER_TYPE(epoch_end_reactivation, true, "Number of reactivations due to exhastion of the ESN space within the current epoch.")

// Number of read batches of records read for all read streams.
EXPORT_COUNTER_TYPE(read_streams_batch_complete, true, "Number of read batches of records read for all read streams.")
// Total number of microseconds spent processing batches.
// Dividing this by read_streams_batch_complete gives the average batch
// processing time.
EXPORT_COUNTER_TYPE(read_streams_batch_processing_microsec, true, "Total number of microseconds spent processing batches. Dividing this by read_streams_batch_complete gives the average batch processing time.")
// Total number of microseconds spent waiting on the catchup queue.
// Dividing this by read_streams_batch_complete gives the average time waiting
// in catchup queue before processing a batch.
EXPORT_COUNTER_TYPE(read_streams_batch_queue_microsec, true, "Total number of microseconds spent waiting on the catchup queue. Dividing this by read_streams_batch_complete gives the average time waiting in catchup queue before processing a batch.")

EXPORT_COUNTER_TYPE(wal_syncs, true, "")
EXPORT_COUNTER_TYPE(wal_sync_microsec, true, "")
EXPORT_COUNTER_TYPE(fsyncs, true, "")
EXPORT_COUNTER_TYPE(fsync_microsec, true, "")
EXPORT_COUNTER_TYPE(fdatasyncs, true, "")
EXPORT_COUNTER_TYPE(fdatasync_microsec, true, "")

EXPORT_COUNTER_TYPE(rebuilding_store_sent, true, "")
EXPORT_COUNTER_TYPE(rebuilding_amend_sent, true, "")
EXPORT_COUNTER_TYPE(rebuilding_donor_stored_ok, true, "")
EXPORT_COUNTER_TYPE(rebuilding_donor_amended_ok, true, "")
EXPORT_COUNTER_TYPE(rebuilding_recipient_stored_ok, true, "")
EXPORT_COUNTER_TYPE(rebuilding_recipient_amended_ok, true, "")
EXPORT_COUNTER_TYPE(rebuilding_malformed_records, true, "")
EXPORT_COUNTER_TYPE(rebuilding_bad_copysets, true, "")
EXPORT_COUNTER_TYPE(record_rebuilding_retries, true, "")
EXPORT_COUNTER_TYPE(record_rebuilding_store_timeouts, true, "")
EXPORT_COUNTER_TYPE(record_rebuilding_amend_in_progress, true, "")
EXPORT_COUNTER_TYPE(record_rebuilding_amend_timeouts, true, "")
EXPORT_COUNTER_TYPE(record_rebuilding_amend_retries, true, "")
EXPORT_COUNTER_TYPE(record_rebuilding_amend_failed, true, "")

// How many times we've seen an amend pseudorecord without an corresponding
// full record.
EXPORT_COUNTER_TYPE(dangling_amends_seen, true, "How many times we've seen an amend pseudorecord without an corresponding full record.")
// How many times we've seen a record with its corresponding CSI entry such
// that the copyset or flags in the CSI entry are different from those in the
// record.
EXPORT_COUNTER_TYPE(csi_contents_mismatch, true, "How many times we've seen a record with its corresponding CSI entry such that the copyset or flags in the CSI entry are different from those in the record.")
// Number of times we saw a large range of records/csi-entries without
// corresponding csi-entries/records.
EXPORT_COUNTER_TYPE(csi_unexpectedly_long_skips, true, "Number of times we saw a large range of records/csi-entries without corresponding csi-entries/records.")
// Number of CSI entries that CSIWrapper has skipped because no matching record
// was found
EXPORT_COUNTER_TYPE(read_streams_num_csi_skips_no_record, true, "Number of CSI entries that CSIWrapper has skipped because no matching record was found")

EXPORT_COUNTER_TYPE(gossips_received_on_worker_thread, true, "")
EXPORT_COUNTER_TYPE(gossips_received_on_gossip_thread, true, "")

// How many gossip messages were delayed on recipient
// by atleast 1 second.
EXPORT_COUNTER_TYPE(gossips_delayed_total, true, "How many gossip messages were delayed on recipient by atleast 1 second.")

// How many gossip messages were dropped since they were delayed
// by >= 'gossip_time_skew_threshold' on receiving side.
EXPORT_COUNTER_TYPE(gossips_dropped_total, true, "How many gossip messages were dropped since they were delayed by >= 'gossip_time_skew_threshold' on receiving side.")

// How many gossip messages were rejected because the incoming message
// contains a lower instance id for the sender node than the recipient
// already knows
EXPORT_COUNTER_TYPE(gossips_rejected_instance_id, true, "How many gossip messages were rejected because the incoming message contains a lower instance id for the sender node than the recipient already knows")

// How many gossip messages were delayed because of sitting in Pipe
EXPORT_COUNTER_TYPE(gossips_delayed_pipe, true, "How many gossip messages were delayed because of sitting in Pipe")

// How many times the failure detector failed to send gossip messages
EXPORT_COUNTER_TYPE(gossips_failed_to_send, true, "How many times the failure detector failed to send gossip messages")

// How many times the failure detector failed to send gossip messages to an alive node
EXPORT_COUNTER_TYPE(gossips_failed_to_send_to_alive_nodes, true, "How many times the failure detector failed to send gossip messages to an alive node")

// How many times a node didn't process gossips for
// 'gossip_intervals_without_processing_threshold' intervals.
EXPORT_COUNTER_TYPE(gossips_failed_to_process, true, "How many times a node didn't process gossips for 'gossip_intervals_without_processing_threshold' intervals.")

// Total number of nodes expected to be seen (including self)
EXPORT_COUNTER_TYPE(num_nodes, true, "Total number of nodes expected to be seen (including self)")
// Effective number of nodes in the cluster, excluding disabled nodes
EXPORT_COUNTER_TYPE(effective_num_nodes, false, "Effective number of nodes in the cluster, excluding disabled nodes")

// How many nodes are marked DEAD by the FailureDetector.
// This measures whether the FailureDetector state is in
// sync across all nodes.
EXPORT_COUNTER_TYPE(num_dead_nodes, false, "How many nodes are marked DEAD by the FailureDetector. This measures whether the FailureDetector state is in sync across all nodes.")

// Effective number of dead nodes, excluding disabled nodes
EXPORT_COUNTER_TYPE(effective_dead_nodes, false, "Effective number of dead nodes, excluding disabled nodes")

// How many times the watchdog took more than
// expected time to enter stall detection loop.
EXPORT_COUNTER_TYPE(watchdog_num_delays, true, "How many times the watchdog took more than expected time to enter stall detection loop.")

// Num workers detected as stalled by watchdog
EXPORT_COUNTER_TYPE(num_stalled_workers, true, "Num workers detected as stalled by watchdog")

// How many nodes are marked OVERLOADED by the FailureDetector.
EXPORT_COUNTER_TYPE(num_overloaded_nodes, false, "How many nodes are marked OVERLOADED by the FailureDetector.")

// How many nodes are marked UNHEALTHY by the FailureDetector.
EXPORT_COUNTER_TYPE(num_unhealthy_nodes, false, "How many nodes are marked UNHEALTHY by the FailureDetector.")
// Effective number of unhealthy nodes, excluding disabled nodes
EXPORT_COUNTER_TYPE(effective_unhealthy_nodes, false, "Effective number of unhealthy nodes, excluding disabled nodes")

// Increases for every HM loop in which the node is found to be overloaded
EXPORT_COUNTER_TYPE(health_monitor_overloaded, true, "Increases for every HM loop in which the node is found to be overloaded")
// Increases for every HM loop in which the node is found to be unhealthy
EXPORT_COUNTER_TYPE(health_monitor_unhealthy, true, "Increases for every HM loop in which the node is found to be unhealthy")

// Number of records from the event log that EventLogReader has seen so far.
EXPORT_COUNTER_TYPE(num_event_log_records_read, true, "Number of records from the event log that EventLogReader has seen so far.")
EXPORT_COUNTER_TYPE(malformed_event_log_records_read, true, "")

// Total size of records and copyset index entries written to rocksdb.
EXPORT_COUNTER_TYPE(csi_bytes_written, true, "Total size of records and copyset index entries written to rocksdb.")
EXPORT_COUNTER_TYPE(record_bytes_written, true, "")
EXPORT_COUNTER_TYPE(index_bytes_written, true, "")

// Number and total size of all rocksdb blocks written to sst files.
// Only when RocksDBFlushBlockPolicy is used. In particular, metadata column
// family is excluded because it doesn't use RocksDBFlushBlockPolicy.
EXPORT_COUNTER_TYPE(sst_blocks_written, true, "Number and total size of all rocksdb blocks written to sst files. Only when RocksDBFlushBlockPolicy is used. In particular, metadata column family is excluded because it doesn't use RocksDBFlushBlockPolicy.")
EXPORT_COUNTER_TYPE(sst_blocks_bytes, true, "")
// Number and size of blocks consisting of data records.
EXPORT_COUNTER_TYPE(sst_record_blocks_written, true, "Number and size of blocks consisting of data records.")
EXPORT_COUNTER_TYPE(sst_record_blocks_bytes, true, "")

// Approximate breakdown of the (uncompressed) size of sst files written by
// rocksdb (both flushes and compactions). Metadata column family excluded.
// Can be used to estimate space overhead of the various headers and indexes.
EXPORT_COUNTER_TYPE(sst_bytes_payload, true, "Approximate breakdown of the (uncompressed) size of sst files written by rocksdb (both flushes and compactions). Metadata column family excluded. Can be used to estimate space overhead of the various headers and indexes.")
EXPORT_COUNTER_TYPE(sst_bytes_record_header, true, "")
EXPORT_COUNTER_TYPE(sst_bytes_csi, true, "")
EXPORT_COUNTER_TYPE(sst_bytes_index, true, "")
EXPORT_COUNTER_TYPE(sst_bytes_other, true, "")

// Number of shards scheduled for rebuilding
EXPORT_COUNTER_TYPE(shard_rebuilding_scheduled, true, "Number of shards scheduled for rebuilding")
// Number of rebuilding triggered by the rebuilding supervisor
EXPORT_COUNTER_TYPE(shard_rebuilding_triggered, true, "Number of rebuilding triggered by the rebuilding supervisor")
// Number of rebuilding *not* triggered by the rebuilding supervisor because
// - the node doesn't appear in the config
EXPORT_COUNTER_TYPE(node_rebuilding_not_triggered_notinconfig, true, "Number of rebuilding *not* triggered by the rebuilding supervisor because - the node doesn't appear in the config")
// - the node is not a storage node
EXPORT_COUNTER_TYPE(node_rebuilding_not_triggered_notstorage, true, "- the node is not a storage node")
// - the node is alive
EXPORT_COUNTER_TYPE(shard_rebuilding_not_triggered_nodealive, true, "- the node is alive")
// - rebuilding has already started
EXPORT_COUNTER_TYPE(shard_rebuilding_not_triggered_started, true, "- rebuilding has already started")
// Rebuilding supervisors that are throttled due to the number of triggers
// exceeding the trigger queue threshold
EXPORT_COUNTER_TYPE(rebuilding_supervisor_throttled, true, "Rebuilding supervisors that are throttled due to the number of triggers exceeding the trigger queue threshold")

// LogsConfigManager Request Stats
EXPORT_COUNTER_TYPE(logsconfig_api_requests_received, true, "LogsConfigManager Request Stats")
// Requests that was served successfully, This include legit error responses
// (e.g, the request ended up returning E::EXISTS)
EXPORT_COUNTER_TYPE(logsconfig_api_requests_success, true, "Requests that was served successfully, This include legit error responses (e.g, the request ended up returning E::EXISTS)")
// This accounts only errors that are unexpected (including NOTSUPPORTED)
EXPORT_COUNTER_TYPE(logsconfig_api_requests_failed, true, "This accounts only errors that are unexpected (including NOTSUPPORTED)")
// The maximum payload size of a response, this should not exceed the
// Message::false, ""_LEN
EXPORT_COUNTER_TYPE(logsconfig_api_reply_payload_size, false, "The maximum payload size of a response, this should not exceed the Message::false, ""_LEN")
// Messages that had a big payload that was too big and were dropped
EXPORT_COUNTER_TYPE(logsconfig_api_response_toobig, true, "Messages that had a big payload that was too big and were dropped")
// Size of the logsconfig snapshot record
EXPORT_COUNTER_TYPE(logsconfig_snapshot_size, false, "Size of the logsconfig snapshot record")

// the amount of boycotts active by a controller
EXPORT_COUNTER_TYPE(boycotts_by_controller_active, false, "the amount of boycotts active by a controller")
// the amount of boycotts made by a controller
EXPORT_COUNTER_TYPE(boycotts_by_controller_total, false, "the amount of boycotts made by a controller")
// the amount of boycotts being forwarded in the gossip by this node
EXPORT_COUNTER_TYPE(boycotts_seen, false, "the amount of boycotts being forwarded in the gossip by this node")
// the latest count of append outliers as reported by the AppendOutlierDetector
EXPORT_COUNTER_TYPE(append_success_outliers_active, false, "the latest count of append outliers as reported by the AppendOutlierDetector")

// Event log RSM snapshotting errors
EXPORT_COUNTER_TYPE(eventlog_snapshotting_errors, true, "Event log RSM snapshotting errors")

// Size of the event log snapshot
EXPORT_COUNTER_TYPE(eventlog_snapshot_size, false, "Size of the event log snapshot")

// Size of the maintenance log snapshot
EXPORT_COUNTER_TYPE(maintenance_log_snapshot_size, false, "Size of the maintenance log snapshot")

// Maintenance log RSM snapshotting errors
EXPORT_COUNTER_TYPE(maintenance_log_snapshotting_errors, true, "Maintenance log RSM snapshotting errors")

// Server message handler
// skip permission
EXPORT_COUNTER_TYPE(server_message_dispatch_skip_permission, true, "Server message handler skip permission")
// check permission
EXPORT_COUNTER_TYPE(server_message_dispatch_check_permission, true, "check permission")
// bypass permission
EXPORT_COUNTER_TYPE(server_message_dispatch_bypass_permission, true, "bypass permission")
// check server messages sent by client
EXPORT_COUNTER_TYPE(unauthorized_server_message_by_client, true, "check server messages sent by client")

// ACL Cache
// cache misses
EXPORT_COUNTER_TYPE(acl_cache_misses, true, "ACL Cache cache misses")
// cache hits
EXPORT_COUNTER_TYPE(acl_cache_hits, true, "cache hits")

// Number of denied unauthenticated connections
EXPORT_COUNTER_TYPE(unauthenticated_connection_denied, true, "Number of denied unauthenticated connections")

// how many times metadata log has been trimmed
EXPORT_COUNTER_TYPE(metadata_log_trims, true, "how many times metadata log has been trimmed")

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */

// Number of partitions in all partitioned stores
EXPORT_COUNTER_TYPE(partitions, false, "Number of partitions in all partitioned stores")
// Number of partitions that are being compacted now
EXPORT_COUNTER_TYPE(partition_compactions_in_progress, false, "Number of partitions that are being compacted now")

EXPORT_COUNTER_TYPE(read_storage_tasks_allocated_records_bytes, false, "")
// Number of ReadStorageTasks that are in flight, ie they are executing on a
// storage thread or waiting to be sent back to the worker thread.
EXPORT_COUNTER_TYPE(num_in_flight_read_storage_tasks, false, "Number of ReadStorageTasks that are in flight, ie they are executing on a storage thread or waiting to be sent back to the worker thread.")
// Number of read storage tasks being delayed because we reached the limit on
// the number of read storage tasks in flight.
EXPORT_COUNTER_TYPE(read_storage_tasks_delayed, false, "Number of read storage tasks being delayed because we reached the limit on the number of read storage tasks in flight.")

// Current number of log recovery requests enqueued because the number of
// active running log recovery request reaches the limit
EXPORT_COUNTER_TYPE(recovery_enqueued, false, "Current number of log recovery requests enqueued because the number of active running log recovery request reaches the limit")

// Stats for rebuilding
EXPORT_COUNTER_TYPE(num_logs_rebuilding, false, "Stats for rebuilding")

// Number of pending GetSeqStateRequests with UNRELEASED_RECORD context
EXPORT_COUNTER_TYPE(get_seq_state_pending_context_unreleased_record, false, "Number of pending GetSeqStateRequests with UNRELEASED_RECORD context")

// Number of logs with LogStorageState::permanent_error_ = true.
// We're likely to send errors to the client when trying to read these logs.
EXPORT_COUNTER_TYPE(logs_with_permanent_error, false, "Number of logs with LogStorageState::permanent_error_ = true. We're likely to send errors to the client when trying to read these logs.")

// set to 1 if the node is chosen as controller, 0 otherwise
EXPORT_COUNTER_TYPE(is_controller, false, "set to 1 if the node is chosen as controller, 0 otherwise")

/*
 * These stats will not be aggregated for destroyed threads.
 */

// the current total size of all appender buffer queues, cannot be reset
EXPORT_COUNTER_TYPE(appenderbuffer_pending_appenders, false, "the current total size of all appender buffer queues, cannot be reset")
// Number of accepted connections that are waiting for logdevice protocol
// negotiation
EXPORT_COUNTER_TYPE(num_backlog_connections, false, "Number of accepted connections that are waiting for logdevice protocol negotiation")
// Total number of open connections
EXPORT_COUNTER_TYPE(num_connections, false, "Total number of open connections")
EXPORT_COUNTER_TYPE(num_connections_incoming_data, false, "")
EXPORT_COUNTER_TYPE(num_connections_incoming_data_low_priority, false, "")
EXPORT_COUNTER_TYPE(num_connections_incoming_data_high_priority, false, "")
EXPORT_COUNTER_TYPE(num_connections_incoming_data_ssl, false, "")
EXPORT_COUNTER_TYPE(num_connections_incoming_gossip, false, "")
EXPORT_COUNTER_TYPE(num_connections_incoming_server_to_server, false, "")

// Total number of open connections using ssl
EXPORT_COUNTER_TYPE(num_ssl_connections, false, "Total number of open connections using ssl")
// Dropped connections due to limit/burst
EXPORT_COUNTER_TYPE(dropped_connection_limit, false, "Dropped connections due to limit/burst")
EXPORT_COUNTER_TYPE(dropped_connection_burst, false, "")
// fd/connection limits
EXPORT_COUNTER_TYPE(fd_limit, false, "fd/connection limits")
EXPORT_COUNTER_TYPE(num_reserved_fds, false, "")
EXPORT_COUNTER_TYPE(max_incoming_connections, false, "")
EXPORT_COUNTER_TYPE(max_external_connections, false, "")

// Number of tasks queued on the storage thread pool
EXPORT_COUNTER_TYPE(num_storage_tasks_fast_time_sensitive, false, "Number of tasks queued on the storage thread pool")
EXPORT_COUNTER_TYPE(num_storage_tasks_fast_stallable, false, "")
EXPORT_COUNTER_TYPE(num_storage_tasks_slow, false, "")
EXPORT_COUNTER_TYPE(num_storage_tasks_default, false, "")
// Total pending bytes in all sockets.
// Includes messages waiting for traffic shaping bandwidth, waiting for
// serialization, waiting to be passed to TCP.
EXPORT_COUNTER_TYPE(sockets_bytes_pending_total, false, "Total pending bytes in all sockets. Includes messages waiting for traffic shaping bandwidth, waiting for serialization, waiting to be passed to TCP.")
// Total pending bytes in all sockets of one worker. Max across worker threads.
EXPORT_COUNTER_TYPE(sockets_bytes_pending_max_worker, false, "Total pending bytes in all sockets of one worker. Max across worker threads.")
// Count of flow groups runs that overran the configured max runtime
EXPORT_COUNTER_TYPE(flow_groups_run_deadline_exceeded, false, "Count of flow groups runs that overran the configured max runtime")
EXPORT_COUNTER_TYPE(flow_groups_run_deadline_exceeded_rt, false, "")
// If this is > 0, there is a non authoritative rebuilding, which means too many
// shards lost data such that rebuilding could not restore all records, ie some
// records had all of their copies on these shards. When this happens, the
// shards are never transitioned to AUTHORITATIVE_EMPTY and readers may stall.
// This can be fixed by either restoring enough of these shards with their data
// intact or marking each shard "unrecoverable" which means "I know the data on
// this shard will never be restored, allow readers to make progress".
EXPORT_COUNTER_TYPE(rebuilding_waiting_for_recoverable_shards, false, "")
// Number of storage tasks buffered across all workers.
EXPORT_COUNTER_TYPE(storage_task_buffer_size_fast_time_sensitive, false, "Number of storage tasks buffered across all workers.")
EXPORT_COUNTER_TYPE(storage_task_buffer_size_fast_stallable, false, "")
EXPORT_COUNTER_TYPE(storage_task_buffer_size_slow, false, "")
EXPORT_COUNTER_TYPE(storage_task_buffer_size_default, false, "")
// Total number of appenders
EXPORT_COUNTER_TYPE(num_appenders, false, "Total number of appenders")
// Total size of appenders along with their append messages and payloads
EXPORT_COUNTER_TYPE(total_size_of_appenders, false, "Total size of appenders along with their append messages and payloads")
// Appenders started
EXPORT_COUNTER_TYPE(appender_start, false, "Appenders started")

// Current number of log recoveries that are reading sequencer metadata
// from its metadata log
EXPORT_COUNTER_TYPE(num_recovery_reading_sequencer_metadata, false, "Current number of log recoveries that are reading sequencer metadata from its metadata log")

// Current total number of read streams that are in single copy delivery mode.
EXPORT_COUNTER_TYPE(read_streams_num_scd, false, "Current total number of read streams that are in single copy delivery mode.")
// are in single copy delivery mode.
EXPORT_COUNTER_TYPE(read_streams_total_known_down_size, false, "are in single copy delivery mode.")
// Number of ServerReadStreams that see themselves in the SCD known down list.
EXPORT_COUNTER_TYPE(read_streams_total_self_in_known_down, false, "Number of ServerReadStreams that see themselves in the SCD known down list.")

// Number of active RecordRebuilding state machines.
EXPORT_COUNTER_TYPE(record_rebuilding_in_progress, false, "Number of active RecordRebuilding state machines.")

// Number of logs configured for this cluster
EXPORT_COUNTER_TYPE(num_logs_configured, false, "Number of logs configured for this cluster")

EXPORT_COUNTER_TYPE(record_cache_repopulations_failed, false, "")
EXPORT_COUNTER_TYPE(record_cache_repopulated_bytes, false, "")

// Number of replicated state machines that are stalled because they saw a TRIM
// or DATALOSS gap in the delta log and are waiting for a snapshot.
EXPORT_COUNTER_TYPE(num_replicated_state_machines_stalled, false, "Number of replicated state machines that are stalled because they saw a TRIM or DATALOSS gap in the delta log and are waiting for a snapshot.")

// How many times sequencer initiated trimming
EXPORT_COUNTER_TYPE(sbt_num_seq_initiated_trims, false, "How many times sequencer initiated trimming")

// Storage Node GrayListing
// 1. How many times a shard is added to the graylist, this may be more
//    than the number of nodes actually, because of
//    a) multiple store timeouts around the same time
//    b) the graylist is per NodeSetState(which is per log)
EXPORT_COUNTER_TYPE(graylist_shard_added, false, "Storage Node GrayListing. How many times a shard is added to the graylist, this may be more than the number of nodes actually, because of a) multiple store timeouts around the same time b) the graylist is per NodeSetState(which is per log)")

// 2. How many times graylist was reset because we couldn't pick enough nodes
//    to form a copyset
EXPORT_COUNTER_TYPE(graylist_reset_cant_pick_copyset, false, "Storage Node GrayListing. How many times graylist was reset because we couldn't pick enough nodes to form a copyset")

// 3. If number of nodes in graylist has reached or exceeded the threshold
//    percentage(of nodeset), then we clear the graylist
EXPORT_COUNTER_TYPE(graylist_reset_on_threshold_reached, false, "Storage Node GrayListing. If number of nodes in graylist has reached or exceeded the threshold percentage(of nodeset), then we clear the graylist")

// 4. If graylisting is disabled in settings for some reason, we need to
//    clear the existing graylist
EXPORT_COUNTER_TYPE(graylist_reset_on_disable, false, "Storage Node GrayListing. If graylisting is disabled in settings for some reason, we need to clear the existing graylist")

// 5. For the outlier based graylisting, we don't care about the reason of the
//    reset. So this counter is incremented (per worker) if the graylist is
//    cleared for any reason.
EXPORT_COUNTER_TYPE(graylist_reseted, false, "Storage Node GrayListing. For the outlier based graylisting, we don't care about the reason of the reset. So this counter is incremented (per worker) if the graylist is cleared for any reason.")

// how many time fail to recompute byteoffset during Epoch recovery
EXPORT_COUNTER_TYPE(recompute_byteoffset_failed, false, "how many time fail to recompute byteoffset during Epoch recovery")
