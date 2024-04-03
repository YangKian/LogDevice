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

// The end of the rebuilding global window
EXPORT_COUNTER_TYPE(rebuilding_global_window_end, false, "The end of the rebuilding global window")
// Number of times the global window was slid
EXPORT_COUNTER_TYPE(rebuilding_global_window_slide_num, true, "Number of times the global window was slid")
// The total number of milliseconds the global window slid by
EXPORT_COUNTER_TYPE(rebuilding_global_window_slide_total, true, "The total number of milliseconds the global window slid by")
// 1 if we are waiting on the global window, 0 otherwise
EXPORT_COUNTER_TYPE(rebuilding_global_window_waiting_flag, false, "1 if we are waiting on the global window, 0 otherwise")

// Number of records read by RebuildingReadStorageTask
EXPORT_COUNTER_TYPE(rebuilding_num_records_read, true, "Number of records read by RebuildingReadStorageTask")
// Number of bytes read by RebuildingReadStorageTask
EXPORT_COUNTER_TYPE(rebuilding_num_bytes_read, true, "Number of bytes read by RebuildingReadStorageTask")
// Number of record bytes read by RebuildingReadStorageTask
EXPORT_COUNTER_TYPE(rebuilding_num_record_bytes_read, true, "Number of record bytes read by RebuildingReadStorageTask")
// Number of CSI bytes read by RebuildingReadStorageTask
EXPORT_COUNTER_TYPE(rebuilding_num_csi_bytes_read, true, "Number of CSI bytes read by RebuildingReadStorageTask")
// Number of CSI entries read by RebuildingReadStorageTask
EXPORT_COUNTER_TYPE(rebuilding_num_csi_entries_read, true, "Number of CSI entries read by RebuildingReadStorageTask")
// Total size of rocksdb blocks read from disk by LocalLogStoreReader for rebuilding
EXPORT_COUNTER_TYPE(rebuilding_block_bytes_read, true, "Total size of rocksdb blocks read from disk by LocalLogStoreReader for rebuilding")

// number of times that a storage node failed to write to the local log store
// (err is E::LOCAL_LOG_STORE_WRITE)
EXPORT_COUNTER_TYPE(local_logstore_failed_writes, true, "number of times that a storage node failed to write to the local log store (err is E::LOCAL_LOG_STORE_WRITE)")
// number of times that a storage node failed to access metadata on the local
// log store (err is E::LOCAL_LOG_STORE)
EXPORT_COUNTER_TYPE(local_logstore_failed_metadata, true, "number of times that a storage node failed to access metadata on the local log store (err is E::LOCAL_LOG_STORE)")
// Number of times we've seen rocksdb::Iterator::status().ok() == true, "".
EXPORT_COUNTER_TYPE(iterator_errors, true, "Number of times we've seen rocksdb::Iterator::status().ok() == true, "".")

// IO errors reported by rocksdb. Usually indicates hardware or filesystem
// problem. External tools can use this stat as a trigger for disk maintenance
// or swap.
EXPORT_COUNTER_TYPE(disk_io_errors, true, "IO errors reported by rocksdb. Usually indicates hardware or filesystem problem. External tools can use this stat as a trigger for disk maintenance or swap.")
// Corruption errors reported by rocksdb.
EXPORT_COUNTER_TYPE(corruption_errors, true, "Corruption errors reported by rocksdb.")
// Number of times the MemTable window for shard moved
EXPORT_COUNTER_TYPE(active_memtables_window_move, true, "Number of times the MemTable window for shard moved")
// The total size of trash files
EXPORT_COUNTER_TYPE(trash_size, false, "The total size of trash files")
// The rate limit on deletions of trash files
EXPORT_COUNTER_TYPE(trash_deletion_ratelimit, false, "The rate limit on deletions of trash files")

// For how long this shard was rejecting all writes to limit memtable growth.
EXPORT_COUNTER_TYPE(reject_writes_microsec, true, "For how long this shard was rejecting all writes to limit memtable growth.")
// For how long this shard was stalling low-pri writes OR rejecting all writes.
EXPORT_COUNTER_TYPE(low_pri_write_stall_microsec, true, "For how long this shard was stalling low-pri writes OR rejecting all writes.")
// Total number of flushes per shard.
EXPORT_COUNTER_TYPE(num_memtable_flush_completed, true, "Total number of flushes per shard.")
// Total number of metadata memtable flushes for a shard.
EXPORT_COUNTER_TYPE(num_metadata_memtable_flush_completed, true, "Total number of metadata memtable flushes for a shard.")
// Total number of memtables created on a shard.
EXPORT_COUNTER_TYPE(num_memtables, true, "Total number of memtables created on a shard.")
// Cumulative sum of age for all memtables created.
EXPORT_COUNTER_TYPE(cumulative_memtable_age_ms, true, "Cumulative sum of age for all memtables created.")
// Number of times metadata memtable was selected to be flushed because there

EXPORT_COUNTER_TYPE(memtable_size_active, false, "")
EXPORT_COUNTER_TYPE(memtable_size_flushing, false, "")
EXPORT_COUNTER_TYPE(memtable_size_pinned, false, "")
EXPORT_COUNTER_TYPE(memtable_count_active, false, "")

// Total number of chunks rebuilt, and total number of records and bytes in
// them. V2 only.
EXPORT_COUNTER_TYPE(chunks_rebuilt, true, "Total number of chunks rebuilt. V2 only.")
EXPORT_COUNTER_TYPE(records_rebuilt, true, "Total records of chunks rebuilt. V2 only")
EXPORT_COUNTER_TYPE(bytes_rebuilt, true, "Total bytes of chunks rebuilt. V2 only")

// Time spent by shard rebuilding in different states. V2 only.
// Time spent waiting for global window.
EXPORT_COUNTER_TYPE(rebuilding_ms_stalled, true, "Time spent by shard rebuilding in different states. V2 only. Time spent waiting for global window.")
// Time spent without any RecordRebuilding-s in flight.
EXPORT_COUNTER_TYPE(rebuilding_ms_waiting_for_read, true, "Time spent without any RecordRebuilding-s in flight.")
// Time spent without any RecordRebuilding-s in flight and with rate limiter
// saying we're not allowed to read right now.
EXPORT_COUNTER_TYPE(rebuilding_ms_rate_limited, true, "Time spent without any RecordRebuilding-s in flight and with rate limiter saying we're not allowed to read right now.")
// Time spent without a read storage task in flight.
EXPORT_COUNTER_TYPE(rebuilding_ms_waiting_for_rereplication, true, "Time spent without a read storage task in flight.")
// The remaining time (i.e. with both read task and RecordRebuilding-s in
// flight).
EXPORT_COUNTER_TYPE(rebuilding_ms_fully_occupied, false, "The remaining time (i.e. with both read task and RecordRebuilding-s in flight).")

EXPORT_COUNTER_TYPE(append_stores_over_mem_limit, false, "")
EXPORT_COUNTER_TYPE(rebuilding_stores_over_mem_limit, false, "")

// Number and total size of STORE messages received by this shard.
// Separate for STOREs sent by appenders and rebuilding. STOREs from other
// sources, e.g. recovery, are counted as append.
EXPORT_COUNTER_TYPE(append_stores_received, true, "Number of STORE messages sent by appenders.")
EXPORT_COUNTER_TYPE(append_stores_received_bytes, true, "Total size of STORE messages sent by appenders.")
EXPORT_COUNTER_TYPE(rebuilding_stores_received, true, "Number of STORE messages sent by rebuilding.")
EXPORT_COUNTER_TYPE(rebuilding_stores_received_bytes, true, "Total size of STORE messages sent by rebuilding.")

// How many IO operations took longer than rocksdb-io-tracing-threshold.
// Note that this counts different types of IO operations together: file
// creations/deletions, file writes/reads, syncs, etc. And also some of the
// tracked operations don't actually do file IO. For these reasons, it wouldn't
// be useful to try to measure an "average" or "p99" latency here, but it still
// makes sense to count really slow operations (IO operations are not expected
// to take > 5 seconds under normal circumstances, regardless of the IO kind).
EXPORT_COUNTER_TYPE(slow_iops, true, "How many IO operations took longer than rocksdb-io-tracing-threshold. Note that this counts different types of IO operations together: file creations/deletions, file writes/reads, syncs, etc. And also some of the tracked operations don't actually do file IO. For these reasons, it wouldn't be useful to try to measure an 'average' or 'p99' latency here, but it still makes sense to count really slow operations (IO operations are not expected to take > 5 seconds under normal circumstances, regardless of the IO kind).")

// Time taken to *successfully* open a rocksDB instance. Failed instances are
// not counted here. Since the RocksDB instnce can be local or over the network
// this stat is useful to debug general problems as well as implementation
// specific issues, such as slow opens on WS vs local attached disk.
EXPORT_COUNTER_TYPE(rocksdb_open_duration_ms, false, "Time taken to *successfully* open a rocksDB instance. Failed instances are not counted here. Since the RocksDB instnce can be local or over the network this stat is useful to debug general problems as well as implementation specific issues, such as slow opens on WS vs local attached disk.")

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */

// 1 if we failed to open a log store and opened a FailingLocalLogStore instead.
EXPORT_COUNTER_TYPE(failing_log_stores, false, "1 if we failed to open a log store and opened a FailingLocalLogStore instead.")
// 1 if this shard encountered an error indicating partial loss of
// access or corruption causing the shard to enter "fail-safe mode".
// Reads can still be attempted, but writes will always be denied.
EXPORT_COUNTER_TYPE(failed_safe_log_stores, false, "1 if this shard encountered an error indicating partial loss of access or corruption causing the shard to enter 'fail-safe mode'. Reads can still be attempted, but writes will always be denied.")

// 1 if the shard is waiting to be rebuilt or undrained. If the shard is only
// dirty (see shard_dirty stat), the shard will accept writes and reads.
// Otherwise writes and reads are disabled.
EXPORT_COUNTER_TYPE(shard_missing_all_data, false, "1 if the shard is waiting to be rebuilt or undrained. If the shard is only dirty (see shard_dirty stat), the shard will accept writes and reads. Otherwise writes and reads are disabled.")
// 1 if the shard was rebuilt but is not acking the rebuilding because the shard
// is marked as drained, waiting for SHARD_UNDRAIN event.
EXPORT_COUNTER_TYPE(shard_waiting_for_undrain, false, "1 if the shard was rebuilt but is not acking the rebuilding because the shard is marked as drained, waiting for SHARD_UNDRAIN event.")
// 1 if, according to out view of the event log, this node is in rebuilding set
// for this shard
EXPORT_COUNTER_TYPE(rebuilding_set_contains_myself, false, "1 if, according to out view of the event log, this node is in rebuilding set for this shard")
// 1 if rebuilding_set_contains_myself, and the rebuilding mode is RESTORE.
// Not counting mini-rebuildings.
EXPORT_COUNTER_TYPE(full_restore_set_contains_myself, false, "1 if rebuilding_set_contains_myself, and the rebuilding mode is RESTORE. Not counting mini-rebuildings.")
// 1 if shard has under-replicated regions due to an unsafe shutdown.
// Reads and writes are still accepted, and the node will schedule a
// mini-rebuilding to restore the replication factor.
EXPORT_COUNTER_TYPE(shard_dirty, false, "1 if shard has under-replicated regions due to an unsafe shutdown. Reads and writes are still accepted, and the node will schedule a mini-rebuilding to restore the replication factor.")

// What fraction of rebuilding is complete, between 1 and 10^6.
// 0 if there's no rebuilding, or if we're not a donor, or if not supported.
// Set from RebuildingCoordinator's thread.
// ("ppm" stands for parts per million.)
EXPORT_COUNTER_TYPE(rebuilding_progress_ppm, false, "What fraction of rebuilding is complete, between 1 and 10^6. 0 if there's no rebuilding, or if we're not a donor, or if not supported. Set from RebuildingCoordinator's thread. ('ppm' stands for parts per million.)")

// Derived stats (see Stats::deriveStats()).

EXPORT_COUNTER_TYPE(shards_waiting_for_non_started_restore, false, "")
EXPORT_COUNTER_TYPE(non_empty_shards_in_restore, false, "")
EXPORT_COUNTER_TYPE(unwritable_non_restore_shards, false, "")
EXPORT_COUNTER_TYPE(iterator_errors_in_writable_shards, false, "")
EXPORT_COUNTER_TYPE(unhealthy_shards, false, "")

// How many times space based trimming decided to trim
// partitions on a particular shard.
EXPORT_COUNTER_TYPE(sbt_num_storage_trims, true, "How many times space based trimming decided to trim partitions on a particular shard.")

// The starting time of the oldest partition to keep, i.e.
// the data for which we are honoring time based retention.
// Must only be set from one thread: the low-pri logsdb background thread of
// this shard. Otherwise the values from different threads would be aggregated
// incorrectly.
EXPORT_COUNTER_TYPE(sbt_effective_retention_seconds, false, "The starting time of the oldest partition to keep, i.e. the data for which we are honoring time based retention. Must only be set from one thread: the low-pri logsdb background thread of this shard. Otherwise the values from different threads would be aggregated incorrectly.")

/*
 * These stats will not be aggregated for destroyed threads.
 */

// No. of logs that are not in config, but are not being trimmed,
// because grace period hasn't expired.
EXPORT_COUNTER_TYPE(logs_waiting_for_grace_period, false, "No. of logs that are not in config, but are not being trimmed, because grace period hasn't expired.")

// How many partitions are waiting to be compacted for expired data.
EXPORT_COUNTER_TYPE(pending_retention_compactions, false, "How many partitions are waiting to be compacted for expired data.")

// How many partitions are waiting to be compacted for coalescing.
EXPORT_COUNTER_TYPE(pending_partial_compactions, false, "How many partitions are waiting to be compacted for coalescing.")

// The longest-running IO operation that is still running. Updated at around
// rocksdb-io-tracing-stall-threshold granularity, and only if
// rocksdb-io-tracing-shards is enabled.
EXPORT_COUNTER_TYPE(longest_ongoing_io_ms, false, "The longest-running IO operation that is still running. Updated at around rocksdb-io-tracing-stall-threshold granularity, and only if rocksdb-io-tracing-shards is enabled.")

// Number of threads currently executing an IO operation that has already took
// longer than rocksdb-io-tracing-stall-threshold.
EXPORT_COUNTER_TYPE(num_threads_stalled_on_io, false, "Number of threads currently executing an IO operation that has already took longer than rocksdb-io-tracing-stall-threshold.")

