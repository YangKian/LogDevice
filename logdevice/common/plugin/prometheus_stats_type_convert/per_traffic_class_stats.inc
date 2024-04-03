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

EXPORT_COUNTER_TYPE(messages_sent, true, "")
EXPORT_COUNTER_TYPE(messages_received, true, "")
EXPORT_COUNTER_TYPE(bytes_sent, true, "")
EXPORT_COUNTER_TYPE(bytes_received, true, "")

EXPORT_COUNTER_TYPE(gap_messages_sent, true, "")

EXPORT_COUNTER_TYPE(read_streams_created, true, "")

EXPORT_COUNTER_TYPE(record_messages_sent, true, "")
// Payload bytes sent to clients in RECORDs
EXPORT_COUNTER_TYPE(record_payload_bytes, true, "Payload bytes sent to clients in RECORDs")

EXPORT_COUNTER_TYPE(started_messages_sent, true, "")

// Number of STORE messages received
EXPORT_COUNTER_TYPE(store_received, true, "Number of STORE messages received")
// Payload bytes sent by sequencers to storage nodes in STORE messages
EXPORT_COUNTER_TYPE(store_payload_bytes, true, "Payload bytes sent by sequencers to storage nodes in STORE messages")

/*
 * The following stats will not be reset by Stats::reset() and the 'reset'
 * admin command.
 */

// Number of active server read streams
EXPORT_COUNTER_TYPE(read_streams, false, "Number of active server read streams")
// Number of server read streams that are catching up
EXPORT_COUNTER_TYPE(read_streams_num_catching_up, false, "Number of server read streams that are catching up")
// Number of server read streams waiting for their windows to slide
EXPORT_COUNTER_TYPE(read_streams_num_window_end, false, "Number of server read streams waiting for their windows to slide")

