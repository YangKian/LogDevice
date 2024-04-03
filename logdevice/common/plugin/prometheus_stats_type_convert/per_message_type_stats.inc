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

// Number of messages sent
EXPORT_COUNTER_TYPE(message_sent, true, "Number of messages sent")
// Number of messages received
EXPORT_COUNTER_TYPE(message_received, true, "Number of messages received")
// Number of microseconds that workers spent processing callbacks for this
// message type.
EXPORT_COUNTER_TYPE(message_worker_usec, true, "Number of microseconds that workers spent processing callbacks for this message type.")
// Bytes of messages of this type enqueued in Socket.
// Including messages waiting for traffic shaping bandwidth, waiting for
// serialization, waiting to be passed to TCP.
EXPORT_COUNTER_TYPE(message_bytes_pending, true, "Bytes of messages of this type enqueued in Socket. Including messages waiting for traffic shaping bandwidth, waiting for serialization, waiting to be passed to TCP.")

