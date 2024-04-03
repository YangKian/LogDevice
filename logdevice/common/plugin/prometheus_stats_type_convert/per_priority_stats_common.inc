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

// Priority class max bytes per second limit defined according to the config.
EXPORT_COUNTER_TYPE(max_bw, false, "Priority class max bytes per second limit defined according to the config.")
// Priority class max burst bytes per second limit as per the config.
EXPORT_COUNTER_TYPE(max_burst, false, "Priority class max burst bytes per second limit as per the config.")
// Priority class guaranteed bytes per second limit as per the config.
EXPORT_COUNTER_TYPE(guaranteed_bw, false, "Priority class guaranteed bytes per second limit as per the config.")
// Per priority class bw consumed during an interval as messages are sent by the
// sender.
EXPORT_COUNTER_TYPE(bwconsumed, false, "Per priority class bw consumed during an interval as messages are sent by the sender.")
// If there are no credits left in the priority class bucket, and the
// client wants to send message on the socket, it won't be allowed to send.
// Instead, the callback or envelope will be added to a list belonging to the
// priority class/bucket. When credits are deposited to the bucket or when the
// deadline timer to run flow groups expires, we go over the list and drain
// messages.
// This uses up the new credits deposited or if credits are available in the
// priority queue bucket we use them to send message on the socket. This stat
// tracks whether there are messages still backlogged for a bucket after
// priorityq was drained
EXPORT_COUNTER_TYPE(pq_backlog, false, "If there are no credits left in the priority class bucket, and the client wants to send message on the socket, it won't be allowed to send. Instead, the callback or envelope will be added to a list belonging to the priority class/bucket. When credits are deposited to the bucket or when the deadline timer to run flow groups expires, we go over the list and drain messages. This uses up the new credits deposited or if credits are available in the priority queue bucket we use them to send message on the socket. This stat tracks whether there are messages still backlogged for a bucket after priorityq was drained")

