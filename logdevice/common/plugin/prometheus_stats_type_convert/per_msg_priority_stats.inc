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

// A message was successfully pushed to the kernel for transmission.
EXPORT_COUNTER_TYPE(sent_ok, true, "A message was successfully pushed to the kernel for transmission.")
// byte count of successfully transmitted messages.
EXPORT_COUNTER_TYPE(sent_bytes, true, "byte count of successfully transmitted messages.")
// A message transmission failed for any error code.
EXPORT_COUNTER_TYPE(sent_error, true, "A message transmission failed for any error code.")
// Sender sending a message with this particular priority class was deferred
// because of insufficient credits in priority class bucket.
EXPORT_COUNTER_TYPE(deferred, false, "Sender sending a message with this particular priority class was deferred because of insufficient credits in priority class bucket.")
// Bandwidth discarded to global priority queue class bucket because of overflow
// in this particular priority class.
EXPORT_COUNTER_TYPE(bwdiscarded, false, "Bandwidth discarded to global priority queue class bucket because of overflow in this particular priority class.")
// Message belonging to a particular priority class was discarded because of no
// bandwidth headroom in that priority class. A callback supplied by the sender
// was registered and will be invoked once bandwidth is deposited in the
// priority bucket fullfilled.
EXPORT_COUNTER_TYPE(discarded, false, "Message belonging to a particular priority class was discarded because of no bandwidth headroom in that priority class. A callback supplied by the sender was registered and will be invoked once bandwidth is deposited in the priority bucket fullfilled.")
// Priority class did not have sufficient credits in it's own bucket and hence
// stole credits from global bucket of priority queue class. This stat captures
// amount of credits transferred from priority queue class.
EXPORT_COUNTER_TYPE(bwtransferred, false, "Priority class did not have sufficient credits in it's own bucket and hence stole credits from global bucket of priority queue class. This stat captures amount of credits transferred from priority queue class.")
