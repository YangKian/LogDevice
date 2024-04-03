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

// Sender was able to release a registered message to the Connection without
// any traffic shaping induced delay.
EXPORT_COUNTER_TYPE(direct_dispatched, true, "Sender was able to release a registered message to the Connection without any traffic shaping induced delay.")
// Sender was unable to direct dispatch a registered message. It will
// either be released by consuming priority queue bandwidth at the end
// of the event loop, or when sufficient bandwidth is released to its
// priority class.
EXPORT_COUNTER_TYPE(deferred, false, "Sender was unable to direct dispatch a registered message. It will either be released by consuming priority queue bandwidth at the end of the event loop, or when sufficient bandwidth is released to its priority class.")
// Sender was unable to direct dispatch a registered message, but a bandwidth
// available callback was provided. The callback will be registered
// (cbregistered stat will be incremented too) but this message will likely
// be discarded by the caller as it waits for its callback to fire.
EXPORT_COUNTER_TYPE(discarded, false, "Sender was unable to direct dispatch a registered message, but a bandwidth available callback was provided. The callback will be registered (cbregistered stat will be incremented too) but this message will likely be discarded by the caller as it waits for its callback to fire.")
// A bandwidth available callback was registered via either the
// Sender::sendMessage() or Sender::canSendTo() APIs.
EXPORT_COUNTER_TYPE(cbregistered, false, "A bandwidth available callback was registered via either the Sender::sendMessage() or Sender::canSendTo() APIs.")
// Bandwidth limit for the flow group, as defined by configuration
EXPORT_COUNTER_TYPE(limit, false, "Bandwidth limit for the flow group, as defined by configuration")
// Bandwidth thrown away because it was not used by any traffic classes for
// multiple iterations.
EXPORT_COUNTER_TYPE(bwdiscarded, false, "Bandwidth thrown away because it was not used by any traffic classes for multiple iterations.")

