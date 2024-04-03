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

// Number of requests posted
EXPORT_COUNTER_TYPE(post_request, true, "Number of requests posted")
// Number of microseconds that workers spent processing requests of this type.
EXPORT_COUNTER_TYPE(request_worker_usec, true, "Number of microseconds that workers spent processing requests of this type.")

