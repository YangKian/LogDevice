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

// The number of tasks posted
EXPORT_COUNTER_TYPE(storage_tasks_posted, true, "The number of tasks posted")
// The number of tasks executed
EXPORT_COUNTER_TYPE(storage_tasks_executed, true, "The number of tasks executed")
// Number of microseconds spent in storage threads on executing this type of
// storage tasks
EXPORT_COUNTER_TYPE(storage_thread_usec, true, "Number of microseconds spent in storage threads on executing this type of storage tasks")
// Number of microseconds spent by task in the storage (scheduler) queue.
EXPORT_COUNTER_TYPE(storage_q_usec, true, "Number of microseconds spent by task in the storage (scheduler) queue.")
// Number of microseconds spent by StorageTaskResponse on worker thread.
EXPORT_COUNTER_TYPE(storage_task_response_worker_usec, true, "Number of microseconds spent by StorageTaskResponse on worker thread.")

