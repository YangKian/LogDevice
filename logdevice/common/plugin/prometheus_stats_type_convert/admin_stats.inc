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

// Stats published by Admin server.
EXPORT_COUNTER_TYPE(admin_server_started, false, "Stats published by Admin server.")

// Maintenances
EXPORT_COUNTER_TYPE(num_maintenances, false, "")
EXPORT_COUNTER_TYPE(maintenance_state_version, false, "")

EXPORT_COUNTER_TYPE(maintenance_progress_UNKNOWN, false, "")
EXPORT_COUNTER_TYPE(maintenance_progress_BLOCKED_UNTIL_SAFE, false, "")
EXPORT_COUNTER_TYPE(maintenance_progress_IN_PROGRESS, false, "")
EXPORT_COUNTER_TYPE(maintenance_progress_COMPLETED, false, "")

// Maintenance Manager
EXPORT_COUNTER_TYPE(mm_evaluations, false, " Manager")
EXPORT_COUNTER_TYPE(mm_metadata_nodeset_selection_failed, false, "")
EXPORT_COUNTER_TYPE(mm_ncm_update_errors, false, "")
EXPORT_COUNTER_TYPE(mm_expired_maintenances_removed, false, "")

// Safety Checker (via MM)
EXPORT_COUNTER_TYPE(mm_safety_checker_runs, false, "Safety Checker (via MM)")

// Safety Checker (general)
// The total number of refreshes we have done
EXPORT_COUNTER_TYPE(safety_checker_metadata_refreshes, false, "The total number of refreshes we have done")
EXPORT_COUNTER_TYPE(num_historical_epochs, false, "")
