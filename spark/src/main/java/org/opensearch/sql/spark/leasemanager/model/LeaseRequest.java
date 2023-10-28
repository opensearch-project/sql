/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.leasemanager.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.spark.dispatcher.model.JobType;

/** Lease Request. */
@Getter
@RequiredArgsConstructor
public class LeaseRequest {
  private final JobType jobType;
  private final String datasourceName;
}
