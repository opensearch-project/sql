/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.leasemanager;

import org.opensearch.sql.spark.leasemanager.model.LeaseRequest;

/** Lease manager */
public interface LeaseManager {

  /**
   * Borrow from LeaseManager. If no exception, lease successfully.
   *
   * @throws ConcurrencyLimitExceededException
   */
  void borrow(LeaseRequest request);
}
