/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.leasemanager;

/** Concurrency limit exceeded. */
public class ConcurrencyLimitExceededException extends RuntimeException {
  public ConcurrencyLimitExceededException(String message) {
    super(message);
  }
}
