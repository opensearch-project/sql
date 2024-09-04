/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.leasemanager;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class ConcurrencyLimitExceededExceptionTest {
  @Test
  public void test() {
    ConcurrencyLimitExceededException e = new ConcurrencyLimitExceededException("Test");

    assertEquals("Test", e.getMessage());
  }
}
