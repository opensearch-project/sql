/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.utils;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RealTimeProviderTest {
  @Test
  public void testCurrentEpochMillis() {
    RealTimeProvider realTimeProvider = new RealTimeProvider();

    assertTrue(realTimeProvider.currentEpochMillis() > 0);
  }
}
