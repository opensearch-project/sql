/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class AlwaysHealthyMonitorTest {

  @Test
  void isHealthy() {
    assertTrue(new AlwaysHealthyMonitor().isHealthy());
  }
}
