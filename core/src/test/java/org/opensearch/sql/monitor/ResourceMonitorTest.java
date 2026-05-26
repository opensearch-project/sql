/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor;

import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class ResourceMonitorTest {

  @Test
  void testDefaultImplementationThrowsException() {
    // Create a minimal subclass that doesn't override getStatus() or isHealthyImpl()
    ResourceMonitor monitor = new ResourceMonitor() {
          // Intentionally empty - doesn't override anything
        };

    // Attempting to use the default path should throw UnsupportedOperationException
    assertThrows(UnsupportedOperationException.class, monitor::isHealthy);
  }
}
