/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin;

import org.junit.Test;
import org.opensearch.test.rest.OpenSearchRestTestCase;

/**
 * Smoke test: verifies that opensearch-sql loads cleanly alongside arrow-flight-rpc and
 * analytics-engine. A successful cluster start is the only assertion — no sql-specific logic runs.
 */
public class AnalyticsEngineCompatIT extends OpenSearchRestTestCase {

  @Test
  public void testClusterStarted() {
    // If the cluster booted, all three plugins loaded without classloader errors.
  }
}
