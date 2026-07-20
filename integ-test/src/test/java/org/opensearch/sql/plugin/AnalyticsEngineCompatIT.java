/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin;

import static org.junit.Assume.assumeTrue;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.OpenSearchSQLRestTestCase;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Smoke test: verifies that opensearch-sql loads cleanly alongside arrow-flight-rpc and
 * analytics-engine. A successful cluster start is the only assertion — no sql-specific logic runs.
 *
 * <p>This test is only meaningful when the analytics-engine plugin is installed; the dedicated
 * {@code :integ-test:analyticsEngineCompatIT} Gradle task bundles the plugin stack for exactly that
 * purpose. Other lanes can still discover this class — notably the distribution integ-test
 * pipeline, which scans every {@code *IT} class against the built distribution and may run with the
 * security plugin enabled and without analytics-engine. A {@code build.gradle} exclude does not
 * protect against that pipeline, so the test guards itself instead:
 *
 * <ul>
 *   <li>It extends {@link OpenSearchSQLRestTestCase} so the REST client honours the {@code https},
 *       {@code user}, and {@code password} system properties of a secured cluster (a bare {@code
 *       OpenSearchRestTestCase} speaks plain HTTP and gets its connection closed during client init
 *       on a TLS-secured port).
 *   <li>It skips via an assumption when analytics-engine is absent, so a build without the plugin
 *       reports the test as skipped rather than failed.
 * </ul>
 */
public class AnalyticsEngineCompatIT extends OpenSearchSQLRestTestCase {

  /**
   * Skips the suite unless the analytics-engine plugin is installed. Runs after the framework has
   * established the (security-aware) REST client, so the lookup itself succeeds on both plain and
   * secured clusters.
   */
  @Before
  public void requireAnalyticsEngine() throws IOException {
    Response response = client().performRequest(new Request("GET", "/_cat/plugins?h=component"));
    String installedPlugins = TestUtils.getResponseBody(response, true);
    assumeTrue(
        "analytics-engine plugin not installed — skipping coexistence smoke test",
        installedPlugins.contains("analytics-engine"));
  }

  @Test
  public void testClusterStarted() {
    // If the cluster booted with analytics-engine present, all plugins loaded without classloader
    // errors. The assumption above guarantees we only assert this where it is meaningful.
  }
}
