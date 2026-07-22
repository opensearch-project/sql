/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.plugin.rest.RestPPLQueryAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for queries dispatched to the complex worker pool. Verifies that queries
 * containing scripts (e.g., parse command) are correctly routed and that profile responses include
 * thread_pool metadata.
 */
public class CalciteComplexPoolIT extends PPLIntegTestCase {

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    enableCalcite();
    enableComplexPool();
  }

  private void enableComplexPool() throws IOException {
    updateClusterSettings(
        new ClusterSetting(PERSISTENT, "plugins.sql.complex_worker_pool.enabled", "true"));
  }

  @Test
  public void testParseCommandDispatchesToComplexPool() throws IOException {
    // parse creates script nodes, triggering complex pool dispatch
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | parse address '(?<number>\\\\d+) (?<street>.*)'"
                    + " | fields number, street | head 1",
                TEST_INDEX_BANK));

    verifyDataRows(result, rows("880", "Holmes Lane"));
  }

  @Test
  public void testComplexPoolProfileIncludesThreadPool() throws IOException {
    // Query with parse to trigger complex pool
    String query =
        String.format(
            "source=%s | parse address '(?<num>\\\\d+)' | fields num | head 1", TEST_INDEX_BANK);

    JSONObject result = executeQueryWithProfile(query);

    assertTrue("Response has profile", result.has("profile"));
    JSONObject profile = result.getJSONObject("profile");

    assertTrue("Profile has thread_pool", profile.has("thread_pool"));
    String threadPool = profile.getString("thread_pool");
    assertEquals("Thread pool is sql-complex-worker", "sql-complex-worker", threadPool);

    // Verify profile structure is intact
    assertTrue("Profile has summary", profile.has("summary"));
    assertTrue("Profile has phases", profile.has("phases"));
  }

  @Test
  public void testSimpleQueryUsesWorkerPool() throws IOException {
    // Query without scripts — should use sql-worker pool
    String query = String.format("source=%s | fields account_number | head 1", TEST_INDEX_BANK);

    JSONObject result = executeQueryWithProfile(query);

    assertTrue("Response has profile", result.has("profile"));
    JSONObject profile = result.getJSONObject("profile");

    assertTrue("Profile has thread_pool", profile.has("thread_pool"));
    String threadPool = profile.getString("thread_pool");
    assertEquals("Thread pool is sql-worker", "sql-worker", threadPool);
  }

  private JSONObject executeQueryWithProfile(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(
        String.format(Locale.ROOT, "{\"query\": \"%s\", \"profile\": true}", query));
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response, true));
  }
}
