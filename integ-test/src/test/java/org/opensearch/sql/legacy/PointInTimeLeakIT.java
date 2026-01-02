/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.utils.StringUtils;

/**
 * Integration test to verify Point-in-Time (PIT) context leak fix in Legacy SQL engine.
 *
 * <p>Fix: When queries are executed without fetch_size (non-paginated), the Legacy SQL engine now
 * conditionally creates PIT contexts only when pagination is requested (fetch_size > 0) and
 * properly cleans them up when not used for cursor-based pagination.
 *
 * <p>This test verifies that:
 *
 * <ul>
 *   <li>Non-paginated queries (fetch_size=0) do not create PITs
 *   <li>Paginated queries (fetch_size>0) properly manage PIT lifecycle through cursors
 *   <li>No PIT contexts leak after query execution
 * </ul>
 *
 * @see <a href="https://github.com/opensearch-project/sql/issues/5002">Issue #5002</a>
 */
public class PointInTimeLeakIT extends SQLIntegTestCase {

  private static final String TEST_INDEX = "test-logs-2025.01.01";
  private static final String PIT_STATS_ENDPOINT =
      "/_nodes/stats/indices/search?filter_path=nodes.*.indices.search.point_in_time_current";

  @Before
  public void setUpTestIndex() throws IOException {
    try {
      executeRequest(new Request("DELETE", "/" + TEST_INDEX));
    } catch (ResponseException e) {
      // Ignore 404 - index doesn't exist, which is expected in clean state
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }

    Request createIndex = new Request("PUT", "/" + TEST_INDEX);
    createIndex.setJsonEntity(
        "{  \"mappings\": {    \"properties\": {      \"action\": {\"type\": \"text\", \"fields\":"
            + " {\"keyword\": {\"type\": \"keyword\"}}},      \"timestamp\": {\"type\": \"date\"}  "
            + "  }  }}");
    executeRequest(createIndex);

    Request bulkRequest = new Request("POST", "/" + TEST_INDEX + "/_bulk");
    bulkRequest.addParameter("refresh", "true");
    bulkRequest.setJsonEntity(
        "{\"index\":{}}\n"
            + "{\"action\":\"login_success\",\"timestamp\":\"2025-01-01T10:00:00Z\"}\n"
            + "{\"index\":{}}\n"
            + "{\"action\":\"login_success\",\"timestamp\":\"2025-01-01T10:01:00Z\"}\n"
            + "{\"index\":{}}\n"
            + "{\"action\":\"login_failed\",\"timestamp\":\"2025-01-01T10:02:00Z\"}\n");
    executeRequest(bulkRequest);
  }

  /**
   * Test verifying that PIT leak is fixed when executing queries without fetch_size.
   *
   * <p>This test:
   *
   * <ul>
   *   <li>Records baseline PIT count
   *   <li>Executes multiple SQL queries WITHOUT fetch_size parameter
   *   <li>Verifies that NO PIT contexts are created or leaked
   *   <li>Confirms the fix prevents resource leaks
   * </ul>
   */
  @Test
  public void testNoPitLeakWithoutFetchSize() throws IOException, InterruptedException {
    int baselinePitCount = getCurrentPitCount();
    System.out.println("Baseline PIT count: " + baselinePitCount);

    int numQueries = 10;

    for (int i = 0; i < numQueries; i++) {
      String query =
          StringUtils.format(
              "SELECT * FROM %s WHERE action LIKE 'login%%' ORDER BY timestamp ASC", TEST_INDEX);

      JSONObject response = executeQueryWithoutFetchSize(query);

      assertTrue("Query should succeed", response.has("datarows"));
      JSONArray dataRows = response.getJSONArray("datarows");
      assertThat("Should return results", dataRows.length(), greaterThan(0));
      assertFalse("Should not have cursor for non-paginated query", response.has("cursor"));

      System.out.println(
          String.format(
              "[%d/%d] Query executed, returned %d rows", i + 1, numQueries, dataRows.length()));
    }

    int currentPitCount = getCurrentPitCount();
    int leakedPits = currentPitCount - baselinePitCount;

    System.out.println(
        String.format(
            "After %d queries: Current PIT count = %d, Leaked PITs = %d",
            numQueries, currentPitCount, leakedPits));

    assertThat("No PITs should leak after fix", leakedPits, equalTo(0));

    System.out.println("✓ FIX VERIFIED: No PIT contexts leaked!");
  }

  /**
   * Test showing expected behavior: queries with fetch_size properly manage PITs.
   *
   * <p>When fetch_size is specified, PITs are properly managed through cursor lifecycle.
   */
  @Test
  public void testPitManagedProperlyWithFetchSize() throws IOException {
    int baselinePitCount = getCurrentPitCount();

    String query =
        StringUtils.format(
            "SELECT * FROM %s WHERE action LIKE 'login%%' ORDER BY timestamp ASC", TEST_INDEX);

    JSONObject response = executeQueryWithFetchSize(query, 2);

    assertTrue("Should have cursor with fetch_size", response.has("cursor"));
    String cursor = response.getString("cursor");

    JSONObject closeResponse = executeCursorCloseQuery(cursor);
    assertTrue("Cursor close should succeed", closeResponse.getBoolean("succeeded"));

    int finalPitCount = getCurrentPitCount();

    System.out.println(
        String.format(
            "With proper cursor management: Baseline=%d, Final=%d",
            baselinePitCount, finalPitCount));

    assertThat(
        "PIT should be cleaned up after cursor close", finalPitCount, equalTo(baselinePitCount));
  }

  /**
   * Test comparing Legacy SQL vs V2 SQL engine behavior. V2 engine may create PITs internally but
   * cleans them up properly.
   */
  @Test
  public void testCompareV1AndV2EnginePitBehavior() throws IOException {
    int baselinePitCount = getCurrentPitCount();

    String v1Query =
        StringUtils.format(
            "SELECT * FROM %s WHERE action LIKE 'login%%' ORDER BY timestamp ASC", TEST_INDEX);

    JSONObject v1Response = executeQueryWithoutFetchSize(v1Query);
    int afterV1PitCount = getCurrentPitCount();
    int v1Leaked = afterV1PitCount - baselinePitCount;

    System.out.println(String.format("V1 Legacy SQL: PITs leaked = %d", v1Leaked));

    String v2Query =
        StringUtils.format(
            "SELECT * FROM `%s` WHERE action LIKE 'login%%' ORDER BY timestamp ASC", TEST_INDEX);

    JSONObject v2Response = executeQueryWithoutFetchSize(v2Query);
    int afterV2PitCount = getCurrentPitCount();

    System.out.println(
        String.format("After V1 and V2 queries: Total PIT count = %d", afterV2PitCount));

    assertTrue("V1 should return results", v1Response.has("datarows"));
    assertTrue("V2 should return results", v2Response.has("datarows"));

    // Both engines should not leak PITs for non-paginated queries
    assertThat("V1 Legacy SQL should not leak PITs", v1Leaked, equalTo(0));
  }

  private JSONObject executeQueryWithoutFetchSize(String query) throws IOException {
    Request sqlRequest = new Request("POST", "/_plugins/_sql?format=jdbc");
    sqlRequest.setJsonEntity(String.format("{\"query\": \"%s\"}", query));

    Response response = client().performRequest(sqlRequest);
    return new JSONObject(TestUtils.getResponseBody(response));
  }

  private JSONObject executeQueryWithFetchSize(String query, int fetchSize) throws IOException {
    Request sqlRequest = new Request("POST", "/_plugins/_sql?format=jdbc");
    sqlRequest.setJsonEntity(
        String.format("{\"query\": \"%s\", \"fetch_size\": %d}", query, fetchSize));

    Response response = client().performRequest(sqlRequest);
    return new JSONObject(TestUtils.getResponseBody(response));
  }

  private int getCurrentPitCount() throws IOException {
    Request statsRequest = new Request("GET", PIT_STATS_ENDPOINT);
    Response response = client().performRequest(statsRequest);
    JSONObject stats = new JSONObject(TestUtils.getResponseBody(response));

    int totalPits = 0;
    if (stats.has("nodes")) {
      JSONObject nodes = stats.getJSONObject("nodes");
      for (String nodeId : nodes.keySet()) {
        JSONObject node = nodes.getJSONObject(nodeId);
        if (node.has("indices")) {
          JSONObject indices = node.getJSONObject("indices");
          if (indices.has("search")) {
            JSONObject search = indices.getJSONObject("search");
            if (search.has("point_in_time_current")) {
              totalPits += search.getInt("point_in_time_current");
            }
          }
        }
      }
    }

    return totalPits;
  }
}
