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
 * Integration test verifying PIT contexts are created only when needed and properly cleaned up.
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

  @Test
  public void testNoPitLeakWithoutFetchSize() throws IOException, InterruptedException {
    int baselinePitCount = getCurrentPitCount();

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
    }

    int currentPitCount = getCurrentPitCount();
    int leakedPits = currentPitCount - baselinePitCount;

    assertThat("No PITs should leak after fix", leakedPits, equalTo(0));
  }

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

    assertThat(
        "PIT should be cleaned up after cursor close", finalPitCount, equalTo(baselinePitCount));
  }

  @Test
  public void testCompareV1AndV2EnginePitBehavior() throws IOException {
    int baselinePitCount = getCurrentPitCount();

    String v1Query =
        StringUtils.format(
            "SELECT * FROM %s WHERE action LIKE 'login%%' ORDER BY timestamp ASC", TEST_INDEX);

    JSONObject v1Response = executeQueryWithoutFetchSize(v1Query);
    int afterV1PitCount = getCurrentPitCount();
    int v1Leaked = afterV1PitCount - baselinePitCount;

    String v2Query =
        StringUtils.format(
            "SELECT * FROM `%s` WHERE action LIKE 'login%%' ORDER BY timestamp ASC", TEST_INDEX);

    JSONObject v2Response = executeQueryWithoutFetchSize(v2Query);
    int afterV2PitCount = getCurrentPitCount();
    int v2Leaked = afterV2PitCount - afterV1PitCount;

    assertTrue("V1 should return results", v1Response.has("datarows"));
    assertTrue("V2 should return results", v2Response.has("datarows"));

    assertThat("V1 Legacy SQL should not leak PITs", v1Leaked, equalTo(0));
    assertThat("V2 SQL should not leak PITs", v2Leaked, equalTo(0));
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

    if (!stats.has("nodes")) {
      return 0;
    }

    int totalPits = 0;
    JSONObject nodes = stats.getJSONObject("nodes");
    for (String nodeId : nodes.keySet()) {
      Object pitValue =
          stats.optQuery("/nodes/" + nodeId + "/indices/search/point_in_time_current");
      if (pitValue instanceof Number) {
        totalPits += ((Number) pitValue).intValue();
      }
    }

    return totalPits;
  }
}
