/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ONLINE;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.EXPLAIN_API_ENDPOINT;

import java.io.IOException;

import lombok.SneakyThrows;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.util.TestUtils;

public class PaginationIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.CALCS);
    loadIndex(Index.ONLINE);
  }

  @Test
  public void testSmallDataSet() throws IOException {
    var query = "SELECT * from " + TEST_INDEX_CALCS;
    var response = new JSONObject(executeFetchQuery(query, 4, "jdbc"));
    assertTrue(response.has("cursor"));
    assertEquals(4, response.getInt("size"));
    TestUtils.verifyIsV2Cursor(response);
  }

  @Test
  public void testLargeDataSetV2WithWhere() throws IOException {
    var query = "SELECT * from " + TEST_INDEX_ONLINE + " WHERE 1 = 1";
    var response = new JSONObject(executeFetchQuery(query, 4, "jdbc"));
    assertEquals(4, response.getInt("size"));
    TestUtils.verifyIsV2Cursor(response);
  }

  @Test
  public void testLargeDataSetV2() throws IOException {
    var query = "SELECT * from " + TEST_INDEX_ONLINE;
    var response = new JSONObject(executeFetchQuery(query, 4, "jdbc"));
    assertEquals(4, response.getInt("size"));
    TestUtils.verifyIsV2Cursor(response);
  }

  @Ignore("Scroll may not expire after timeout")
  // Scroll keep alive parameter guarantees that scroll context would be kept for that time,
  // but doesn't define how fast it will be expired after time out.
  // With KA = 1s scroll may be kept up to 30 sec or more. We can't test exact expiration.
  // I disable the test to prevent it waiting for a minute and delay all CI.
  public void testCursorTimeout() throws IOException, InterruptedException {
    updateClusterSettings(
        new ClusterSetting(PERSISTENT, Settings.Key.SQL_CURSOR_KEEP_ALIVE.getKeyValue(), "1s"));

    var query = "SELECT * from " + TEST_INDEX_CALCS;
    var response = new JSONObject(executeFetchQuery(query, 4, "jdbc"));
    assertTrue(response.has("cursor"));
    var cursor = response.getString("cursor");
    Thread.sleep(2222L); // > 1s

    ResponseException exception =
        expectThrows(ResponseException.class, () -> executeCursorQuery(cursor));
    response = new JSONObject(TestUtils.getResponseBody(exception.getResponse()));
    assertEquals(response.getJSONObject("error").getString("reason"),
        "Error occurred in OpenSearch engine: all shards failed");
    assertTrue(response.getJSONObject("error").getString("details")
        .contains("SearchContextMissingException[No search context found for id"));
    assertEquals(response.getJSONObject("error").getString("type"),
        "SearchPhaseExecutionException");

    wipeAllClusterSettings();
  }

  @Test
  @SneakyThrows
  public void testCloseCursor() {
    // Initial page request to get cursor
    var query = "SELECT * from " + TEST_INDEX_CALCS;
    var response = new JSONObject(executeFetchQuery(query, 4, "jdbc"));
    assertTrue(response.has("cursor"));
    var cursor = response.getString("cursor");

    // Close the cursor
    Request closeCursorRequest = new Request("POST", "_plugins/_sql/close");
    closeCursorRequest.setJsonEntity(String.format("{ \"cursor\" : \"%s\" } ", cursor));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    closeCursorRequest.setOptions(restOptionsBuilder);
    response = new JSONObject(executeRequest(closeCursorRequest));
    assertTrue(response.has("succeeded"));
    assertTrue(response.getBoolean("succeeded"));

    // Test that cursor is no longer available
    ResponseException exception =
        expectThrows(ResponseException.class, () -> executeCursorQuery(cursor));
    response = new JSONObject(TestUtils.getResponseBody(exception.getResponse()));
    assertEquals(response.getJSONObject("error").getString("reason"),
        "Error occurred in OpenSearch engine: all shards failed");
    assertTrue(response.getJSONObject("error").getString("details")
        .contains("SearchContextMissingException[No search context found for id"));
    assertEquals(response.getJSONObject("error").getString("type"),
        "SearchPhaseExecutionException");
  }
}
