/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class PaginationWindowIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.PHRASE);
    loadIndex(Index.CALCS_WITH_SHARDS);
  }

  @After
  public void resetParams() throws IOException {
    resetMaxResultWindow(TEST_INDEX_PHRASE);
    resetQuerySizeLimit();
  }

  @Test
  public void testFetchSizeLessThanMaxResultWindow() throws IOException {
    setMaxResultWindow(TEST_INDEX_PHRASE, 6);
    JSONObject response = executeQueryTemplate("SELECT * FROM %s", TEST_INDEX_PHRASE, 5);

    int numRows = 0;
    do {
      // Process response
      String cursor = response.getString("cursor");
      numRows += response.getJSONArray("datarows").length();
      response = executeCursorQuery(cursor);
    } while (response.has("cursor"));
    numRows += response.getJSONArray("datarows").length();

    var countRows =
        executeJdbcRequest("SELECT COUNT(*) FROM " + TEST_INDEX_PHRASE)
            .getJSONArray("datarows")
            .getJSONArray(0)
            .get(0);
    assertEquals(countRows, numRows);
  }

  @Test
  public void testQuerySizeLimitDoesNotEffectTotalRowsReturned() throws IOException {
    int querySizeLimit = 4;
    setQuerySizeLimit(querySizeLimit);
    JSONObject response = executeQueryTemplate("SELECT * FROM %s", TEST_INDEX_PHRASE, 5);
    assertTrue(response.getInt("size") > querySizeLimit);

    int numRows = 0;
    do {
      // Process response
      String cursor = response.getString("cursor");
      numRows += response.getJSONArray("datarows").length();
      response = executeCursorQuery(cursor);
    } while (response.has("cursor"));
    numRows += response.getJSONArray("datarows").length();
    var countRows =
        executeJdbcRequest("SELECT COUNT(*) FROM " + TEST_INDEX_PHRASE)
            .getJSONArray("datarows")
            .getJSONArray(0)
            .get(0);
    assertEquals(countRows, numRows);
    assertTrue(numRows > querySizeLimit);
  }

  @Test
  public void testQuerySizeLimitDoesNotEffectPageSize() throws IOException {
    setQuerySizeLimit(3);
    setMaxResultWindow(TEST_INDEX_PHRASE, 4);
    var response = executeQueryTemplate("SELECT * FROM %s", TEST_INDEX_PHRASE, 4);
    assertEquals(4, response.getInt("size"));

    var response2 = executeQueryTemplate("SELECT * FROM %s", TEST_INDEX_PHRASE, 2);
    assertEquals(2, response2.getInt("size"));
  }

  @Test
  public void testFetchSizeLargerThanResultWindowFails() throws IOException {
    final int window = 2;
    setMaxResultWindow(TEST_INDEX_PHRASE, 2);
    assertThrows(
        ResponseException.class,
        () -> executeQueryTemplate("SELECT * FROM %s", TEST_INDEX_PHRASE, window + 1));
    resetMaxResultWindow(TEST_INDEX_PHRASE);
  }

  @Test
  public void testMultiShardPagesEqualsActualData() throws IOException {
    // A bug made it so when pulling unordered data from an index with multiple shards, data gets
    // lost if the fetchSize
    // is not a multiple of the shard count. This tests that, for an index with 4 shards, pulling
    // one page of 10 records
    // is equivalent to pulling two pages of 5 records.

    var query = "SELECT key from " + TEST_INDEX_CALCS;

    var expectedResponse = new JSONObject(executeFetchQuery(query, 10, "jdbc"));
    var expectedRows = expectedResponse.getJSONArray("datarows");

    List<String> expectedKeys = new ArrayList<>();
    for (int i = 0; i < expectedRows.length(); i++) {
      expectedKeys.add(expectedRows.getJSONArray(i).getString(0));
    }

    var actualPage1 = new JSONObject(executeFetchQuery(query, 5, "jdbc"));

    var actualRows1 = actualPage1.getJSONArray("datarows");
    var cursor = actualPage1.getString("cursor");
    var actualPage2 = executeCursorQuery(cursor);

    var actualRows2 = actualPage2.getJSONArray("datarows");

    List<String> actualKeys = new ArrayList<>();
    for (int i = 0; i < actualRows1.length(); i++) {
      actualKeys.add(actualRows1.getJSONArray(i).getString(0));
    }
    for (int i = 0; i < actualRows2.length(); i++) {
      actualKeys.add(actualRows2.getJSONArray(i).getString(0));
    }

    assertEquals(expectedKeys, actualKeys);
  }
}
