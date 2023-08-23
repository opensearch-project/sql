/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PHRASE;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class PaginationWindowIT extends SQLIntegTestCase {
  @Override
  public void init() throws IOException {
    loadIndex(Index.PHRASE);
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
}
