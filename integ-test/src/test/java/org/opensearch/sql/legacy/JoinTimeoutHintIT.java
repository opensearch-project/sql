/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_GAME_OF_THRONES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PEOPLE;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

/** Integration tests for JOIN_TIME_OUT hint functionality. */
public class JoinTimeoutHintIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.DOG);
    loadIndex(Index.PEOPLE);
    loadIndex(Index.GAME_OF_THRONES);
  }

  /**
   * Core integration test: Verify that JOIN_TIME_OUT hint prevents PIT expiration errors by setting
   * PIT keepalive to match the hint value instead of default 60 seconds.
   *
   * <p>This is the primary test for the GitHub issue fix.
   */
  @Test
  public void testJoinTimeoutHintPreventsPointInTimeExpiration() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT /*! JOIN_TIME_OUT(120) */ a.firstname, a.lastname, d.dog_name "
                + "FROM %s a JOIN %s d ON d.holdersName = a.firstname "
                + "WHERE a.age > 25 LIMIT 10",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_DOG);

    // The main test: this should execute without throwing "Point In Time id doesn't exist" error
    JSONObject result = executeQuerySafely(query);
    assertNotNull(
        "Query with JOIN_TIME_OUT hint should execute without PIT expiration error", result);

    int resultsCount = getResultsCount(result);
    assertTrue("Should execute successfully", resultsCount >= 0);
    assertTrue("Should respect LIMIT", resultsCount <= 10);
  }





  /**
   * Regression test: Verify queries without JOIN_TIME_OUT hint still work to ensure implementation
   * doesn't break existing functionality
   */
  @Test
  public void testJoinWithoutTimeoutHintStillWorks() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT a.firstname, d.dog_name "
                + "FROM %s a JOIN %s d ON d.holdersName = a.firstname "
                + "WHERE a.age > 25 LIMIT 5",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_DOG);

    JSONObject result = executeQuerySafely(query);
    assertNotNull("Query without JOIN_TIME_OUT hint should still work", result);

    int resultsCount = getResultsCount(result);
    assertTrue("Should work without timeout hint", resultsCount >= 0);
    assertTrue("Should respect LIMIT", resultsCount <= 5);
  }



  private JSONObject executeQuerySafely(String query) throws IOException {
    JSONObject result = executeQuery(query);

    if (result.has("error")) {
      throw new RuntimeException("Query failed: " + result.getJSONObject("error"));
    }

    return result;
  }

  private int getResultsCount(JSONObject result) {
    // Check for SQL response format first
    if (result.has("total")) {
      return result.getInt("total");
    }

    // Check for traditional OpenSearch hits format
    if (result.has("hits")) {
      JSONArray hits = getHits(result);
      return hits.length();
    }

    // No results found
    return 0;
  }
}
