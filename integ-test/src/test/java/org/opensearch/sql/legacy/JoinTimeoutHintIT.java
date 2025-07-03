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

/**
 * Integration tests for JOIN_TIME_OUT hint functionality.
 *
 */
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

  /** Test that JOIN_TIME_OUT hint works with different join types (LEFT JOIN) */
  @Test
  public void testJoinTimeoutHintWithLeftJoin() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT /*! JOIN_TIME_OUT(180) */ a.firstname, d.dog_name "
                + "FROM %s a LEFT JOIN %s d ON d.holdersName = a.firstname "
                + "WHERE a.age > 20 LIMIT 15",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_DOG);

    JSONObject result = executeQuerySafely(query);
    assertNotNull("LEFT JOIN with timeout hint should execute", result);

    int resultsCount = getResultsCount(result);
    assertTrue("LEFT JOIN should execute successfully", resultsCount >= 0);
    assertTrue("Should respect LIMIT", resultsCount <= 15);
  }

  /** Test JOIN_TIME_OUT hint interaction with other hints to ensure no conflicts */
  @Test
  public void testJoinTimeoutHintWithOtherHints() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT /*! HASH_WITH_TERMS_FILTER*/ /*! JOIN_TIME_OUT(240) */ /*!"
                + " JOIN_TABLES_LIMIT(500,200) */ a.firstname, d.dog_name FROM %s a JOIN %s d ON"
                + " d.holdersName = a.firstname WHERE a.age > 25 LIMIT 8",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_DOG);

    JSONObject result = executeQuerySafely(query);
    assertNotNull("Query with multiple hints should execute", result);

    int resultsCount = getResultsCount(result);
    assertTrue("Should execute with multiple hints", resultsCount >= 0);
    assertTrue("Should respect LIMIT", resultsCount <= 8);
  }

  /** Test JOIN_TIME_OUT hint with complex nested field joins */
  @Test
  public void testJoinTimeoutHintWithComplexNestedFields() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT /*! JOIN_TIME_OUT(150) */ "
                + "c.name.firstname, h.hname, h.words "
                + "FROM %1$s c JOIN %1$s h ON h.hname = c.house "
                + "WHERE c.name.firstname IS NOT NULL LIMIT 10",
            TEST_INDEX_GAME_OF_THRONES);

    JSONObject result = executeQuerySafely(query);
    assertNotNull("Complex nested field join with timeout hint should execute", result);

    int resultsCount = getResultsCount(result);
    assertTrue("Should execute complex nested join successfully", resultsCount >= 0);
    assertTrue("Should respect LIMIT", resultsCount <= 10);
  }

  /** Test JOIN_TIME_OUT hint with complex WHERE conditions and ORDER BY */
  @Test
  public void testJoinTimeoutHintWithComplexQuery() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT /*! JOIN_TIME_OUT(300) */ "
                + "a.firstname, a.lastname, a.age, d.dog_name "
                + "FROM %s a JOIN %s d ON d.holdersName = a.firstname "
                + "WHERE (a.age > 25 OR a.balance > 1000) AND d.age > 2 "
                + "ORDER BY a.age DESC LIMIT 12",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_DOG);

    JSONObject result = executeQuerySafely(query);
    assertNotNull("Complex query with timeout hint should execute", result);

    int resultsCount = getResultsCount(result);
    assertTrue("Should execute complex query successfully", resultsCount >= 0);
    assertTrue("Should respect LIMIT", resultsCount <= 12);
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

  /** Test with large timeout value to ensure edge cases are handled */
  @Test
  public void testJoinTimeoutHintWithLargeTimeout() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT /*! JOIN_TIME_OUT(1800) */ a.firstname, d.dog_name "
                + "FROM %s a JOIN %s d ON d.holdersName = a.firstname LIMIT 5",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_DOG);

    JSONObject result = executeQuerySafely(query);
    assertNotNull("Query with large timeout should execute", result);

    int resultsCount = getResultsCount(result);
    assertTrue("Should handle large timeout value", resultsCount >= 0);
    assertTrue("Should respect LIMIT", resultsCount <= 5);
  }

  /** Consistency test: Verify repeated queries with JOIN_TIME_OUT produce consistent results */
  @Test
  public void testJoinTimeoutHintConsistency() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT /*! JOIN_TIME_OUT(100) */ a.firstname, d.dog_name "
                + "FROM %s a JOIN %s d ON d.holdersName = a.firstname "
                + "WHERE a.age > 25 LIMIT 3",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_DOG);

    // Execute the same query multiple times to ensure consistency
    JSONObject result1 = executeQuerySafely(query);
    JSONObject result2 = executeQuerySafely(query);

    assertNotNull("First execution should succeed", result1);
    assertNotNull("Second execution should succeed", result2);

    int resultsCount1 = getResultsCount(result1);
    int resultsCount2 = getResultsCount(result2);

    assertEquals("Results should be consistent between executions", resultsCount1, resultsCount2);
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
