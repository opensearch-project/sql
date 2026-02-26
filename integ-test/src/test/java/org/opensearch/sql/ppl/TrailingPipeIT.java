/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

/** Integration tests for trailing pipe and middle empty pipe functionality in PPL queries. */
public class TrailingPipeIT extends PPLIntegTestCase {

  /**
   * Initializes the test environment by loading the account index.
   *
   * @throws Exception if initialization fails
   */
  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  /**
   * Tests that a trailing pipe after a source command produces identical results to a query without
   * the trailing pipe.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTrailingPipeAfterSource() throws IOException {
    // Query with trailing pipe should produce same results as without
    JSONObject resultWithout = executeQuery(String.format("source=%s", TEST_INDEX_ACCOUNT));
    JSONObject resultWith = executeQuery(String.format("source=%s |", TEST_INDEX_ACCOUNT));

    // Both should return the same data
    assertTrue(resultWithout.similar(resultWith));
  }

  /**
   * Tests that a trailing pipe after a fields command produces identical results to a query without
   * the trailing pipe.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTrailingPipeAfterFields() throws IOException {
    JSONObject resultWithout =
        executeQuery(
            String.format(
                "source=%s | where age > 30 | fields firstname, age", TEST_INDEX_ACCOUNT));
    JSONObject resultWith =
        executeQuery(
            String.format(
                "source=%s | where age > 30 | fields firstname, age |", TEST_INDEX_ACCOUNT));

    assertTrue(resultWithout.similar(resultWith));
  }

  /**
   * Tests that a trailing pipe after a head command produces identical results to a query without
   * the trailing pipe.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTrailingPipeAfterHead() throws IOException {
    JSONObject resultWithout =
        executeQuery(
            String.format("source=%s | fields firstname, age | head 3", TEST_INDEX_ACCOUNT));
    JSONObject resultWith =
        executeQuery(
            String.format("source=%s | fields firstname, age | head 3 |", TEST_INDEX_ACCOUNT));

    assertTrue(resultWithout.similar(resultWith));
  }

  /**
   * Tests that a trailing pipe after a complex query with multiple commands (where, fields, stats,
   * sort) produces identical results to a query without the trailing pipe.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testTrailingPipeWithComplexQuery() throws IOException {
    JSONObject resultWithout =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | fields firstname, age, state | stats avg(age) by"
                    + " state | sort state",
                TEST_INDEX_ACCOUNT));
    JSONObject resultWith =
        executeQuery(
            String.format(
                "source=%s | where age > 25 | fields firstname, age, state | stats avg(age) by"
                    + " state | sort state |",
                TEST_INDEX_ACCOUNT));

    assertTrue(resultWithout.similar(resultWith));
  }

  /**
   * Tests that an empty pipe in the middle of a query pipeline is properly ignored and produces
   * identical results to a query without the empty pipe.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testEmptyPipeInMiddle() throws IOException {
    // Empty pipe in middle should be ignored
    JSONObject resultNormal =
        executeQuery(
            String.format(
                "source=%s | where age > 30 | fields firstname, age", TEST_INDEX_ACCOUNT));
    JSONObject resultWithEmpty =
        executeQuery(
            String.format(
                "source=%s | | where age > 30 | fields firstname, age", TEST_INDEX_ACCOUNT));

    assertTrue(resultNormal.similar(resultWithEmpty));
  }

  /**
   * Tests that multiple empty pipes scattered throughout a query pipeline are properly ignored and
   * produce identical results to a query without the empty pipes.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testMultipleEmptyPipes() throws IOException {
    // Multiple empty pipes should be ignored
    JSONObject resultNormal =
        executeQuery(
            String.format(
                "source=%s | where age > 30 | fields firstname, age | sort age",
                TEST_INDEX_ACCOUNT));
    JSONObject resultWithEmpty =
        executeQuery(
            String.format(
                "source=%s | | where age > 30 | | fields firstname, age | sort age",
                TEST_INDEX_ACCOUNT));

    assertTrue(resultNormal.similar(resultWithEmpty));
  }

  /**
   * Tests that a combination of empty pipes in the middle and a trailing pipe at the end are
   * properly handled and produce identical results to a query without these extraneous pipes.
   *
   * @throws IOException if query execution fails
   */
  @Test
  public void testEmptyPipesAndTrailingPipe() throws IOException {
    // Multiple empty pipes should be ignored
    JSONObject resultNormal =
        executeQuery(
            String.format(
                "source=%s | where age > 30 | fields firstname, age | sort age",
                TEST_INDEX_ACCOUNT));
    JSONObject resultWithEmpty =
        executeQuery(
            String.format(
                "source=%s | | where age > 30 | fields firstname, age | sort age |",
                TEST_INDEX_ACCOUNT));

    assertTrue(resultNormal.similar(resultWithEmpty));
  }
}
