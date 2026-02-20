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

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testTrailingPipeAfterSource() throws IOException {
    // Query with trailing pipe should produce same results as without
    JSONObject resultWithout = executeQuery(String.format("source=%s", TEST_INDEX_ACCOUNT));
    JSONObject resultWith = executeQuery(String.format("source=%s |", TEST_INDEX_ACCOUNT));

    // Both should return the same data
    assertEquals(resultWithout.toString(), resultWith.toString());
  }

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

    assertEquals(resultWithout.toString(), resultWith.toString());
  }

  @Test
  public void testTrailingPipeAfterHead() throws IOException {
    JSONObject resultWithout =
        executeQuery(
            String.format("source=%s | fields firstname, age | head 3", TEST_INDEX_ACCOUNT));
    JSONObject resultWith =
        executeQuery(
            String.format("source=%s | fields firstname, age | head 3 |", TEST_INDEX_ACCOUNT));

    assertEquals(resultWithout.toString(), resultWith.toString());
  }

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

    assertEquals(resultWithout.toString(), resultWith.toString());
  }

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

    assertEquals(resultNormal.toString(), resultWithEmpty.toString());
  }

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

    assertEquals(resultNormal.toString(), resultWithEmpty.toString());
  }

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

    assertEquals(resultNormal.toString(), resultWithEmpty.toString());
  }
}
