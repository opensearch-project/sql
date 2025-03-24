/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.util.MatcherUtils.columnName;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyColumn;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;

public class SearchCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.DOG);
  }

  @Test
  public void testSearchAllFields() throws IOException {
    JSONObject result = executeQuery(String.format("search source=%s", TEST_INDEX_DOG));
    verifyColumn(result, columnName("dog_name"), columnName("holdersName"), columnName("age"));
  }

  @Test
  public void testSearchCommandWithoutSearchKeyword() throws IOException {
    assertEquals(
        executeQueryToString(String.format("search source=%s", TEST_INDEX_BANK)),
        executeQueryToString(String.format("source=%s", TEST_INDEX_BANK)));
  }

  @Test
  public void testSearchCommandWithSpecialIndexName() throws IOException {
    executeRequest(new Request("PUT", "/logs-2021.01.11"));
    verifyDataRows(executeQuery("search source=logs-2021.01.11"));

    executeRequest(new Request("PUT", "/logs-7.10.0-2021.01.11"));
    verifyDataRows(executeQuery("search source=logs-7.10.0-2021.01.11"));
  }

  @Test
  public void testSearchCommandWithLogicalExpression() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s firstname='Hattie' | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void searchCommandWithoutSourceShouldFailToParse() throws IOException {
    try {
      executeQuery("search firstname='Hattie'");
      fail();
    } catch (ResponseException e) {
      assertTrue(e.getMessage().contains("RuntimeException"));
      assertTrue(e.getMessage().contains("Failed to parse query due to offending symbol"));
    }
  }
}
