/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class SettingsIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testQuerySizeLimit() throws IOException {
    // Default setting, fetch 200 rows from query
    JSONObject result =
        executeQuery(String.format("search source=%s age>35 | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"), rows("Elinor"), rows("Virginia"));

    // Fetch 1 rows from query
    setQuerySizeLimit(1);
    result =
        executeQuery(String.format("search source=%s age>35 | fields firstname", TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"));
  }

  @Test
  public void testQuerySizeLimit_NoPushdown() throws IOException {
    // Default setting, fetch 200 rows from query
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | eval a = 1 | where age>35 | fields firstname",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"), rows("Elinor"), rows("Virginia"));

    // Fetch 2 rows from query
    setQuerySizeLimit(2);
    result =
        executeQuery(
            String.format(
                "search source=%s | eval a = 1 | where age>35 | fields firstname",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"), rows("Elinor"));

    // Fetch 1 rows from query
    setQuerySizeLimit(1);
    result =
        executeQuery(
            String.format(
                "search source=%s | eval a = 1 | where age>35 | fields firstname",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Hattie"));
  }
}
