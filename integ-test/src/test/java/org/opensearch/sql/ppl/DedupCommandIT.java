/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class DedupCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
  }

  @Test
  public void testDedup() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | dedup male | fields male", TEST_INDEX_BANK));
    verifyDataRows(result, rows(true), rows(false));
  }

  @Test
  public void testConsecutiveDedup() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | dedup male consecutive=true | fields male", TEST_INDEX_BANK));
    verifyDataRows(result, rows(true), rows(false), rows(true), rows(false));
  }

  @Test
  public void testAllowMoreDuplicates() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | dedup 2 male | fields male", TEST_INDEX_BANK));
    verifyDataRows(result, rows(true), rows(true), rows(false), rows(false));
  }

  @Test
  public void testKeepEmptyDedup() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | dedup balance keepempty=true | fields firstname, balance",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifyDataRows(
        result,
        rows("Amber JOHnny", 39225),
        rows("Hattie", null),
        rows("Nanette", 32838),
        rows("Dale", 4180),
        rows("Elinor", null),
        rows("Virginia", null),
        rows("Dillard", 48086));
  }
}
