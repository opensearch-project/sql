/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/** End-to-end tests for implicit format subsearches on the query_string pushdown path. */
public class CalciteImplicitFormatCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    enabledOnlyWhenPushdownIsEnabled();
    loadIndex(Index.BANK);
  }

  @Test
  public void testImplicitFormatExecutesRawSearchField() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " | head 1 | eval search='account_number=1' | fields search ]"
                + " | fields account_number");

    verifyDataRows(result, rows(1));
  }

  @Test
  public void testImplicitFormatExecutesOrdinaryFields() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " account_number=1 OR account_number=6 | fields account_number ]"
                + " | fields account_number | sort account_number");

    verifyDataRows(result, rows(1), rows(6));
  }

  @Test
  public void testImplicitFormatUnderNotExpression() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " account_number=6 NOT [ search source="
                + TEST_INDEX_BANK
                + " account_number=1 | fields account_number ]"
                + " | fields account_number");

    verifyDataRows(result, rows(6));
  }

  @Test
  public void testImplicitFormatExplainUsesBoundPredicate() throws IOException {
    String result =
        explainQueryToString(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " | head 1 | eval search='account_number=1' | fields search ]"
                + " | fields account_number");

    assertTrue(result, result.contains("account_number:1"));
    assertFalse(result, result.contains("SCALAR_QUERY"));
  }

  @Test
  public void testPostFormatEvalResultIsExecutedByParentSearch() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " account_number=1 | fields account_number | format "
                + "| eval search=replace(search, '1', '6') ]"
                + " | fields account_number");

    verifyDataRows(result, rows(6));
  }
}
