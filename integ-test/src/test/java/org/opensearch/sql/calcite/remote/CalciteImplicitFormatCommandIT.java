/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;

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
    loadIndex(Index.ACCOUNT);
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
  public void testMultipleImplicitFormatSubsearchesFeedOneParentSearch() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " account_number=1 | fields account_number ] OR [ search source="
                + TEST_INDEX_BANK
                + " account_number=6 | fields account_number ]"
                + " | fields account_number | sort account_number");

    verifyDataRows(result, rows(1), rows(6));
  }

  @Test
  public void testNestedImplicitFormatSubsearchesFeedParentSearch() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_ACCOUNT
                + " [ search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_ACCOUNT
                + " firstname=Hattie | fields city, state ]"
                + " | fields account_number ]"
                + " | fields account_number, firstname, lastname, city, state");

    verifyDataRows(result, rows(6, "Hattie", "Bond", "Dante", "TN"));
  }

  @Test
  public void testStaticSearchPredicateCombinesWithImplicitFormatSubsearch() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " age=36 [ search source="
                + TEST_INDEX_BANK
                + " account_number=6 OR account_number=13 | fields account_number ]"
                + " | fields account_number, firstname, age | sort account_number");

    verifyDataRows(result, rows(6, "Hattie", 36));
  }

  @Test
  public void testParentPipelineAggregatesDynamicSearchResults() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " account_number=1 OR account_number=6 | fields account_number ]"
                + " | stats count() as matched");

    verifyDataRows(result, rows(2));
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
  public void testImplicitFormatExplainShowsCorrelatedDynamicScan() throws IOException {
    String result =
        explainQueryToString(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " | head 1 | eval search='account_number=1' | fields search ]"
                + " | fields account_number");

    assertTrue(result, result.contains("LogicalCorrelate"));
    assertTrue(result, result.contains("dynamicQueryString=$cor"));
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

  @Test
  public void testAppendInsideImplicitFormatSubsearch() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " account_number=1 | fields account_number"
                + " | append [ search source="
                + TEST_INDEX_BANK
                + " account_number=6 | fields account_number ] ]"
                + " | fields account_number | sort account_number");

    verifyDataRows(result, rows(1), rows(6));
  }

  @Test
  public void testAppendAfterParentDynamicSearch() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " account_number=1 | fields account_number ]"
                + " | fields account_number | append [ search source="
                + TEST_INDEX_BANK
                + " account_number=6 | fields account_number ]"
                + " | sort account_number");

    verifyDataRows(result, rows(1), rows(6));
  }

  @Test
  public void testEmptyScalarSearchFieldMatchesAllParentRows() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " | head 1 | eval search='' | fields search ]"
                + " | stats count() as matched");

    verifyDataRows(result, rows(7));
  }

  @Test
  public void testRelationalInSubqueryCanFollowImplicitFormatSearch() throws IOException {
    JSONObject result =
        executeQuery(
            "search source="
                + TEST_INDEX_BANK
                + " [ search source="
                + TEST_INDEX_BANK
                + " account_number=1 | fields account_number ]"
                + " | where account_number in [ source="
                + TEST_INDEX_BANK
                + " | where account_number in (1, 6) | fields account_number ]"
                + " | fields account_number");

    verifyDataRows(result, rows(1));
  }

  @Test
  public void testInvalidRawSearchFieldReportsActionableError() {
    Throwable exception =
        assertThrowsWithReplace(
            RuntimeException.class,
            () ->
                executeQuery(
                    "search source="
                        + TEST_INDEX_BANK
                        + " [ search source="
                        + TEST_INDEX_BANK
                        + " | head 1 | eval search='account_number=1 | head 1'"
                        + " | fields search ]"));

    verifyErrorMessageContains(
        exception,
        "The subsearch produced a value that is not a valid search predicate. Ensure the 'search'"
            + " field contains a search expression and does not include pipeline commands.");
  }
}
