/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyOrder;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.SortCommandIT;

public class CalciteSortCommandIT extends SortCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
  }

  // TODO: Move this test to SortCommandIT once head-then-sort is fixed in v2.
  @Test
  public void testHeadThenSort() throws IOException {
    JSONObject result =
        executeQuery(String.format("source=%s | head 2 | sort age | fields age", TEST_INDEX_BANK));
    verifyOrder(result, rows(32), rows(36));
  }

  @Test
  public void testPushdownSortPlusExpression() throws IOException {
    String ppl =
        String.format(
            Locale.ROOT,
            "source=%s | eval age2 = age + 2 | sort age2 | fields age | head 2",
            TEST_INDEX_BANK);
    JSONObject result = executeQuery(ppl);
    verifyOrder(result, rows(28), rows(32));
  }

  @Test
  public void testPushdownSortMinusExpression() throws IOException {
    String ppl =
        String.format(
            Locale.ROOT,
            "source=%s | eval age2 = 1 - age | sort age2 | fields age | head 2",
            TEST_INDEX_BANK);
    JSONObject result = executeQuery(ppl);
    verifyOrder(result, rows(39), rows(36));
  }

  @Test
  public void testPushdownSortTimesExpression() throws IOException {
    String ppl =
        String.format(
            Locale.ROOT,
            "source=%s | eval age2 = 5 * age | sort age2 | fields age | head 2",
            TEST_INDEX_BANK);
    JSONObject result = executeQuery(ppl);
    verifyOrder(result, rows(28), rows(32));
  }

  @Test
  public void testPushdownSortByMultiExpressions() throws IOException {
    String ppl =
        String.format(
            Locale.ROOT,
            "source=%s | eval age2 = 5 * age | sort gender, age2 | fields gender, age | head 2",
            TEST_INDEX_BANK);
    JSONObject result = executeQuery(ppl);
    verifyOrder(result, rows("F", 28), rows("F", 34));
  }

  @Test
  public void testPushdownSortCastExpression() throws IOException {
    String ppl =
        String.format(
            Locale.ROOT,
            "source=%s | eval age2 = cast(age * 5 as long) | sort age2 | fields age | head 2",
            TEST_INDEX_BANK);
    JSONObject result = executeQuery(ppl);
    verifyOrder(result, rows(28), rows(32));
  }

  @Test
  public void testPushdownSortCastToDoubleExpression() throws IOException {
    // Similar to query: 'source=%s | sort num(age)'. But left query doesn't output casted column.
    String ppl =
        String.format(
            "source=%s | eval age2 = cast(age as double) | sort age2 | fields age, age2 | head 2",
            TEST_INDEX_BANK);
    String explained = explainQueryToString(ppl);
    if (!isPushdownDisabled()) {
      assertTrue(
          explained.contains(
              "SORT->[{\\n"
                  + "  \\\"age\\\" : {\\n"
                  + "    \\\"order\\\" : \\\"asc\\\",\\n"
                  + "    \\\"missing\\\" : \\\"_first\\\"\\n"
                  + "  }\\n"
                  + "}]"));
    }

    JSONObject result = executeQuery(ppl);
    verifySchema(result, schema("age", "int"), schema("age2", "double"));
    verifyOrder(result, rows(28, 28d), rows(32, 32d));
  }
}
