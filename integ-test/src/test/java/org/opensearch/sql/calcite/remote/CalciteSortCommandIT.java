/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
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

  @Test
  public void testPushdownSortStringExpression() throws IOException {
    String ppl =
        String.format(
            "source=%s | eval firstname2 = substring(firstname, 0, 3) | sort firstname2 | fields"
                + " firstname2, firstname",
            TEST_INDEX_BANK_WITH_NULL_VALUES);
    if (!isPushdownDisabled()) {
      String explained = explainQueryToString(ppl);
      assertTrue(explained.contains("SORT_EXPR->[SUBSTRING($0, 0, 3) ASCENDING NULLS_FIRST]"));
    }

    JSONObject result = executeQuery(ppl);
    verifySchema(result, schema("firstname2", "string"), schema("firstname", "string"));
    verifyOrder(
        result,
        rows("Am", "Amber JOHnny"),
        rows("Da", "Dale"),
        rows("Di", "Dillard"),
        rows("El", "Elinor"),
        rows("Ha", "Hattie"),
        rows("Na", "Nanette"),
        rows("Vi", "Virginia"));
  }

  @Test
  public void testPushdownSortExpressionContainsNull() throws IOException {
    String ppl =
        String.format(
            "source=%s | eval balance2 = abs(balance) | sort -balance2 | fields balance, balance2",
            TEST_INDEX_BANK_WITH_NULL_VALUES);
    if (!isPushdownDisabled()) {
      String explained = explainQueryToString(ppl);
      assertTrue(explained.contains("SORT_EXPR->[ABS($0) DESCENDING NULLS_LAST]"));
    }

    JSONObject result = executeQuery(ppl);
    verifySchema(result, schema("balance", "bigint"), schema("balance2", "bigint"));
    verifyOrder(
        result,
        rows(48086, 48086),
        rows(39225, 39225),
        rows(32838, 32838),
        rows(4180, 4180),
        rows(null, null),
        rows(null, null),
        rows(null, null));
  }

  @Test
  public void testPushdownSortExpressionWithMixedFieldSort() throws IOException {
    String ppl =
        String.format(
            "source=%s | eval balance2 = abs(balance) | sort -balance2, account_number | fields"
                + " balance2, account_number",
            TEST_INDEX_BANK_WITH_NULL_VALUES);
    if (!isPushdownDisabled()) {
      String explained = explainQueryToString(ppl);
      assertTrue(
          explained.contains(
              "SORT_EXPR->[ABS($1) DESCENDING NULLS_LAST, account_number ASCENDING NULLS_FIRST]"));
    }

    JSONObject result = executeQuery(ppl);
    verifySchema(result, schema("balance2", "bigint"), schema("account_number", "bigint"));
    verifyOrder(
        result,
        rows(48086, 32),
        rows(39225, 1),
        rows(32838, 13),
        rows(4180, 18),
        rows(null, 6),
        rows(null, 20),
        rows(null, 25));
  }
}
