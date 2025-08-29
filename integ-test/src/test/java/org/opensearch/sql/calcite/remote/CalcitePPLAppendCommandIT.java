/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLAppendCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
  }

  @Test
  public void testAppend() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ source=%s |"
                    + " stats sum(age) as sum_age_by_state by state | sort sum_age_by_state ] |"
                    + " head 5",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual,
        schema("sum_age_by_gender", "bigint"),
        schema("gender", "string"),
        schema("sum_age_by_state", "bigint"),
        schema("state", "string"));
    verifyDataRows(
        actual,
        rows(14947, "F", null, null),
        rows(15224, "M", null, null),
        rows(null, null, 369, "NV"),
        rows(null, null, 412, "NM"),
        rows(null, null, 414, "AZ"));
  }

  @Test
  public void testAppendEmptySearchCommand() throws IOException {
    List<String> testPPLs =
        Arrays.asList(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ |"
                    + " stats sum(age) as sum_age_by_state by state | sort sum_age_by_state ]",
                TEST_INDEX_ACCOUNT),
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age_by_gender by gender | append [ ]",
                TEST_INDEX_ACCOUNT));

    for (String ppl : testPPLs) {
      JSONObject actual = executeQuery(ppl);
      verifySchemaInOrder(
          actual, schema("sum_age_by_gender", "bigint"), schema("gender", "string"));
      verifyDataRows(actual, rows(14947, "F"), rows(15224, "M"));
    }
  }

  @Test
  public void testAppendDifferentIndex() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum by gender | append [ source=%s | stats"
                    + " sum(age) as bank_sum_age ]",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_BANK));
    verifySchemaInOrder(
        actual,
        schema("sum", "bigint"),
        schema("gender", "string"),
        schema("bank_sum_age", "bigint"));
    verifyDataRows(actual, rows(14947, "F", null), rows(15224, "M", null), rows(null, null, 238));
  }

  @Test
  public void testAppendWithMergedColumn() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum by gender |"
                    + " append [ source=%s | stats sum(age) as sum by state | sort sum ] | head 5",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual, schema("sum", "bigint"), schema("gender", "string"), schema("state", "string"));
    verifyDataRows(
        actual,
        rows(14947, "F", null),
        rows(15224, "M", null),
        rows(369, null, "NV"),
        rows(412, null, "NM"),
        rows(414, null, "AZ"));
  }

  public void testAppendWithConflictTypeColumn() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum by gender | append [ source=%s | stats sum(age)"
                    + " as sum by state | sort sum | eval sum = cast(sum as double) ] | head 5",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual,
        schema("sum", "bigint"),
        schema("gender", "string"),
        schema("state", "string"),
        schema("sum0", "double"));
    verifyDataRows(
        actual,
        rows(14947, "F", null, null),
        rows(15224, "M", null, null),
        rows(null, null, "NV", 369d),
        rows(null, null, "NM", 412d),
        rows(null, null, "AZ", 414d));
  }
}
