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
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLAppendPipeCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
  }

  @Test
  public void testAppendPipe() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age_by_gender by gender | appendpipe [ "
                    + "  sort -sum_age_by_gender ] |"
                    + " head 5",
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(actual, schema("sum_age_by_gender", "bigint"), schema("gender", "string"));
    verifyDataRows(actual, rows(14947, "F"), rows(15224, "M"), rows(15224, "M"), rows(14947, "F"));
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
  public void testAppendpipeWithMergedColumn() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum by gender |"
                    + " appendpipe [  stats sum(sum) as sum ] | head 5",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(actual, schema("sum", "bigint"), schema("gender", "string"));
    verifyDataRows(actual, rows(14947, "F"), rows(15224, "M"), rows(30171, null));
  }

  @Test
  public void testAppendpipeWithConflictTypeColumn() throws IOException {
    Exception exception =
        assertThrows(
            Exception.class,
            () ->
                executeQuery(
                    String.format(
                        Locale.ROOT,
                        "source=%s | stats sum(age) as sum by gender | appendpipe [ eval sum ="
                            + " cast(sum as double) ] | head 5",
                        TEST_INDEX_ACCOUNT)));
    assertTrue(exception.getMessage().contains("due to incompatible types"));
  }

  /** Regression test: double appendpipe with different aggregations (issue #5173). */
  @Test
  public void testDoubleAppendPipe() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age by gender"
                    + " | appendpipe [ stats avg(sum_age) as avg_sum_age ]"
                    + " | appendpipe [ stats max(sum_age) as max_sum_age ]",
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual,
        schema("sum_age", "bigint"),
        schema("gender", "string"),
        schema("avg_sum_age", "double"),
        schema("max_sum_age", "bigint"));
    // 2 original rows + 1 avg row + 1 max row
    verifyDataRows(
        actual,
        rows(14947, "F", null, null),
        rows(15224, "M", null, null),
        rows(null, null, 15085.5, null),
        rows(null, null, null, 15224));
  }

  /** Regression test: triple appendpipe with different aggregations (issue #5173). */
  @Test
  public void testTripleAppendPipe() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age by gender"
                    + " | appendpipe [ stats avg(sum_age) as avg_sum_age ]"
                    + " | appendpipe [ stats max(sum_age) as max_sum_age ]"
                    + " | appendpipe [ stats min(sum_age) as min_sum_age ]",
                TEST_INDEX_ACCOUNT));
    verifySchemaInOrder(
        actual,
        schema("sum_age", "bigint"),
        schema("gender", "string"),
        schema("avg_sum_age", "double"),
        schema("max_sum_age", "bigint"),
        schema("min_sum_age", "bigint"));
    // 2 original rows + 1 avg + 1 max + 1 min
    verifyDataRows(
        actual,
        rows(14947, "F", null, null, null),
        rows(15224, "M", null, null, null),
        rows(null, null, 15085.5, null, null),
        rows(null, null, null, 15224, null),
        rows(null, null, null, null, 14947));
  }

  /** Regression test: double appendpipe with non-aggregation (filter) subpipeline. */
  @Test
  public void testDoubleAppendPipeWithFilter() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                Locale.ROOT,
                "source=%s | stats sum(age) as sum_age by gender"
                    + " | appendpipe [ where gender = 'F' ]"
                    + " | appendpipe [ where gender = 'M' ]",
                TEST_INDEX_ACCOUNT));
    // 2 original + 1 (F filter from original) + 1 (M filter from cumulative 3 rows)
    verifyDataRows(actual, rows(14947, "F"), rows(15224, "M"), rows(14947, "F"), rows(15224, "M"));
  }
}
