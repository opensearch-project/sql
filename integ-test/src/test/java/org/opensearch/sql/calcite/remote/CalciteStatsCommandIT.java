/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_SIMPLE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.StatsCommandIT;

public class CalciteStatsCommandIT extends StatsCommandIT {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();

    loadIndex(Index.NESTED_SIMPLE);
  }

  @Test
  public void testNestedAggregation() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(address.area) as count_area, min(address.area) as"
                    + " min_area, max(address.area) as max_area, avg(address.area) as avg_area,"
                    + " avg(age) as avg_age",
                TEST_INDEX_NESTED_SIMPLE));
    verifySchemaInOrder(
        actual,
        isCalciteEnabled() ? schema("count_area", "bigint") : schema("count_area", "int"),
        schema("min_area", "double"),
        schema("max_area", "double"),
        schema("avg_area", "double"),
        schema("avg_age", "double"));
    verifyDataRows(actual, rows(9, 9.99, 1000.99, 300.11555555555555, 25.2));
  }

  @Test
  public void testNestedAggregationBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(address.area) as count_area, min(address.area) as"
                    + " min_area, max(address.area) as max_area, avg(address.area) as avg_area,"
                    + " avg(age) as avg_age by name",
                TEST_INDEX_NESTED_SIMPLE));
    verifySchemaInOrder(
        actual,
        isCalciteEnabled() ? schema("count_area", "bigint") : schema("count_area", "int"),
        schema("min_area", "double"),
        schema("max_area", "double"),
        schema("avg_area", "double"),
        schema("avg_age", "double"),
        schema("name", "string"));
    verifyDataRows(
        actual,
        rows(4, 10.24, 400.99, 209.69, 24, "abbas"),
        rows(0, null, null, null, 19, "andy"),
        rows(2, 9.99, 1000.99, 505.49, 32, "chen"),
        rows(1, 190.5, 190.5, 190.5, 25, "david"),
        rows(2, 231.01, 429.79, 330.4, 26, "peng"));
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3384")
  public void testNestedAggregationBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(address.area) as count_area, min(address.area) as"
                    + " min_area, max(address.area) as max_area, avg(address.area) as avg_area,"
                    + " avg(age) as avg_age by span(age, 10)",
                TEST_INDEX_NESTED_SIMPLE));
  }
}
