/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_NESTED_SIMPLE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifyNumOfRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLNestedAggregationIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.NESTED_SIMPLE);
  }

  @Test
  public void testNestedAggregation() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
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
    enabledOnlyWhenPushdownIsEnabled();
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

  @Test
  public void testNestedAggregationBySpan() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(address.area) as count_area, min(address.area) as"
                    + " min_area, max(address.area) as max_area, avg(address.area) as avg_area,"
                    + " avg(age) as avg_age by span(age, 10)",
                TEST_INDEX_NESTED_SIMPLE));
    verifyDataRows(
        actual,
        rows(0, null, null, null, 19, 10),
        rows(7, 10.24, 429.79, 241.43714285714285, 25, 20),
        rows(2, 9.99, 1000.99, 505.49, 32, 30));
  }

  @Test
  public void testNestedAggregationSingleCount() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(address.city), count(address.area)",
                TEST_INDEX_NESTED_SIMPLE));
    verifyDataRows(actual, rows(11, 9));
  }

  @Test
  public void testNestedAggregationByNestedPath() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(), min(age), min(address.area) by address.city",
                TEST_INDEX_NESTED_SIMPLE));
    verifySchema(
        actual,
        schema("count()", null, "bigint"),
        schema("min(age)", null, "bigint"),
        schema("min(address.area)", null, "double"),
        schema("address.city", null, "string"));
    verifyDataRows(
        actual,
        rows(1, null, 9.99, "Miami"),
        rows(1, null, 10.24, "New york city"),
        rows(1, null, null, "austin"),
        rows(1, null, null, "bellevue"),
        rows(1, null, null, "charlotte"),
        rows(1, null, null, "chicago"),
        rows(1, null, null, "houston"),
        rows(1, null, null, "los angeles"),
        rows(1, null, 190.5, "raleigh"),
        rows(1, null, 231.01, "san diego"),
        rows(1, null, null, "seattle"));
  }

  @Test
  public void testNestedAggregationByNestedRoot() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(), min(age) by address", TEST_INDEX_NESTED_SIMPLE));
    verifySchema(
        actual,
        schema("count()", null, "bigint"),
        schema("min(age)", null, "bigint"),
        schema("address", null, "array"));
    verifyNumOfRows(actual, 5);
  }

  @Test
  public void testRareTopNestedRoot() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    JSONObject actual =
        executeQuery(String.format("source=%s | top address", TEST_INDEX_NESTED_SIMPLE));
    verifySchemaInOrder(actual, schema("address", null, "array"), schema("count", null, "bigint"));
    verifyNumOfRows(actual, 5);
  }

  @Test
  public void testDedupNestedRoot() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    JSONObject actual =
        executeQuery(String.format("source=%s | dedup address", TEST_INDEX_NESTED_SIMPLE));
    verifySchemaInOrder(
        actual,
        schema("name", null, "string"),
        schema("address", null, "array"),
        schema("id", null, "bigint"),
        schema("age", null, "bigint"));
    verifyNumOfRows(actual, 5);
  }

  @Test
  public void testNestedAggregationThrowExceptionIfPushdownCannotApplied() throws IOException {
    enabledOnlyWhenPushdownIsEnabled();
    Throwable t =
        assertThrowsWithReplace(
            UnsupportedOperationException.class,
            () ->
                executeQuery(
                    String.format(
                        "source=%s | stats count(), min(age), min(address.area) by address",
                        TEST_INDEX_NESTED_SIMPLE)));
    verifyErrorMessageContains(t, "Cannot execute nested aggregation");
  }
}
