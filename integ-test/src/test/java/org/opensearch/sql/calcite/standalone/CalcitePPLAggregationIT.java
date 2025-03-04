/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.List;
import org.json.JSONObject;
import org.junit.Ignore;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;

public class CalcitePPLAggregationIT extends CalcitePPLIntegTestCase {

  @Override
  public void init() throws IOException {
    super.init();

    loadIndex(Index.BANK);
  }

  @Test
  public void testSimpleCount0() throws IOException {
    Request request1 = new Request("PUT", "/test/_doc/1?refresh=true");
    request1.setJsonEntity("{\"name\": \"hello\", \"age\": 20}");
    client().performRequest(request1);
    Request request2 = new Request("PUT", "/test/_doc/2?refresh=true");
    request2.setJsonEntity("{\"name\": \"world\", \"age\": 30}");
    client().performRequest(request2);

    JSONObject actual = executeQuery("source=test | stats count() as c");
    verifySchema(actual, schema("c", "long"));
    verifyDataRows(actual, rows(2));
  }

  @Test
  public void testSimpleCount() {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats count() as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "long"));
    verifyDataRows(actual, rows(7));
  }

  @Test
  public void testSimpleAvg() {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats avg(balance)", TEST_INDEX_BANK));
    verifySchema(actual, schema("avg(balance)", "double"));
    verifyDataRows(actual, rows(26710.428571428572));
  }

  @Test
  public void testSumAvg() {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats sum(balance)", TEST_INDEX_BANK));
    verifySchema(actual, schema("sum(balance)", "long"));

    verifyDataRows(actual, rows(186973));
  }

  @Test
  public void testMultipleAggregatesWithAliases() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as avg, max(balance) as max, min(balance) as min,"
                    + " count()",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("avg", "double"),
        schema("max", "long"),
        schema("min", "long"),
        schema("count()", "long"));
    verifyDataRows(actual, rows(26710.428571428572, 48086, 4180, 7));
  }

  @Test
  public void testMultipleAggregatesWithAliasesByClause() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as avg, max(balance) as max, min(balance) as min,"
                    + " count() as cnt by gender",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("gender", "string"),
        schema("avg", "double"),
        schema("max", "long"),
        schema("min", "long"),
        schema("cnt", "long"));
    verifyDataRows(
        actual, rows("F", 40488.0, 48086, 32838, 3), rows("M", 16377.25, 39225, 4180, 4));
  }

  @Test
  public void testAvgByField() {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats avg(balance) by gender", TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("avg(balance)", "double"));
    verifyDataRows(actual, rows("F", 40488.0), rows("M", 16377.25));
  }

  @org.junit.Test
  public void testAvgBySpan() {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats avg(balance) by span(age, 10)", TEST_INDEX_BANK));
    verifySchema(actual, schema("span(age,10)", "double"), schema("avg(balance)", "double"));
    verifyDataRows(actual, rows(20.0, 32838.0), rows(30.0, 25689.166666666668));
  }

  @Test
  public void testAvgBySpanAndFields() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) by span(age, 10) as age_span, gender",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("gender", "string"),
        schema("age_span", "double"),
        schema("avg(balance)", "double"));
    verifyDataRows(
        actual, rows("F", 30.0, 44313.0), rows("M", 30.0, 16377.25), rows("F", 20.0, 32838.0));
  }

  /**
   * TODO Calcite doesn't support group by window, but it support Tumble table function. See
   * `SqlToRelConverterTest`
   */
  @Ignore
  public void testAvgByTimeSpanAndFields() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) by span(birthdate, 1 day) as age_balance",
                TEST_INDEX_BANK));
    verifySchema(
        actual, schema("span(birthdate, 1 day)", "string"), schema("age_balance", "double"));
    verifyDataRows(actual, rows("F", 3L), rows("M", 4L));
  }

  @Test
  public void testCountDistinct() {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats distinct_count(state) by gender", TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("distinct_count(state)", "long"));
    verifyDataRows(actual, rows("F", 3L), rows("M", 4L));
  }

  @Test
  public void testCountDistinctWithAlias() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats distinct_count(state) as dc by gender", TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("dc", "long"));
    verifyDataRows(actual, rows("F", 3L), rows("M", 4L));
  }

  @Ignore
  public void testApproxCountDistinct() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats distinct_count_approx(state) by gender", TEST_INDEX_BANK));
  }

  @Test
  public void testStddevSampStddevPop() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats stddev_samp(balance) as ss, stddev_pop(balance) as sp by gender",
                TEST_INDEX_BANK));
    verifySchema(
        actual, schema("gender", "string"), schema("ss", "double"), schema("sp", "double"));
    verifyDataRows(
        actual,
        rows("F", 7624.132999889233, 6225.078526947806),
        rows("M", 16177.114233282358, 14009.791885945344));
  }

  @Test
  public void testAggWithEval() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = 1, b = a | stats avg(a) as avg_a by b", TEST_INDEX_BANK));
    verifySchema(actual, schema("b", "integer"), schema("avg_a", "double"));
    verifyDataRows(actual, rows(1, 1.0));
  }

  @Test
  public void testAggWithBackticksAlias() {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats sum(`balance`) as `sum_b`", TEST_INDEX_BANK));
    verifySchema(actual, schema("sum_b", "long"));
    verifyDataRows(actual, rows(186973L));
  }

  @Test
  public void testSimpleTwoLevelStats() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as avg_by_gender by gender | stats"
                    + " avg(avg_by_gender) as avg_avg",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("avg_avg", "double"));
    verifyDataRows(actual, rows(28432.625));
  }

  @Test
  public void testTake() {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats take(firstname, 2) as take", TEST_INDEX_BANK));
    verifySchema(actual, schema("take", "array"));
    verifyDataRows(actual, rows(List.of("Amber JOHnny", "Hattie")));
  }
}
