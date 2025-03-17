/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

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
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.CALCS);
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
        actual, rows(40488.0, 48086, 32838, 3, "F"), rows(16377.25, 39225, 4180, 4, "M"));
  }

  @Test
  public void testAvgByField() {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats avg(balance) by gender", TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("avg(balance)", "double"));
    verifyDataRows(actual, rows(40488.0, "F"), rows(16377.25, "M"));
  }

  @Test
  public void testAvgByMultipleFields() {
    JSONObject actual1 =
        executeQuery(
            String.format("source=%s | stats avg(balance) by gender, city", TEST_INDEX_BANK));
    verifySchema(
        actual1,
        schema("avg(balance)", "double"),
        schema("gender", "string"),
        schema("city", "string"));
    verifyDataRows(
        actual1,
        rows(40540.0, "F", "Nicholson"),
        rows(32838.0, "F", "Nogal"),
        rows(48086.0, "F", "Veguita"),
        rows(39225.0, "M", "Brogan"),
        rows(5686.0, "M", "Dante"),
        rows(4180.0, "M", "Orick"),
        rows(16418.0, "M", "Ribera"));

    JSONObject actual2 =
        executeQuery(
            String.format("source=%s | stats avg(balance) by city, gender", TEST_INDEX_BANK));
    verifySchema(
        actual2,
        schema("avg(balance)", "double"),
        schema("city", "string"),
        schema("gender", "string"));
    verifyDataRows(
        actual2,
        rows(39225.0, "Brogan", "M"),
        rows(5686.0, "Dante", "M"),
        rows(40540.0, "Nicholson", "F"),
        rows(32838.0, "Nogal", "F"),
        rows(4180.0, "Orick", "M"),
        rows(16418.0, "Ribera", "M"),
        rows(48086.0, "Veguita", "F"));
  }

  @Test
  public void testStatsBySpanAndMultipleFields() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats count() by span(age,10), gender, state", TEST_INDEX_BANK));
    verifySchemaInOrder(
        response,
        schema("count()", null, "long"),
        schema("span(age,10)", null, "integer"),
        schema("gender", null, "string"),
        schema("state", null, "string"));
    verifyDataRows(
        response,
        rows(1, 20, "F", "VA"),
        rows(1, 30, "F", "IN"),
        rows(1, 30, "F", "PA"),
        rows(1, 30, "M", "IL"),
        rows(1, 30, "M", "MD"),
        rows(1, 30, "M", "TN"),
        rows(1, 30, "M", "WA"));
  }

  @Test
  public void testStatsByMultipleFieldsAndSpan() throws IOException {
    // Use verifySchemaInOrder() and verifyDataRows() to check that the span column is always
    // the first column in result whatever the order of span in query is first or last one
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats count() by gender, state, span(age,10)", TEST_INDEX_BANK));
    verifySchemaInOrder(
        response,
        schema("count()", null, "long"),
        schema("span(age,10)", null, "integer"),
        schema("gender", null, "string"),
        schema("state", null, "string"));
    verifyDataRows(
        response,
        rows(1, 20, "F", "VA"),
        rows(1, 30, "F", "IN"),
        rows(1, 30, "F", "PA"),
        rows(1, 30, "M", "IL"),
        rows(1, 30, "M", "MD"),
        rows(1, 30, "M", "TN"),
        rows(1, 30, "M", "WA"));
  }

  @org.junit.Test
  public void testAvgBySpan() {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats avg(balance) by span(age, 10)", TEST_INDEX_BANK));
    verifySchema(actual, schema("span(age,10)", "integer"), schema("avg(balance)", "double"));
    verifyDataRows(actual, rows(32838.0, 20), rows(25689.166666666668, 30));
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
        schema("age_span", "integer"),
        schema("avg(balance)", "double"));
    verifyDataRows(actual, rows(32838.0, 20, "F"), rows(44313.0, 30, "F"), rows(16377.25, 30, "M"));
  }

  @Test
  public void testAvgByTimeSpanAndFields() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) by span(birthdate, 1 month) as age_balance",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("age_balance", "timestamp"), schema("avg(balance)", "double"));
    verifyDataRows(
        actual,
        rows(39225, "2017-10-01 00:00:00"),
        rows(24628, "2018-06-01 00:00:00"),
        rows(4180, "2018-11-01 00:00:00"),
        rows(44313, "2018-08-01 00:00:00"),
        rows(5686, "2017-11-01 00:00:00"));
  }

  @Test
  public void testCountByCustomMinuteTimeSpan() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 5 | stats count(datetime0) by span(datetime0, 15 minute) as"
                    + " datetime_span",
                TEST_INDEX_CALCS));
    verifySchema(actual, schema("datetime_span", "timestamp"), schema("count(datetime0)", "long"));
    verifyDataRows(
        actual,
        rows(1, "2004-07-26 12:30:00"),
        rows(1, "2004-07-28 23:30:00"),
        rows(1, "2004-07-09 10:15:00"),
        rows(1, "2004-08-02 07:45:00"),
        rows(1, "2004-07-05 13:00:00"));
  }

  @Test
  public void testCountDistinct() {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats distinct_count(state) by gender", TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("distinct_count(state)", "long"));
    verifyDataRows(actual, rows(3, "F"), rows(4, "M"));
  }

  @Test
  public void testCountDistinctWithAlias() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats distinct_count(state) as dc by gender", TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("dc", "long"));
    verifyDataRows(actual, rows(3, "F"), rows(4, "M"));
  }

  @Ignore("https://github.com/opensearch-project/sql/issues/3353")
  public void testApproxCountDistinct() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats distinct_count_approx(state) by gender", TEST_INDEX_BANK));
  }

  @Test
  public void testVarSampVarPop() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats var_samp(balance) as vs, var_pop(balance) as vp by gender",
                TEST_INDEX_BANK));
    verifySchema(
        actual, schema("gender", "string"), schema("vs", "double"), schema("vp", "double"));
    verifyDataRows(
        actual,
        rows(58127404, 38751602.666666664, "F"),
        rows(261699024.91666666, 196274268.6875, "M"));
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
        rows(7624.132999889233, 6225.078526947806, "F"),
        rows(16177.114233282358, 14009.791885945344, "M"));
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

  @Test
  public void testSumGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats sum(balance) as a by age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "long"), schema("age", null, "integer"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(32838, 28),
        rows(39225, 32),
        rows(4180, 33),
        rows(48086, 34),
        rows(null, 36));
  }

  @Test
  public void testAvgGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as a by age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "integer"));
    verifyDataRows(
        response,
        rows(null, null),
        rows(32838, 28),
        rows(39225, 32),
        rows(4180, 33),
        rows(48086, 34),
        rows(null, 36));
  }

  @Test
  public void testSumEmpty() {
    String response =
        execute(
            String.format(
                "source=%s | where 1=2 | stats sum(balance)", TEST_INDEX_BANK_WITH_NULL_VALUES));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"sum(balance)\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + "      null\n"
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        response);
  }

  // TODO https://github.com/opensearch-project/sql/issues/3408
  // In most databases, below test returns null instead of 0.
  @Test
  public void testSumNull() {
    String response =
        execute(
            String.format(
                "source=%s | where age = 36 | stats sum(balance)",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    assertEquals(
        ""
            + "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"sum(balance)\",\n"
            + "      \"type\": \"long\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + (isPushdownEnabled() ? "      0\n" : "      null\n")
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        response);
  }
}
