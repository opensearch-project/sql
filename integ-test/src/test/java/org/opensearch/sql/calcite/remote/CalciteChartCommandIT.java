/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_OTEL_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TIME_DATA;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteChartCommandIT extends PPLIntegTestCase {
  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.OTELLOGS);
    loadIndex(Index.TIME_TEST_DATA);
  }

  @Test
  public void testChartWithSingleGroupKey() throws IOException {
    JSONObject result1 =
        executeQuery(String.format("source=%s | chart avg(balance) by gender", TEST_INDEX_BANK));
    verifySchema(result1, schema("avg(balance)", "double"), schema("gender", "string"));
    verifyDataRows(result1, rows(40488, "F"), rows(16377.25, "M"));
    JSONObject result2 =
        executeQuery(String.format("source=%s | chart avg(balance) over gender", TEST_INDEX_BANK));
    assertJsonEquals(result1.toString(), result2.toString());
  }

  @Test
  public void testChartWithMultipleGroupKeys() throws IOException {
    JSONObject result1 =
        executeQuery(
            String.format("source=%s | chart avg(balance) over gender by age", TEST_INDEX_BANK));
    verifySchema(
        result1,
        schema("gender", "string"),
        schema("age", "string"),
        schema("avg(balance)", "double"));
    verifyDataRows(
        result1,
        rows("F", "28", 32838),
        rows("F", "39", 40540),
        rows("M", "32", 39225),
        rows("M", "33", 4180),
        rows("M", "36", 11052),
        rows("F", "34", 48086));
    JSONObject result2 =
        executeQuery(
            String.format("source=%s | chart avg(balance) by gender, age", TEST_INDEX_BANK));
    assertJsonEquals(result1.toString(), result2.toString());
  }

  @Test
  public void testChartCombineOverByWithLimit0() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart limit=0 avg(balance) over state by gender", TEST_INDEX_BANK));
    verifySchema(
        result,
        schema("avg(balance)", "double"),
        schema("state", "string"),
        schema("gender", "string"));
    verifyDataRows(
        result,
        rows(39225.0, "IL", "M"),
        rows(48086.0, "IN", "F"),
        rows(4180.0, "MD", "M"),
        rows(40540.0, "PA", "F"),
        rows(5686.0, "TN", "M"),
        rows(32838.0, "VA", "F"),
        rows(16418.0, "WA", "M"));
  }

  @Test
  public void testChartMaxBalanceByAgeSpan() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | chart max(balance) by age span=10", TEST_INDEX_BANK));
    verifySchema(result, schema("max(balance)", "bigint"), schema("age", "int"));
    verifyDataRows(result, rows(32838, 20), rows(48086, 30));
  }

  @Test
  public void testChartMaxValueOverTimestampSpanWeekByCategory() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart max(value) over timestamp span=1week by category",
                TEST_INDEX_TIME_DATA));
    verifySchema(
        result,
        schema("timestamp", "timestamp"),
        schema("category", "string"),
        schema("max(value)", "int"));
    // Data spans from 2025-07-28 to 2025-08-01, all within same week
    verifyDataRows(
        result,
        rows("2025-07-28 00:00:00", "A", 9367),
        rows("2025-07-28 00:00:00", "B", 9521),
        rows("2025-07-28 00:00:00", "C", 9187),
        rows("2025-07-28 00:00:00", "D", 8736));
  }

  @Test
  public void testChartMaxValueOverCategoryByTimestampSpanWeek() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart max(value) over category by timestamp span=1week",
                TEST_INDEX_TIME_DATA));
    verifySchema(
        result,
        schema("category", "string"),
        schema("timestamp", "string"),
        schema("max(value)", "int"));
    // All data within same week span
    verifyDataRows(
        result,
        rows("A", "2025-07-28 00:00:00", 9367),
        rows("B", "2025-07-28 00:00:00", 9521),
        rows("C", "2025-07-28 00:00:00", 9187),
        rows("D", "2025-07-28 00:00:00", 8736));
  }

  @Test
  public void testChartMaxValueByTimestampSpanDayAndWeek() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart max(value) by timestamp span=1day, @timestamp span=2weeks",
                TEST_INDEX_TIME_DATA));
    // column split are converted to string in order to be compatible with nullstr and otherstr
    verifySchema(
        result,
        schema("timestamp", "timestamp"),
        schema("@timestamp", "string"),
        schema("max(value)", "int"));
    // Data grouped by day spans
    verifyDataRows(
        result,
        rows("2025-07-28 00:00:00", "2025-07-28 00:00:00", 9367),
        rows("2025-07-29 00:00:00", "2025-07-28 00:00:00", 9521),
        rows("2025-07-30 00:00:00", "2025-07-28 00:00:00", 9234),
        rows("2025-07-31 00:00:00", "2025-07-28 00:00:00", 9318),
        rows("2025-08-01 00:00:00", "2025-07-28 00:00:00", 9015));
  }

  @Test
  public void testChartLimit0WithUseOther() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart limit=0 useother=true otherstr='max_among_other'"
                    + " max(severityNumber) over flags by severityText",
                TEST_INDEX_OTEL_LOGS));
    verifySchema(
        result,
        schema("max(severityNumber)", "bigint"),
        schema("flags", "bigint"),
        schema("severityText", "string"));
    verifyDataRows(
        result,
        rows(5, 0, "DEBUG"),
        rows(6, 0, "DEBUG2"),
        rows(7, 0, "DEBUG3"),
        rows(8, 0, "DEBUG4"),
        rows(17, 0, "ERROR"),
        rows(18, 0, "ERROR2"),
        rows(19, 0, "ERROR3"),
        rows(20, 0, "ERROR4"),
        rows(21, 0, "FATAL"),
        rows(22, 0, "FATAL2"),
        rows(23, 0, "FATAL3"),
        rows(24, 0, "FATAL4"),
        rows(9, 0, "INFO"),
        rows(10, 0, "INFO2"),
        rows(11, 0, "INFO3"),
        rows(12, 0, "INFO4"),
        rows(2, 0, "TRACE2"),
        rows(3, 0, "TRACE3"),
        rows(4, 0, "TRACE4"),
        rows(13, 0, "WARN"),
        rows(14, 0, "WARN2"),
        rows(15, 0, "WARN3"),
        rows(16, 0, "WARN4"),
        rows(17, 1, "ERROR"),
        rows(9, 1, "INFO"),
        rows(1, 1, "TRACE"));
  }

  @Test
  public void testChartLimitTopWithUseOther() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart limit=top 2 useother=true otherstr='max_among_other'"
                    + " max(severityNumber) over flags by severityText",
                TEST_INDEX_OTEL_LOGS));
    verifySchema(
        result,
        schema("flags", "bigint"),
        schema("severityText", "string"),
        schema("max(severityNumber)", "bigint"));
    verifyDataRows(
        result,
        rows(1, "max_among_other", 17),
        rows(0, "max_among_other", 22),
        rows(0, "FATAL3", 23),
        rows(0, "FATAL4", 24));
  }

  @Test
  public void testChartLimitBottomWithUseOther() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart limit=bottom 2 useother=false otherstr='other_small_not_shown'"
                    + " max(severityNumber) over flags by severityText",
                TEST_INDEX_OTEL_LOGS));
    verifySchema(
        result,
        schema("flags", "bigint"),
        schema("severityText", "string"),
        schema("max(severityNumber)", "bigint"));
    verifyDataRows(result, rows(1, "TRACE", 1), rows(0, "TRACE2", 2));
  }

  @Test
  public void testChartLimitTopWithMinAgg() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart limit=top 2 min(severityNumber) over flags by severityText",
                TEST_INDEX_OTEL_LOGS));
    verifySchema(
        result,
        schema("flags", "bigint"),
        schema("severityText", "string"),
        schema("min(severityNumber)", "bigint"));
    verifyDataRows(
        result,
        rows(1, "OTHER", 9),
        rows(1, "TRACE", 1),
        rows(0, "OTHER", 3),
        rows(0, "TRACE2", 2));
  }

  @Test
  public void testChartUseNullTrueWithNullStr() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart nullstr='nil' avg(balance) over gender by age span=10",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(
        result,
        schema("gender", "string"),
        schema("age", "string"),
        schema("avg(balance)", "double"));
    verifyDataRows(
        result,
        rows("M", "30", 21702.5),
        rows("F", "30", 48086.0),
        rows("F", "20", 32838.0),
        rows("F", "nil", null));
  }

  @Test
  public void testChartUseNullFalseWithNullStr() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | chart usenull=false nullstr='not_shown' count() over gender by age"
                    + " span=10",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(
        result, schema("gender", "string"), schema("age", "string"), schema("count()", "bigint"));
    verifyDataRows(result, rows("M", "30", 4), rows("F", "30", 1), rows("F", "20", 1));
  }
}
