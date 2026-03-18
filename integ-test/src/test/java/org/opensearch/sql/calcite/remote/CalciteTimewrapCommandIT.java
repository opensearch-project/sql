/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.util.MatcherUtils.*;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteTimewrapCommandIT extends PPLIntegTestCase {

  // Standard WHERE clause covering all test data — simulates frontend time picker
  private static final String WHERE_ALL =
      " | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <= '2024-07-04 06:00:00'";
  private static final String WHERE_JUL1_TO_JUL3 =
      " | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <= '2024-07-03 18:00:00'";
  private static final String WHERE_JUL2_TO_JUL3 =
      " | where @timestamp >= '2024-07-02 00:00:00' and @timestamp <= '2024-07-03 18:00:00'";
  private static final String WHERE_JUL1_ONLY =
      " | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <= '2024-07-01 18:00:00'";
  private static final String WHERE_JUL1_TO_JUL2 =
      " | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <= '2024-07-02 18:00:00'";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
    loadIndex(Index.TIMEWRAP_TEST);
  }

  // --- Day-over-day with different aggregations ---

  @Test
  public void testTimewrapDayOverDayWithSum() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h sum(requests) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_3days_before", "bigint"),
        schema("sum(requests)_2days_before", "bigint"),
        schema("sum(requests)_1day_before", "bigint"),
        schema("sum(requests)_latest_day", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-04 00:00:00", 180, 205, 165, 80),
        rows("2024-07-04 06:00:00", 240, 260, 225, 100),
        rows("2024-07-04 12:00:00", null, 310, 330, 285),
        rows("2024-07-04 18:00:00", null, 190, 215, 165));
  }

  @Test
  public void testTimewrapDayOverDayWithAvg() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h avg(requests) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("avg(requests)_3days_before", "double"),
        schema("avg(requests)_2days_before", "double"),
        schema("avg(requests)_1day_before", "double"),
        schema("avg(requests)_latest_day", "double"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-04 00:00:00", 90.0, 102.5, 82.5, 40.0),
        rows("2024-07-04 06:00:00", 120.0, 130.0, 112.5, 50.0),
        rows("2024-07-04 12:00:00", null, 155.0, 165.0, 142.5),
        rows("2024-07-04 18:00:00", null, 95.0, 107.5, 82.5));
  }

  @Test
  public void testTimewrapDayOverDayWithCount() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test" + WHERE_ALL + " | timechart span=6h count() | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("count()_3days_before", "bigint"),
        schema("count()_2days_before", "bigint"),
        schema("count()_1day_before", "bigint"),
        schema("count()_latest_day", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-04 00:00:00", 2, 2, 2, 2),
        rows("2024-07-04 06:00:00", 2, 2, 2, 2),
        rows("2024-07-04 12:00:00", null, 2, 2, 2),
        rows("2024-07-04 18:00:00", null, 2, 2, 2));
  }

  @Test
  public void testTimewrapWithDifferentAggField() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h sum(errors) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(errors)_3days_before", "bigint"),
        schema("sum(errors)_2days_before", "bigint"),
        schema("sum(errors)_1day_before", "bigint"),
        schema("sum(errors)_latest_day", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-04 00:00:00", 2, 4, 1, 0),
        rows("2024-07-04 06:00:00", 6, 6, 3, 1),
        rows("2024-07-04 12:00:00", null, 5, 9, 6),
        rows("2024-07-04 18:00:00", null, 1, 3, 1));
  }

  // --- Incomplete period / null fill ---

  @Test
  public void testTimewrapIncompletePeriodNullFill() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h sum(requests) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_3days_before", "bigint"),
        schema("sum(requests)_2days_before", "bigint"),
        schema("sum(requests)_1day_before", "bigint"),
        schema("sum(requests)_latest_day", "bigint"));
    verifyDataRowsSome(result, rows("2024-07-04 12:00:00", null, 310, 330, 285));
    verifyDataRowsSome(result, rows("2024-07-04 18:00:00", null, 190, 215, 165));
  }

  // --- Different timescales ---

  @Test
  public void testTimewrapWeekSpanSinglePeriod() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h sum(requests) | timewrap 1week");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("sum(requests)_latest_week", "bigint"));
    verifyDataRowsSome(result, rows("2024-07-04 00:00:00", 80), rows("2024-07-04 06:00:00", 100));
  }

  @Test
  public void testTimewrapTwelveHourSpan() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h sum(requests) | timewrap 12h");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_72hours_before", "bigint"),
        schema("sum(requests)_60hours_before", "bigint"),
        schema("sum(requests)_48hours_before", "bigint"),
        schema("sum(requests)_36hours_before", "bigint"),
        schema("sum(requests)_24hours_before", "bigint"),
        schema("sum(requests)_12hours_before", "bigint"),
        schema("sum(requests)_latest_hour", "bigint"));
    verifyDataRowsSome(result, rows("2024-07-04 00:00:00", 180, 310, 205, 330, 165, 285, 80));
  }

  @Test
  public void testTimewrapWithMinuteSpan() throws IOException {
    loadIndex(Index.EVENTS);
    JSONObject result =
        executeQuery(
            "source=events | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <="
                + " '2024-07-01 00:04:00' | timechart span=1m count() | timewrap 1min");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("count()_4minutes_before", "bigint"),
        schema("count()_3minutes_before", "bigint"),
        schema("count()_2minutes_before", "bigint"),
        schema("count()_1minute_before", "bigint"),
        schema("count()_latest_minute", "bigint"));
    verifyDataRows(result, rows("2024-07-01 00:04:00", 1, 1, 1, 1, 1));
  }

  // --- WHERE clause with different time ranges ---

  @Test
  public void testTimewrapWithWhereThreeDays() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_JUL1_TO_JUL3
                + " | timechart span=6h sum(requests) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_2days_before", "bigint"),
        schema("sum(requests)_1day_before", "bigint"),
        schema("sum(requests)_latest_day", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-03 00:00:00", 180, 205, 165),
        rows("2024-07-03 06:00:00", 240, 260, 225),
        rows("2024-07-03 12:00:00", 310, 330, 285),
        rows("2024-07-03 18:00:00", 190, 215, 165));
  }

  @Test
  public void testTimewrapWithWhereTwoDays() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_JUL2_TO_JUL3
                + " | timechart span=6h sum(requests) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_1day_before", "bigint"),
        schema("sum(requests)_latest_day", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-03 00:00:00", 205, 165),
        rows("2024-07-03 06:00:00", 260, 225),
        rows("2024-07-03 12:00:00", 330, 285),
        rows("2024-07-03 18:00:00", 215, 165));
  }

  @Test
  public void testTimewrapWithWhereSingleDay() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_JUL1_ONLY
                + " | timechart span=6h sum(requests) | timewrap 1day");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("sum(requests)_latest_day", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-01 00:00:00", 180),
        rows("2024-07-01 06:00:00", 240),
        rows("2024-07-01 12:00:00", 310),
        rows("2024-07-01 18:00:00", 190));
  }

  @Test
  public void testTimewrapWithWhereAvg() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_JUL1_TO_JUL2
                + " | timechart span=6h avg(requests) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("avg(requests)_1day_before", "double"),
        schema("avg(requests)_latest_day", "double"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-02 00:00:00", 90.0, 102.5),
        rows("2024-07-02 06:00:00", 120.0, 130.0),
        rows("2024-07-02 12:00:00", 155.0, 165.0),
        rows("2024-07-02 18:00:00", 95.0, 107.5));
  }

  @Test
  public void testTimewrapWithWhere12hSpan() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_JUL1_TO_JUL2
                + " | timechart span=6h sum(requests) | timewrap 12h");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_36hours_before", "bigint"),
        schema("sum(requests)_24hours_before", "bigint"),
        schema("sum(requests)_12hours_before", "bigint"),
        schema("sum(requests)_latest_hour", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-02 12:00:00", 180, 310, 205, 330),
        rows("2024-07-02 18:00:00", 240, 190, 260, 215));
  }

  @Test
  public void testTimewrapWithWhereCount() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_JUL1_TO_JUL3
                + " | timechart span=6h count() | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("count()_2days_before", "bigint"),
        schema("count()_1day_before", "bigint"),
        schema("count()_latest_day", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-03 00:00:00", 2, 2, 2),
        rows("2024-07-03 06:00:00", 2, 2, 2),
        rows("2024-07-03 12:00:00", 2, 2, 2),
        rows("2024-07-03 18:00:00", 2, 2, 2));
  }

  @Test
  public void testTimewrapWithWhereErrors() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_JUL2_TO_JUL3
                + " | timechart span=6h sum(errors) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(errors)_1day_before", "bigint"),
        schema("sum(errors)_latest_day", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-03 00:00:00", 4, 1),
        rows("2024-07-03 06:00:00", 6, 3),
        rows("2024-07-03 12:00:00", 9, 6),
        rows("2024-07-03 18:00:00", 3, 1));
  }

  // --- WHERE upper bound above data: shifts column numbers ---

  @Test
  public void testTimewrapWithWhereUpperBoundAboveData() throws IOException {
    // WHERE upper bound = July 10 (~5.75 days after max data July 4 06:00)
    // baseOffset = floor(Jul10/86400) - floor(Jul4_06/86400) = 5
    // periodFromNow for oldest(rel=4): (5+4-1)*1=8, newest(rel=1): (5+1-1)*1=5
    JSONObject result =
        executeQuery(
            "source=timewrap_test | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <="
                + " '2024-07-10 00:00:00' | timechart span=6h sum(requests) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_8days_before", "bigint"),
        schema("sum(requests)_7days_before", "bigint"),
        schema("sum(requests)_6days_before", "bigint"),
        schema("sum(requests)_5days_before", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-04 00:00:00", 180, 205, 165, 80),
        rows("2024-07-04 06:00:00", 240, 260, 225, 100),
        rows("2024-07-04 12:00:00", null, 310, 330, 285),
        rows("2024-07-04 18:00:00", null, 190, 215, 165));
  }

  // --- align=end vs align=now ---

  @Test
  public void testTimewrapAlignEndIsDefault() throws IOException {
    JSONObject resultDefault =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h sum(requests) | timewrap 1day");
    JSONObject resultEnd =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h sum(requests) | timewrap 1day align=end");

    verifySchema(
        resultEnd,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_3days_before", "bigint"),
        schema("sum(requests)_2days_before", "bigint"),
        schema("sum(requests)_1day_before", "bigint"),
        schema("sum(requests)_latest_day", "bigint"));
    verifyDataRowsInOrder(
        resultEnd,
        rows("2024-07-04 00:00:00", 180, 205, 165, 80),
        rows("2024-07-04 06:00:00", 240, 260, 225, 100),
        rows("2024-07-04 12:00:00", null, 310, 330, 285),
        rows("2024-07-04 18:00:00", null, 190, 215, 165));
    verifyDataRowsInOrder(
        resultDefault,
        rows("2024-07-04 00:00:00", 180, 205, 165, 80),
        rows("2024-07-04 06:00:00", 240, 260, 225, 100),
        rows("2024-07-04 12:00:00", null, 310, 330, 285),
        rows("2024-07-04 18:00:00", null, 190, 215, 165));
  }

  @Test
  public void testTimewrapAlignNow() throws IOException {
    // align=now uses current time — column names are dynamic
    // Extract actual column names from the result for verification
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h sum(requests) | timewrap 1day align=now");

    // Get actual column names from result schema
    org.json.JSONArray schemaArr = result.getJSONArray("schema");
    String c1 = schemaArr.getJSONObject(1).getString("name");
    String c2 = schemaArr.getJSONObject(2).getString("name");
    String c3 = schemaArr.getJSONObject(3).getString("name");
    String c4 = schemaArr.getJSONObject(4).getString("name");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema(c1, "bigint"),
        schema(c2, "bigint"),
        schema(c3, "bigint"),
        schema(c4, "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-04 00:00:00", 180, 205, 165, 80),
        rows("2024-07-04 06:00:00", 240, 260, 225, 100),
        rows("2024-07-04 12:00:00", null, 310, 330, 285),
        rows("2024-07-04 18:00:00", null, 190, 215, 165));
  }

  // --- Every timescale ---

  @Test
  public void testTimewrapSecondSpan() throws IOException {
    // 5 events at minute-level, wrap by 1 minute (60sec)
    // timechart span=1m gives 3 buckets (00:00, 01:00, 02:00)
    // timewrap 1min: each bucket is in a different 1-minute period → 1 offset row, 3 periods
    loadIndex(Index.EVENTS);
    JSONObject result =
        executeQuery(
            "source=events | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <="
                + " '2024-07-01 00:02:00' | timechart span=1m count() | timewrap 1min");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("count()_2minutes_before", "bigint"),
        schema("count()_1minute_before", "bigint"),
        schema("count()_latest_minute", "bigint"));
    verifyDataRows(result, rows("2024-07-01 00:02:00", 1, 1, 1));
  }

  @Test
  public void testTimewrapMinuteSpan() throws IOException {
    loadIndex(Index.EVENTS);
    JSONObject result =
        executeQuery(
            "source=events | where @timestamp >= '2024-07-01 00:00:00' and @timestamp <="
                + " '2024-07-01 00:04:00' | timechart span=1m count() | timewrap 1min");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("count()_4minutes_before", "bigint"),
        schema("count()_3minutes_before", "bigint"),
        schema("count()_2minutes_before", "bigint"),
        schema("count()_1minute_before", "bigint"),
        schema("count()_latest_minute", "bigint"));
    verifyDataRows(result, rows("2024-07-01 00:04:00", 1, 1, 1, 1, 1));
  }

  @Test
  public void testTimewrapHourSpan() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_JUL1_TO_JUL2
                + " | timechart span=6h sum(requests) | timewrap 12h");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_36hours_before", "bigint"),
        schema("sum(requests)_24hours_before", "bigint"),
        schema("sum(requests)_12hours_before", "bigint"),
        schema("sum(requests)_latest_hour", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-02 12:00:00", 180, 310, 205, 330),
        rows("2024-07-02 18:00:00", 240, 190, 260, 215));
  }

  @Test
  public void testTimewrapDaySpan() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_JUL1_TO_JUL3
                + " | timechart span=6h sum(requests) | timewrap 1day");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_2days_before", "bigint"),
        schema("sum(requests)_1day_before", "bigint"),
        schema("sum(requests)_latest_day", "bigint"));
    verifyDataRowsInOrder(
        result,
        rows("2024-07-03 00:00:00", 180, 205, 165),
        rows("2024-07-03 06:00:00", 240, 260, 225),
        rows("2024-07-03 12:00:00", 310, 330, 285),
        rows("2024-07-03 18:00:00", 190, 215, 165));
  }

  @Test
  public void testTimewrapWeekSpan() throws IOException {
    JSONObject result =
        executeQuery(
            "source=timewrap_test"
                + WHERE_ALL
                + " | timechart span=6h sum(requests) | timewrap 1week");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("sum(requests)_latest_week", "bigint"));
    verifyDataRowsSome(result, rows("2024-07-04 00:00:00", 80), rows("2024-07-04 06:00:00", 100));
  }

  @Test
  public void testTimewrapMonthSpan() throws IOException {
    // Jun 15 to Jul 4: all data within ~19 days → single 30-day month period
    JSONObject result =
        executeQuery(
            "source=timewrap_test | where @timestamp >= '2024-06-15 00:00:00' and @timestamp <="
                + " '2024-07-04 06:00:00' | timechart span=1day sum(requests) | timewrap 1month");

    verifySchema(
        result, schema("@timestamp", "timestamp"), schema("sum(requests)_latest_month", "bigint"));
    // Display timestamps anchored to latest period — Jun 15 maps to Jul 15 offset
    verifyDataRowsSome(result, rows("2024-07-15 00:00:00", 200), rows("2024-07-01 00:00:00", 920));
  }

  @Test
  public void testTimewrapQuarterSpan() throws IOException {
    // Jan 15 to Apr 15 = ~91 days → 2 quarter periods (Jan in one, Apr in another)
    JSONObject result =
        executeQuery(
            "source=timewrap_test | where @timestamp >= '2024-01-15 00:00:00' and @timestamp <="
                + " '2024-04-15 12:00:00' | timechart span=1day sum(requests) | timewrap 1quarter");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_1quarter_before", "bigint"),
        schema("sum(requests)_latest_quarter", "bigint"));
    verifyDataRowsSome(result, rows("2024-04-15 00:00:00", 300, 350));
  }

  @Test
  public void testTimewrapYearSpan() throws IOException {
    // Jan 2024 to Jan 2025 = ~365 days → 2 year periods
    // Period 1 (1year_before): Jan 2024 data (300)
    // Period 2 (latest_year): everything else
    JSONObject result =
        executeQuery(
            "source=timewrap_test | where @timestamp >= '2024-01-15 00:00:00' and @timestamp <="
                + " '2025-01-15 12:00:00' | timechart span=1day sum(requests) | timewrap 1year");

    verifySchema(
        result,
        schema("@timestamp", "timestamp"),
        schema("sum(requests)_1year_before", "bigint"),
        schema("sum(requests)_latest_year", "bigint"));
    verifyDataRowsSome(result, rows("2025-01-15 00:00:00", null, 400));
  }
}
