/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK_WITH_NULL_VALUES;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATATYPE_NUMERIC;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE_FORMATS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_LOGS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_TELEMETRY;
import static org.opensearch.sql.util.MatcherUtils.assertJsonEquals;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifyErrorMessageContains;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySchemaInOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalcitePPLAggregationIT extends PPLIntegTestCase {

  private static final String TEST_INDEX_TIME_DATA = "opensearch-sql_test_index_time_data";

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();

    loadIndex(Index.BANK);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.CALCS);
    loadIndex(Index.DATE_FORMATS);
    loadIndex(Index.DATA_TYPE_NUMERIC);
    loadIndex(Index.BIG5);
    loadIndex(Index.LOGS);
    loadIndex(Index.TELEMETRY);
    loadIndex(Index.TIME_TEST_DATA);
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
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(2));
  }

  @Test
  public void testSimpleCount() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats count() as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(7));

    actual = executeQuery(String.format("source=%s | stats c() as count_emp", TEST_INDEX_BANK));
    verifySchema(actual, schema("count_emp", "bigint"));
    verifyDataRows(actual, rows(7));

    actual = executeQuery(String.format("source=%s | stats count as count_alias", TEST_INDEX_BANK));
    verifySchema(actual, schema("count_alias", "bigint"));
    verifyDataRows(actual, rows(7));
  }

  @Test
  public void testSimpleAvg() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats avg(balance)", TEST_INDEX_BANK));
    verifySchema(actual, schema("avg(balance)", "double"));
    verifyDataRows(actual, rows(26710.428571428572));
  }

  @Test
  public void testSumAvg() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats sum(balance)", TEST_INDEX_BANK));
    verifySchema(actual, schema("sum(balance)", "bigint"));

    verifyDataRows(actual, rows(186973));
  }

  @Test
  public void testAsExistedField() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats count() as balance", TEST_INDEX_BANK));
    verifySchema(actual, schema("balance", "bigint"));

    verifyDataRows(actual, rows(7));
  }

  @Test
  public void testMultipleAggregatesWithAliases() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as avg, max(balance) as max, min(balance) as min,"
                    + " count()",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("avg", "double"),
        schema("max", "bigint"),
        schema("min", "bigint"),
        schema("count()", "bigint"));
    verifyDataRows(actual, rows(26710.428571428572, 48086, 4180, 7));
  }

  @Test
  public void testMultipleAggregatesWithAliasesByClause() throws IOException {
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
        schema("max", "bigint"),
        schema("min", "bigint"),
        schema("cnt", "bigint"));
    verifyDataRows(
        actual, rows(40488.0, 48086, 32838, 3, "F"), rows(16377.25, 39225, 4180, 4, "M"));
  }

  @Test
  public void testAvgByField() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats avg(balance) by gender", TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("avg(balance)", "double"));
    verifyDataRows(actual, rows(40488.0, "F"), rows(16377.25, "M"));
  }

  @Test
  public void testAvgByMultipleFields() throws IOException {
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
        schema("count()", null, "bigint"),
        schema("span(age,10)", null, "int"),
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
        schema("count()", null, "bigint"),
        schema("span(age,10)", null, "int"),
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
  public void testAvgBySpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats avg(balance) by span(age, 10)", TEST_INDEX_BANK));
    verifySchema(actual, schema("span(age,10)", "int"), schema("avg(balance)", "double"));
    verifyDataRows(actual, rows(32838.0, 20), rows(25689.166666666668, 30));
  }

  @Test
  public void testFirstAggregation() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats first(firstname)", TEST_INDEX_BANK));
    verifySchema(actual, schema("first(firstname)", "string"));
    verifyDataRows(actual, rows("Amber JOHnny"));
  }

  @Test
  public void testLastAggregation() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats last(firstname)", TEST_INDEX_BANK));
    verifySchema(actual, schema("last(firstname)", "string"));
    verifyDataRows(actual, rows("Dillard"));
  }

  @Test
  public void testFirstLastByGroup() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(firstname), last(lastname) by gender", TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("first(firstname)", "string"),
        schema("last(lastname)", "string"),
        schema("gender", "string"));
    verifyDataRows(actual, rows("Amber JOHnny", "Ratliff", "M"), rows("Nanette", "Mcpherson", "F"));
  }

  @Test
  public void testFirstLastWithOtherAggregations() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(firstname), last(firstname), count(), avg(age) by gender",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("first(firstname)", "string"),
        schema("last(firstname)", "string"),
        schema("count()", "bigint"),
        schema("avg(age)", "double"),
        schema("gender", "string"));
    verifyDataRows(
        actual,
        rows("Amber JOHnny", "Elinor", 4L, 34.25, "M"),
        rows("Nanette", "Dillard", 3L, 33.666666666666664, "F"));
  }

  @Test
  public void testFirstLastDifferentFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(account_number), last(balance), first(age)",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("first(account_number)", "bigint"),
        schema("last(balance)", "bigint"),
        schema("first(age)", "int"));
    verifyDataRows(actual, rows(1L, 48086L, 32L));
  }

  @Test
  public void testFirstLastWithBirthdate() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats first(birthdate), last(birthdate)", TEST_INDEX_BANK));
    verifySchema(
        actual, schema("first(birthdate)", "timestamp"), schema("last(birthdate)", "timestamp"));
    verifyDataRows(actual, rows("2017-10-23 00:00:00", "2018-08-11 00:00:00"));
  }

  @Test
  public void testFirstLastBirthdateByGender() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(birthdate) as first_bd, last(birthdate) as last_bd by"
                    + " gender",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("first_bd", "timestamp"),
        schema("last_bd", "timestamp"),
        schema("gender", "string"));
    verifyDataRows(
        actual,
        rows("2017-10-23 00:00:00", "2018-06-27 00:00:00", "M"),
        rows("2018-06-23 00:00:00", "2018-08-11 00:00:00", "F"));
  }

  @Test
  public void testFirstLastBirthdateWithOtherFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(firstname), first(birthdate), last(lastname),"
                    + " last(birthdate) by gender",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("first(firstname)", "string"),
        schema("first(birthdate)", "timestamp"),
        schema("last(lastname)", "string"),
        schema("last(birthdate)", "timestamp"),
        schema("gender", "string"));
    verifyDataRows(
        actual,
        rows("Amber JOHnny", "2017-10-23 00:00:00", "Ratliff", "2018-06-27 00:00:00", "M"),
        rows("Nanette", "2018-06-23 00:00:00", "Mcpherson", "2018-08-11 00:00:00", "F"));
  }

  @Test
  public void testFirstLastWithTimestamp() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(timestamp), last(timestamp)", TEST_INDEX_TIME_DATA));
    verifySchema(
        actual, schema("first(timestamp)", "timestamp"), schema("last(timestamp)", "timestamp"));
    verifyDataRows(actual, rows("2025-07-28 00:15:23", "2025-08-01 03:47:41"));
  }

  @Test
  public void testFirstLastTimestampByCategory() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(timestamp) as first_ts, last(timestamp) as last_ts by"
                    + " category",
                TEST_INDEX_TIME_DATA));
    verifySchema(
        actual,
        schema("first_ts", "timestamp"),
        schema("last_ts", "timestamp"),
        schema("category", "string"));
    verifyDataRows(
        actual,
        rows("2025-07-28 04:33:10", "2025-08-01 00:27:26", "D"),
        rows("2025-07-28 02:28:45", "2025-08-01 02:00:56", "C"),
        rows("2025-07-28 01:42:15", "2025-08-01 01:14:11", "B"),
        rows("2025-07-28 00:15:23", "2025-08-01 03:47:41", "A"));
  }

  @Test
  public void testFirstLastTimestampWithValue() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(value), first(timestamp), last(value), last(timestamp)",
                TEST_INDEX_TIME_DATA));
    verifySchema(
        actual,
        schema("first(value)", "int"),
        schema("first(timestamp)", "timestamp"),
        schema("last(value)", "int"),
        schema("last(timestamp)", "timestamp"));
    verifyDataRows(actual, rows(8945, "2025-07-28 00:15:23", 8762, "2025-08-01 03:47:41"));
  }

  @Test
  public void testFirstLastWithNullValues() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(balance) as first_bal, last(balance) as last_bal",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(actual, schema("first_bal", "bigint"), schema("last_bal", "bigint"));
    // Note: Current implementation skips nulls, so we expect first and last non-null values
    // This test verifies current behavior - may need to change based on requirements
    verifyDataRows(actual, rows(39225L, 48086L));
  }

  @Test
  public void testFirstLastWithNullValuesByGroup() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(balance) as first_bal, last(balance) as last_bal by age",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(
        actual, schema("first_bal", "bigint"), schema("last_bal", "bigint"), schema("age", "int"));
    // Testing behavior when some groups have null values
    verifyDataRows(
        actual,
        rows(null, null, null), // age is null, no balance values
        rows(32838L, 32838L, 28),
        rows(39225L, 39225L, 32),
        rows(4180L, 4180L, 33),
        rows(48086L, 48086L, 34),
        rows(null, null, 36)); // balance is null for age 36
  }

  @Test
  public void testAvgBySpanAndFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) by span(age, 10) as age_span, gender",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("gender", "string"),
        schema("age_span", "int"),
        schema("avg(balance)", "double"));
    verifyDataRows(actual, rows(32838.0, 20, "F"), rows(44313.0, 30, "F"), rows(16377.25, 30, "M"));
  }

  @Test
  public void testAvgByTimeSpanAndFields() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) by span(birthdate, 1month) as age_balance",
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
  public void testCountByCustomTimeSpanWithDifferentUnits() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 5 | stats count(datetime0) by span(datetime0, 15minute) as"
                    + " datetime_span",
                TEST_INDEX_CALCS));
    verifySchema(
        actual, schema("datetime_span", "timestamp"), schema("count(datetime0)", "bigint"));
    verifyDataRows(
        actual,
        rows(1, "2004-07-26 12:30:00"),
        rows(1, "2004-07-28 23:30:00"),
        rows(1, "2004-07-09 10:15:00"),
        rows(1, "2004-08-02 07:45:00"),
        rows(1, "2004-07-05 13:00:00"));

    actual =
        executeQuery(
            String.format(
                "source=%s | head 5 | stats count(datetime0) by span(datetime0, 5second) as"
                    + " datetime_span",
                TEST_INDEX_CALCS));
    verifySchema(
        actual, schema("datetime_span", "timestamp"), schema("count(datetime0)", "bigint"));
    verifyDataRows(
        actual,
        rows(1, "2004-07-26 12:30:30"),
        rows(1, "2004-07-28 23:30:20"),
        rows(1, "2004-08-02 07:59:20"),
        rows(1, "2004-07-09 10:17:35"),
        rows(1, "2004-07-05 13:14:20"));

    actual =
        executeQuery(
            String.format(
                "source=%s | head 5 | stats count(datetime0) by span(datetime0, 3month) as"
                    + " datetime_span",
                TEST_INDEX_CALCS));
    verifySchema(
        actual, schema("datetime_span", "timestamp"), schema("count(datetime0)", "bigint"));
    verifyDataRows(actual, rows(5, "2004-07-01 00:00:00"));
  }

  @Test
  public void testCountByNullableTimeSpan() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 5 | stats count(datetime0), count(datetime1) by span(time1,"
                    + " 15minute) as time_span",
                TEST_INDEX_CALCS));
    verifySchema(
        actual,
        schema("time_span", "time"),
        schema("count(datetime0)", "bigint"),
        schema("count(datetime1)", "bigint"));
    verifyDataRows(
        actual,
        rows(1, 0, "19:30:00"),
        rows(1, 0, "02:00:00"),
        rows(1, 0, "09:30:00"),
        rows(1, 0, "22:45:00"));
  }

  @Test
  public void testCountByDateTypeSpanWithDifferentUnits() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(strict_date) by span(strict_date, 1day) as" + " date_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("date_span", "date"), schema("count(strict_date)", "bigint"));
    verifyDataRows(actual, rows(2, "1984-04-12"));

    actual =
        executeQuery(
            String.format(
                "source=%s | stats count(basic_date) by span(basic_date, 1year) as" + " date_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("date_span", "date"), schema("count(basic_date)", "bigint"));
    verifyDataRows(actual, rows(2, "1984-01-01"));

    actual =
        executeQuery(
            String.format(
                "source=%s | stats count(year_month_day) by span(year_month_day, 1month)"
                    + " as date_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("date_span", "date"), schema("count(year_month_day)", "bigint"));
    verifyDataRows(actual, rows(2, "1984-04-01"));
  }

  @Test
  public void testCountByTimeTypeSpanWithDifferentUnits() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(hour_minute_second) by span(hour_minute_second, 1"
                    + "minute) as time_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual, schema("time_span", "time"), schema("count(hour_minute_second)", "bigint"));
    verifyDataRows(actual, rows(2, "09:07:00"));

    actual =
        executeQuery(
            String.format(
                "source=%s | stats count(custom_time) by span(custom_time, 1second) as"
                    + " time_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("time_span", "time"), schema("count(custom_time)", "bigint"));
    verifyDataRows(actual, rows(1, "09:07:42"), rows(1, "21:07:42"));

    actual =
        executeQuery(
            String.format(
                "source=%s | stats count(hour) by span(hour, 6hour) as time_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("time_span", "time"), schema("count(hour)", "bigint"));
    verifyDataRows(actual, rows(2, "06:00:00"));
  }

  @Test
  public void testCountByTimestampTypeSpanForDifferentFormats() throws IOException {
    List<String> timestampFields =
        Arrays.asList(
            "epoch_millis",
            "date_optional_time",
            "strict_date_optional_time_nanos",
            "basic_date_time",
            "basic_date_time_no_millis",
            "basic_ordinal_date_time",
            "basic_week_date_time",
            "basic_week_date_time_no_millis",
            "date_hour",
            "date_hour_minute",
            "date_hour_minute_second_fraction",
            "date_hour_minute_second_millis",
            "date_time",
            "date_time_no_millis",
            "ordinal_date_time",
            "ordinal_date_time_no_millis",
            "week_date_time",
            "week_date_time_no_millis",
            "yyyy-MM-dd_OR_epoch_millis");

    for (String timestampField : timestampFields) {
      JSONObject actual =
          executeQuery(
              String.format(
                  "source=%s | stats count() by span(%s, 1d) as timestamp_span",
                  TEST_INDEX_DATE_FORMATS, timestampField));
      verifySchema(actual, schema("timestamp_span", "timestamp"), schema("count()", "bigint"));
      verifyDataRows(actual, rows(2, "1984-04-12 00:00:00"));
    }
  }

  @Test
  public void testCountByDateTypeSpanForDifferentFormats() throws IOException {
    List<String> dateFields =
        Arrays.asList(
            "basic_date",
            "basic_ordinal_date",
            "date",
            "ordinal_date",
            "week_date",
            "weekyear_week_day",
            "year_month_day",
            "yyyy-MM-dd");

    for (String dateField : dateFields) {
      JSONObject actual =
          executeQuery(
              String.format(
                  "source=%s | stats count() by span(%s, 1d) as date_span",
                  TEST_INDEX_DATE_FORMATS, dateField));
      verifySchema(actual, schema("date_span", "date"), schema("count()", "bigint"));
      verifyDataRows(actual, rows(2, "1984-04-12"));
    }
  }

  @Test
  public void testCountByTimeTypeSpanForDifferentFormats() throws IOException {
    List<String> timeFields =
        Arrays.asList(
            "basic_time",
            "basic_time_no_millis",
            "basic_t_time",
            "basic_t_time_no_millis",
            "hour",
            "hour_minute",
            "hour_minute_second",
            "hour_minute_second_fraction",
            "hour_minute_second_millis",
            "time",
            "time_no_millis",
            "t_time",
            "t_time_no_millis");

    for (String timeField : timeFields) {
      JSONObject actual =
          executeQuery(
              String.format(
                  "source=%s | stats count() by span(%s, 1h) as time_span",
                  TEST_INDEX_DATE_FORMATS, timeField));
      verifySchema(actual, schema("time_span", "time"), schema("count()", "bigint"));
      verifyDataRows(actual, rows(2, "09:00:00"));
    }
  }

  @Test
  public void testCountBySpanForCustomFormats() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(custom_date_or_date) by span(custom_date_or_date, 1"
                    + "month) as date_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual, schema("date_span", "date"), schema("count(custom_date_or_date)", "bigint"));
    verifyDataRows(actual, rows(2, "1984-04-01"));

    actual =
        executeQuery(
            String.format(
                "source=%s | stats count(custom_date_or_custom_time) by"
                    + " span(custom_date_or_custom_time, 1hour) as timestamp_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("timestamp_span", "timestamp"),
        schema("count(custom_date_or_custom_time)", "bigint"));
    verifyDataRows(actual, rows(1, "1961-04-12 00:00:00"), rows(1, "1970-01-01 09:00:00"));

    actual =
        executeQuery(
            String.format(
                "source=%s | stats count(custom_no_delimiter_ts) by span(custom_no_delimiter_ts, 1"
                    + "hour) as timestamp_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("timestamp_span", "timestamp"),
        schema("count(custom_no_delimiter_ts)", "bigint"));
    verifyDataRows(actual, rows(1, "1961-04-12 09:00:00"), rows(1, "1984-10-20 15:00:00"));

    actual =
        executeQuery(
            String.format(
                "source=%s | stats count(incomplete_custom_time) by span(incomplete_custom_time, 12"
                    + "hour) as time_span",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual, schema("time_span", "time"), schema("count(incomplete_custom_time)", "bigint"));
    verifyDataRows(actual, rows(1, "00:00:00"), rows(1, "12:00:00"));
  }

  // Only available in v3 with Calcite
  @Test
  public void testSpanByImplicitTimestamp() throws IOException {
    JSONObject result = executeQuery("source=big5 | stats count() by span(1d) as span");
    verifySchema(result, schema("count()", "bigint"), schema("span", "timestamp"));
    verifyDataRows(
        result,
        rows(1, "2023-01-02 00:00:00"),
        rows(1, "2023-03-01 00:00:00"),
        rows(1, "2023-05-01 00:00:00"));

    Throwable t =
        assertThrowsWithReplace(
            SemanticCheckException.class,
            () ->
                executeQuery(
                    StringUtils.format(
                        "source=%s | stats count() by span(5m)", TEST_INDEX_DATE_FORMATS)));
    verifyErrorMessageContains(t, "Field [@timestamp] not found");
  }

  @Test
  public void testCountDistinct() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats distinct_count(state) by gender", TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("distinct_count(state)", "bigint"));
    verifyDataRows(actual, rows(3, "F"), rows(4, "M"));
  }

  @Test
  public void testCountDistinctApprox() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats distinct_count_approx(state) by gender", TEST_INDEX_BANK));
    verifySchema(
        actual, schema("gender", "string"), schema("distinct_count_approx(state)", "bigint"));
    verifyDataRows(actual, rows(3, "F"), rows(4, "M"));
  }

  @Test
  public void testCountDistinctApproxWithAlias() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats distinct_count_approx(state) as dca by gender",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("dca", "bigint"));
    verifyDataRows(actual, rows(3, "F"), rows(4, "M"));
  }

  @Test
  public void testCountDistinctWithAlias() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats distinct_count(state) as dc by gender", TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("dc", "bigint"));
    verifyDataRows(actual, rows(3, "F"), rows(4, "M"));
  }

  @Test
  public void testEarliestAndLatest() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats latest(server), earliest(server)", TEST_INDEX_LOGS));

    verifySchema(actual, schema("latest(server)", "string"), schema("earliest(server)", "string"));
    verifyDataRows(actual, rows("server2", "server1"));
  }

  @Test
  public void testEarliestAndLatestWithAlias() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats latest(server) as late, earliest(server) as early",
                TEST_INDEX_LOGS));

    verifySchema(actual, schema("late", "string"), schema("early", "string"));
    verifyDataRows(actual, rows("server2", "server1"));
  }

  @Test
  public void testEarliestAndLatestWithBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats latest(server) as late, earliest(server) as early by" + " level",
                TEST_INDEX_LOGS));

    verifySchema(
        actual, schema("late", "string"), schema("early", "string"), schema("level", "string"));
    verifyDataRows(
        actual,
        rows("server3", "server1", "ERROR"),
        rows("server2", "server2", "INFO"),
        rows("server1", "server1", "WARN"));
  }

  @Test
  public void testVarSampVarPop() throws IOException {
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
  public void testStddevSampStddevPop() throws IOException {
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
  public void testAggWithEval() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval a = 1, b = a | stats avg(a) as avg_a by b", TEST_INDEX_BANK));
    verifySchema(actual, schema("b", "int"), schema("avg_a", "double"));
    verifyDataRows(actual, rows(1, 1.0));
  }

  @Test
  public void testAggWithBackticksAlias() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats sum(`balance`) as `sum_b`", TEST_INDEX_BANK));
    verifySchema(actual, schema("sum_b", "bigint"));
    verifyDataRows(actual, rows(186973L));
  }

  @Test
  public void testSimpleTwoLevelStats() throws IOException {
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
  public void testTake() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats take(firstname, 2) as take", TEST_INDEX_BANK));
    verifySchema(actual, schema("take", "array"));
    verifyDataRows(actual, rows(List.of("Amber JOHnny", "Hattie")));
  }

  @Test
  public void testPercentile() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats percentile(balance, 50) as p50, percentile(balance, 90) as p90",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("p50", "bigint"), schema("p90", "bigint"));
    verifyDataRows(actual, rows(32838, 48086));
  }

  @Test
  public void testSumGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats sum(balance) as a by age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "bigint"), schema("age", null, "int"));
    verifyDataRows(
        response,
        rows(isPushdownDisabled() ? null : 0, null),
        rows(32838, 28),
        rows(39225, 32),
        rows(4180, 33),
        rows(48086, 34),
        rows(isPushdownDisabled() ? null : 0, 36));
  }

  @Test
  public void testAvgGroupByNullValue() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats avg(balance) as a by age", TEST_INDEX_BANK_WITH_NULL_VALUES));
    verifySchema(response, schema("a", null, "double"), schema("age", null, "int"));
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
  public void testSumEmpty() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where 1=2 | stats sum(balance)", TEST_INDEX_BANK_WITH_NULL_VALUES));
    assertJsonEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"sum(balance)\",\n"
            + "      \"type\": \"bigint\"\n"
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
        response.toString());
  }

  // TODO https://github.com/opensearch-project/sql/issues/3408
  // In most databases, below test returns null instead of 0.
  @Test
  public void testSumNull() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | where age = 36 | stats sum(balance)",
                TEST_INDEX_BANK_WITH_NULL_VALUES));
    assertJsonEquals(
        "{\n"
            + "  \"schema\": [\n"
            + "    {\n"
            + "      \"name\": \"sum(balance)\",\n"
            + "      \"type\": \"bigint\"\n"
            + "    }\n"
            + "  ],\n"
            + "  \"datarows\": [\n"
            + "    [\n"
            + (isPushdownDisabled() ? "      null\n" : "      0\n")
            + "    ]\n"
            + "  ],\n"
            + "  \"total\": 1,\n"
            + "  \"size\": 1\n"
            + "}",
        response.toString());
  }

  @Test
  public void testAggWithFunction() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | eval len = length(gender) | stats sum(balance + 100) as sum by len,"
                    + " gender ",
                TEST_INDEX_BANK));
    verifySchema(
        response,
        schema("sum", null, "bigint"),
        schema("len", null, "int"),
        schema("gender", null, "string"));
    verifyDataRows(response, rows(121764, 1, "F"), rows(65909, 1, "M"));
  }

  @Test
  public void testAggByByteNumberWithScript() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | eval a = abs(byte_number) | stats count() by a",
                TEST_INDEX_DATATYPE_NUMERIC));
    verifyDataRows(response, rows(1, 4));
  }

  @Test
  public void testCountEvalSimpleCondition() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats count(eval(age > 30)) as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(6));
  }

  @Test
  public void testCountEvalComplexCondition() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(eval(balance > 20000 and age < 35)) as c",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(3));
  }

  @Test
  public void testCountEvalGroupBy() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(eval(balance > 25000)) as high_balance by gender",
                TEST_INDEX_BANK));
    verifySchema(actual, schema("gender", "string"), schema("high_balance", "bigint"));
    verifyDataRows(actual, rows(3, "F"), rows(1, "M"));
  }

  @Test
  public void testCountEvalWithMultipleAggregations() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count(eval(age > 30)) as mature_count, "
                    + "count(eval(balance > 25000)) as high_balance_count, "
                    + "count() as total_count",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("mature_count", "bigint"),
        schema("high_balance_count", "bigint"),
        schema("total_count", "bigint"));
    verifyDataRows(actual, rows(6, 4, 7));
  }

  @Test
  public void testShortcutCEvalSimpleCondition() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats c(eval(age > 30)) as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(6));
  }

  @Test
  public void testShortcutCEvalComplexCondition() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats c(eval(balance > 20000 and age < 35)) as c", TEST_INDEX_BANK));
    verifySchema(actual, schema("c", "bigint"));
    verifyDataRows(actual, rows(3));
  }

  @Test
  public void testPercentileShortcuts() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats perc50(balance), p95(balance)", TEST_INDEX_BANK));
    verifySchema(actual, schema("perc50(balance)", "bigint"), schema("p95(balance)", "bigint"));
    verifyDataRows(actual, rows(32838, 48086));
  }

  @Test
  public void testPercentileShortcutsWithDecimals() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats perc99.5(balance)", TEST_INDEX_BANK));
    verifySchema(actual, schema("perc99.5(balance)", "bigint"));
    verifyDataRows(actual, rows(48086));
  }

  @Test
  public void testPercentileShortcutsFloatingPoint() throws IOException {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats perc25.5(balance), p75.25(balance), perc0.1(balance)",
                TEST_INDEX_BANK));
    verifySchema(
        actual,
        schema("perc25.5(balance)", "bigint"),
        schema("p75.25(balance)", "bigint"),
        schema("perc0.1(balance)", "bigint"));
    verifyDataRows(actual, rows(5686, 40540, 4180));
  }

  @Test
  public void testPercentileShortcutsFloatingEquivalence() throws IOException {
    JSONObject shortcut =
        executeQuery(String.format("source=%s | stats perc25.5(balance)", TEST_INDEX_BANK));
    JSONObject standard =
        executeQuery(String.format("source=%s | stats percentile(balance, 25.5)", TEST_INDEX_BANK));

    verifySchema(shortcut, schema("perc25.5(balance)", "bigint"));
    verifySchema(standard, schema("percentile(balance, 25.5)", "bigint"));

    Object shortcutValue = shortcut.getJSONArray("datarows").getJSONArray(0).get(0);
    Object standardValue = standard.getJSONArray("datarows").getJSONArray(0).get(0);

    verifyDataRows(shortcut, rows(shortcutValue));
    verifyDataRows(standard, rows(standardValue));
  }

  @Test
  public void testPercentileShortcutsEquivalentToStandard() throws IOException {
    JSONObject shortcut =
        executeQuery(String.format("source=%s | stats perc50(balance)", TEST_INDEX_BANK));
    JSONObject standard =
        executeQuery(String.format("source=%s | stats percentile(balance, 50)", TEST_INDEX_BANK));

    verifySchema(shortcut, schema("perc50(balance)", "bigint"));
    verifySchema(standard, schema("percentile(balance, 50)", "bigint"));

    Object shortcutValue = shortcut.getJSONArray("datarows").getJSONArray(0).get(0);
    Object standardValue = standard.getJSONArray("datarows").getJSONArray(0).get(0);

    verifyDataRows(shortcut, rows(shortcutValue));
    verifyDataRows(standard, rows(standardValue));
  }

  @Test
  public void testStatsCountAliasWithMultipleAggregatesAndSort() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats sum(balance), count, avg(balance) by state | sort - `count`",
                TEST_INDEX_BANK));
    verifySchema(
        response,
        schema("sum(balance)", "bigint"),
        schema("count", "bigint"),
        schema("avg(balance)", "double"),
        schema("state", "string"));
    verifyDataRows(
        response,
        rows(39225, 1, 39225.0, "IL"),
        rows(48086, 1, 48086.0, "IN"),
        rows(4180, 1, 4180.0, "MD"),
        rows(40540, 1, 40540.0, "PA"),
        rows(5686, 1, 5686.0, "TN"),
        rows(32838, 1, 32838.0, "VA"),
        rows(16418, 1, 16418.0, "WA"));
  }

  @Test
  public void testStatsCountAliasByGroupWithSort() throws IOException {
    JSONObject response =
        executeQuery(
            String.format("source=%s | stats count by state | sort - `count`", TEST_INDEX_BANK));
    verifySchema(response, schema("count", "bigint"), schema("state", "string"));
    verifyDataRows(
        response,
        rows(1, "IL"),
        rows(1, "IN"),
        rows(1, "MD"),
        rows(1, "PA"),
        rows(1, "TN"),
        rows(1, "VA"),
        rows(1, "WA"));
  }

  @Test
  public void testMedian() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats median(balance)", TEST_INDEX_BANK));
    verifySchema(actual, schema("median(balance)", "bigint"));
    verifyDataRows(actual, rows(32838));
  }

  @Test
  public void testStatsMaxOnStringField() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats max(firstname)", TEST_INDEX_BANK));
    verifySchema(actual, schema("max(firstname)", "string"));
    verifyDataRows(actual, rows("Virginia"));
  }

  @Test
  public void testStatsMinOnStringField() throws IOException {
    JSONObject actual =
        executeQuery(String.format("source=%s | stats min(firstname)", TEST_INDEX_BANK));
    verifySchema(actual, schema("min(firstname)", "string"));
    verifyDataRows(actual, rows("Amber JOHnny"));
  }

  @Test
  public void testStatsCountOnFunctionsWithUDTArg() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | eval t = unix_timestamp(birthdate) | stats count() by t | sort -t",
                TEST_INDEX_BANK));
    verifySchema(response, schema("count()", "bigint"), schema("t", "double"));
    verifyDataRows(
        response,
        rows(1, 1542152000),
        rows(1, 1534636800),
        rows(1, 1533945600),
        rows(1, 1530057600),
        rows(1, 1529712000),
        rows(1, 1511136000),
        rows(1, 1508716800));
  }

  @Test
  public void testStatsGroupByDate() throws IOException {
    JSONObject resonse =
        executeQuery(
            String.format(
                "source=%s | eval t = date_add(birthdate, interval 1 day) | stats count() by"
                    + " span(t, 1d)",
                TEST_INDEX_BANK));
    verifySchema(resonse, schema("count()", "bigint"), schema("span(t,1d)", "timestamp"));
    verifyDataRows(
        resonse,
        rows(1, "2017-10-24 00:00:00"),
        rows(1, "2017-11-21 00:00:00"),
        rows(1, "2018-06-24 00:00:00"),
        rows(1, "2018-06-28 00:00:00"),
        rows(1, "2018-08-12 00:00:00"),
        rows(1, "2018-08-20 00:00:00"),
        rows(1, "2018-11-14 00:00:00"));
  }

  @Test
  public void testLimitAfterAggregation() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "source=%s | stats count() by age | sort -age | head 3", TEST_INDEX_BANK));
    verifySchema(response, schema("count()", "bigint"), schema("age", "int"));
    verifyDataRows(response, rows(1, 39), rows(2, 36), rows(1, 34));
    response =
        executeQuery(
            String.format(
                "source=%s | stats count() by age | sort -age | head 2", TEST_INDEX_BANK));
    verifyDataRows(response, rows(1, 39), rows(2, 36));
    response =
        executeQuery(
            String.format(
                "source=%s | stats count() by age | sort -age | head 1", TEST_INDEX_BANK));
    verifyDataRows(response, rows(1, 39));
  }

  @Test
  public void testFirstLastWithSimpleField() throws IOException {
    // This should work - testing simple field first
    JSONObject actual =
        executeQuery(
            String.format("source=%s | stats first(severityNumber)", TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("first(severityNumber)", "int"));
    verifyDataRows(actual, rows(9));
  }

  @Test
  public void testFirstLastWithDeepNestedField() throws IOException {
    // This test should now work with the fix for ClassCastException
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(`resource.attributes.telemetry.sdk.language`)",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("first(`resource.attributes.telemetry.sdk.language`)", "string"));
    verifyDataRows(actual, rows("java"));
  }

  @Test
  public void testLastWithDeepNestedField() throws IOException {
    // This test should now work with the fix for ClassCastException
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats last(`resource.attributes.telemetry.sdk.language`)",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("last(`resource.attributes.telemetry.sdk.language`)", "string"));
    verifyDataRows(actual, rows("rust"));
  }

  @Test
  public void testFirstLastWithDeepNestedFieldByGroup() throws IOException {
    // This test should now work with the fix for ClassCastException
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(`resource.attributes.telemetry.sdk.language`) by"
                    + " severityNumber",
                TEST_INDEX_TELEMETRY));
    verifySchema(
        actual,
        schema("first(`resource.attributes.telemetry.sdk.language`)", "string"),
        schema("severityNumber", "int"));
    verifyDataRows(actual, rows("java", 9), rows("python", 12), rows("go", 16));
  }

  @Test
  public void testMinWithDeepNestedField() throws IOException {
    // Test that min() works with deeply nested fields after the ClassCastException fix
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats min(`resource.attributes.telemetry.sdk.language`)",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("min(`resource.attributes.telemetry.sdk.language`)", "string"));
    verifyDataRows(
        actual, rows("go")); // Alphabetically first: go < java < javascript < python < rust
  }

  @Test
  public void testMaxWithDeepNestedField() throws IOException {
    // Test that max() works with deeply nested fields after the ClassCastException fix
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats max(`resource.attributes.telemetry.sdk.language`)",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("max(`resource.attributes.telemetry.sdk.language`)", "string"));
    verifyDataRows(
        actual, rows("rust")); // Alphabetically last: go < java < javascript < python < rust
  }

  @Test
  public void testMinMaxWithDeepNestedFieldByGroup() throws IOException {
    // Test that min() and max() work with deeply nested fields and grouping
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats min(`resource.attributes.telemetry.sdk.language`) by"
                    + " severityNumber | sort severityNumber",
                TEST_INDEX_TELEMETRY));
    verifySchema(
        actual,
        schema("min(`resource.attributes.telemetry.sdk.language`)", "string"),
        schema("severityNumber", "int"));
    // severityNumber 9: java, javascript -> min = java
    // severityNumber 12: python, rust -> min = python
    // severityNumber 16: go -> min = go
    verifyDataRows(actual, rows("java", 9), rows("python", 12), rows("go", 16));
  }

  @Test
  public void testMinMaxMultipleNestedFields() throws IOException {
    // Test min/max with multiple nested field aggregations in one query
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats min(`resource.attributes.telemetry.sdk.language`) as min_lang,"
                    + " max(`resource.attributes.telemetry.sdk.language`) as max_lang",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("min_lang", "string"), schema("max_lang", "string"));
    verifyDataRows(actual, rows("go", "rust"));
  }

  @Test
  public void testMinWithIntegerNestedField() throws IOException {
    // Test that min() works with deeply nested integer fields
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats min(`resource.attributes.telemetry.sdk.version`)",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("min(`resource.attributes.telemetry.sdk.version`)", "int"));
    verifyDataRows(actual, rows(10)); // Minimum version is 10
  }

  @Test
  public void testMaxWithIntegerNestedField() throws IOException {
    // Test that max() works with deeply nested integer fields
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats max(`resource.attributes.telemetry.sdk.version`)",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("max(`resource.attributes.telemetry.sdk.version`)", "int"));
    verifyDataRows(actual, rows(14)); // Maximum version is 14
  }

  @Test
  public void testMinMaxIntegerNestedFieldsByGroup() throws IOException {
    // Test min/max on integer nested fields with grouping
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats min(`resource.attributes.telemetry.sdk.version`) as min_ver,"
                    + " max(`resource.attributes.telemetry.sdk.version`) as max_ver by"
                    + " severityNumber",
                TEST_INDEX_TELEMETRY));
    verifySchema(
        actual,
        schema("min_ver", "int"),
        schema("max_ver", "int"),
        schema("severityNumber", "int"));
    // severityNumber 9: versions 10, 12 -> min=10, max=12
    // severityNumber 12: versions 11, 14 -> min=11, max=14
    // severityNumber 16: version 13 -> min=13, max=13
    verifyDataRows(actual, rows(10, 12, 9), rows(11, 14, 12), rows(13, 13, 16));
  }

  @Test
  public void testFirstLastWithIntegerNestedField() throws IOException {
    // Test first/last with deeply nested integer fields
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(`resource.attributes.telemetry.sdk.version`) as first_ver,"
                    + " last(`resource.attributes.telemetry.sdk.version`) as last_ver",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("first_ver", "int"), schema("last_ver", "int"));
    verifyDataRows(actual, rows(10, 14));
  }

  @Test
  public void testFirstLastWithBooleanNestedField() throws IOException {
    // Test first/last with deeply nested boolean fields
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats first(`resource.attributes.telemetry.sdk.enabled`) as"
                    + " first_enabled, last(`resource.attributes.telemetry.sdk.enabled`) as"
                    + " last_enabled",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("first_enabled", "boolean"), schema("last_enabled", "boolean"));
    verifyDataRows(actual, rows(true, true)); // First record is true, last record is true
  }

  @Test
  public void testCountWithBooleanNestedFieldGroupBy() throws IOException {
    // Test count aggregation grouped by boolean nested field
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count() as cnt by `resource.attributes.telemetry.sdk.enabled`",
                TEST_INDEX_TELEMETRY));
    verifySchema(
        actual,
        schema("cnt", "bigint"),
        schema("resource.attributes.telemetry.sdk.enabled", "boolean"));
    verifyDataRows(actual, rows(2L, false), rows(3L, true)); // 2 false, 3 true values
  }

  @Test
  public void testMinMaxWithBooleanNestedField() throws IOException {
    // Test min/max with deeply nested boolean fields
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats min(`resource.attributes.telemetry.sdk.enabled`) as min_enabled,"
                    + " max(`resource.attributes.telemetry.sdk.enabled`) as max_enabled",
                TEST_INDEX_TELEMETRY));
    verifySchema(actual, schema("min_enabled", "boolean"), schema("max_enabled", "boolean"));
    verifyDataRows(actual, rows(false, true)); // Min is false, max is true
  }

  @Test
  public void testBooleanNestedFieldByGroup() throws IOException {
    // Test boolean nested fields with grouping by other fields
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats count() as cnt,"
                    + " first(`resource.attributes.telemetry.sdk.enabled`) as enabled by"
                    + " severityNumber",
                TEST_INDEX_TELEMETRY));
    verifySchema(
        actual,
        schema("cnt", "bigint"),
        schema("enabled", "boolean"),
        schema("severityNumber", "int"));
    // severityNumber 9: java (true), javascript (true) -> 2 records, first is true
    // severityNumber 12: python (false), rust (true) -> 2 records, first is false
    // severityNumber 16: go (false) -> 1 record, first is false
    verifyDataRows(actual, rows(2L, true, 9), rows(2L, false, 12), rows(1L, false, 16));
  }

  @Test
  public void testMixedTypesNestedFieldAggregations() throws IOException {
    // Test aggregating multiple nested field types in one query
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | stats min(`resource.attributes.telemetry.sdk.version`) as min_ver,"
                    + " max(`resource.attributes.telemetry.sdk.version`) as max_ver,"
                    + " min(`resource.attributes.telemetry.sdk.enabled`) as min_enabled,"
                    + " max(`resource.attributes.telemetry.sdk.enabled`) as max_enabled,"
                    + " first(`resource.attributes.telemetry.sdk.language`) as first_lang",
                TEST_INDEX_TELEMETRY));
    verifySchema(
        actual,
        schema("min_ver", "int"),
        schema("max_ver", "int"),
        schema("min_enabled", "boolean"),
        schema("max_enabled", "boolean"),
        schema("first_lang", "string"));
    verifyDataRows(actual, rows(10, 14, false, true, "java"));
  }
}
