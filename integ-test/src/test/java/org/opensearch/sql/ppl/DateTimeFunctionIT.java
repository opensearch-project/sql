/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PEOPLE2;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;

import java.io.IOException;
import java.time.LocalTime;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableMap;
import org.json.JSONArray;
import java.time.LocalTime;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.utils.StringUtils;

public class DateTimeFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws IOException {
    loadIndex(Index.DATE);
    loadIndex(Index.PEOPLE2);
  }

  @Test
  public void testAddDate() throws IOException {
    JSONObject result =
        executeQuery(String.format(
            "source=%s | eval f = adddate(timestamp('2020-09-16 17:30:00'), interval 1 day) | fields f", TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 17:30:00"));

    result = executeQuery(String.format(
        "source=%s | eval f = adddate(date('2020-09-16'), 1) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17"));

    result = executeQuery(String.format(
        "source=%s | eval f = adddate('2020-09-16', 1) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = adddate('2020-09-16 17:30:00', interval 1 day) | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 17:30:00"));
  }

  @Test
  public void testConvertTZ() throws IOException {
    JSONObject result =
        executeQuery(String.format(
            "source=%s | eval f = convert_tz('2008-05-15 12:00:00','+00:00','+10:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-05-15 22:00:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = convert_tz('2021-05-12 00:00:00','-00:00','+00:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 00:00:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = convert_tz('2021-05-12 00:00:00','+10:00','+11:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 01:00:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = convert_tz('2021-05-12 11:34:50','-08:00','+09:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-13 04:34:50"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = convert_tz('2021-05-30 11:34:50','-17:00','+08:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[]{null}));

    result =
        executeQuery(String.format(
            "source=%s | eval f = convert_tz('2021-05-12 11:34:50','-12:00','+15:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[]{null}));


    result = executeQuery(String.format(
        "SELECT DATETIME('2008-01-01 02:00:00+10:00')"));
    verifySchema(result,
        schema("DATETIME('2008-01-01 02:00:00+10:00')", null, "datetime"));
    verifyDataRows(result, rows("2008-01-01 02:00:00"));

    result = executeQuery(String.format(
        "SELECT DATETIME('2008-01-01 02:00:00')"));
    verifySchema(result,
        schema("DATETIME('2008-01-01 02:00:00')", null, "datetime"));
    verifyDataRows(result, rows("2008-01-01 02:00:00"));
  }

  @Test
  public void testDateAdd() throws IOException {
    JSONObject result =
        executeQuery(String.format(
            "source=%s | eval f = date_add(timestamp('2020-09-16 17:30:00'), interval 1 day) | fields f", TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 17:30:00"));

    result = executeQuery(String.format(
        "source=%s | eval f = date_add(date('2020-09-16'), 1) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17"));

    result = executeQuery(String.format(
        "source=%s | eval f = date_add('2020-09-16', 1) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = date_add('2020-09-16 17:30:00', interval 1 day) | fields f", TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 17:30:00"));
  }

  @Test
  public void testDateTime() throws IOException {
    JSONObject result =
        executeQuery(String.format(
            "source=%s | eval f = DATETIME('2008-12-25 05:30:00+00:00', 'America/Los_Angeles') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-24 21:30:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = DATETIME('2008-12-25 05:30:00+00:00', '+01:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-25 06:30:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = DATETIME('2008-12-25 05:30:00-05:00', '+05:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-25 15:30:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = DATETIME('2004-02-28 23:00:00-10:00', '+10:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2004-02-29 19:00:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = DATETIME('2003-02-28 23:00:00-10:00', '+10:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2003-03-01 19:00:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = DATETIME('2008-12-25 05:30:00+00:00', '+14:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-25 19:30:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = DATETIME('2008-01-01 02:00:00+10:00', '-10:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2007-12-31 06:00:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = DATETIME('2008-01-01 02:00:00+10:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-01-01 02:00:00"));

    result =
        executeQuery(String.format(
            "source=%s | eval f = DATETIME('2008-01-01 02:00:00') | fields f",
            TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-01-01 02:00:00"));
  }

  @Test
  public void testDateSub() throws IOException {
    JSONObject result =
        executeQuery(String.format(
            "source=%s | eval f =  date_sub(timestamp('2020-09-16 17:30:00'), interval 1 day) | fields f", TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 17:30:00"));

    result = executeQuery(String.format(
            "source=%s | eval f =  date_sub(date('2020-09-16'), 1) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15"));

    result = executeQuery(String.format(
            "source=%s | eval f =  date_sub('2020-09-16', 1) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15"));

    result =
        executeQuery(String.format(
            "source=%s | eval f =  date_sub('2020-09-16 17:30:00', interval 1 day) | fields f", TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 17:30:00"));
  }

  @Test
  public void testDay() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  day(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));

    result = executeQuery(String.format(
            "source=%s | eval f =  day('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));
  }

  @Test
  public void testDayName() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  dayname(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows("Wednesday"));

    result = executeQuery(String.format(
            "source=%s | eval f =  dayname('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows("Wednesday"));
  }

  @Test
  public void testDayOfMonth() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  dayofmonth(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));

    result = executeQuery(String.format(
            "source=%s | eval f =  dayofmonth('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));
  }

  @Test
  public void testDayOfWeek() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  dayofweek(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(4));

    result = executeQuery(String.format(
            "source=%s | eval f =  dayofweek('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(4));
  }

  @Test
  public void testDayOfYear() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  dayofyear(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(260));

    result = executeQuery(String.format(
            "source=%s | eval f =  dayofyear('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(260));
  }

  @Test
  public void testFromDays() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  from_days(738049) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-16"));
  }

  @Test
  public void testHour() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  hour(timestamp('2020-09-16 17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));

    result = executeQuery(String.format(
            "source=%s | eval f =  hour(time('17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));

    result = executeQuery(String.format(
            "source=%s | eval f =  hour('2020-09-16 17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));

    result = executeQuery(String.format(
            "source=%s | eval f =  hour('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));
  }

  @Test
  public void testMicrosecond() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  microsecond(timestamp('2020-09-16 17:30:00.123456')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123456));

    // Explicit timestamp value with less than 6 microsecond digits
    result = executeQuery(String.format(
            "source=%s | eval f =  microsecond(timestamp('2020-09-16 17:30:00.1234')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123400));

    result = executeQuery(String.format(
            "source=%s | eval f =  microsecond(time('17:30:00.000010')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(10));

    // Explicit time value with less than 6 microsecond digits
    result = executeQuery(String.format(
        "source=%s | eval f =  microsecond(time('17:30:00.1234')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123400));

    result = executeQuery(String.format(
            "source=%s | eval f =  microsecond('2020-09-16 17:30:00.123456') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123456));

    // Implicit timestamp value with less than 6 microsecond digits
    result = executeQuery(String.format(
        "source=%s | eval f =  microsecond('2020-09-16 17:30:00.1234') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123400));

    result = executeQuery(String.format(
            "source=%s | eval f =  microsecond('17:30:00.000010') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(10));

    // Implicit time value with less than 6 microsecond digits
    result = executeQuery(String.format(
            "source=%s | eval f =  microsecond('17:30:00.1234') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123400));
  }

  @Test
  public void testMinute() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  minute(timestamp('2020-09-16 17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));

    result = executeQuery(String.format(
            "source=%s | eval f =  minute(time('17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));

    result = executeQuery(String.format(
            "source=%s | eval f =  minute('2020-09-16 17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));

    result = executeQuery(String.format(
            "source=%s | eval f =  minute('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));
  }

  @Test
  public void testMonth() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  month(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(9));

    result = executeQuery(String.format(
            "source=%s | eval f =  month('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(9));
  }

  @Test
  public void testMonthName() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  monthname(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows("September"));

    result = executeQuery(String.format(
            "source=%s | eval f =  monthname('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows("September"));
  }

  @Test
  public void testQuarter() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  quarter(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(3));

    result = executeQuery(String.format(
            "source=%s | eval f =  quarter('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(3));
  }

  @Test
  public void testSecond() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  second(timestamp('2020-09-16 17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));

    result = executeQuery(String.format(
            "source=%s | eval f =  second(time('17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));

    result = executeQuery(String.format(
            "source=%s | eval f =  second('2020-09-16 17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));

    result = executeQuery(String.format(
            "source=%s | eval f =  second('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));
  }

  @Test
  public void testSubDate() throws IOException {
    JSONObject result =
        executeQuery(String.format(
            "source=%s | eval f =  subdate(timestamp('2020-09-16 17:30:00'), interval 1 day) | fields f", TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 17:30:00"));

    result = executeQuery(String.format(
            "source=%s | eval f =  subdate(date('2020-09-16'), 1) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15"));

    result =
        executeQuery(String.format(
            "source=%s | eval f =  subdate('2020-09-16 17:30:00', interval 1 day) | fields f", TEST_INDEX_DATE));
    verifySchema(result,
        schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 17:30:00"));

    result = executeQuery(String.format(
            "source=%s | eval f =  subdate('2020-09-16', 1) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15"));
  }

  @Test
  public void testTimeToSec() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  time_to_sec(time('17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(63000));

    result = executeQuery(String.format(
            "source=%s | eval f =  time_to_sec('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(63000));
  }

  @Test
  public void testToDays() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  to_days(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(738049));

    result = executeQuery(String.format(
            "source=%s | eval f =  to_days('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(738049));
  }

  private void week(String date, int mode, int expectedResult) throws IOException {
    JSONObject result = executeQuery(StringUtils.format(
        "source=%s | eval f = week(date('%s'), %d) | fields f", TEST_INDEX_DATE, date, mode));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(expectedResult));
  }

  @Test
  public void testWeek() throws IOException {
    JSONObject result = executeQuery(String.format(
        "source=%s | eval f = week(date('2008-02-20')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(7));

    week("2008-02-20", 0, 7);
    week("2008-02-20", 1, 8);
    week("2008-12-31", 1, 53);
    week("2000-01-01", 0, 0);
    week("2000-01-01", 2, 52);
  }

  @Test
  public void testYear() throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  year(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(2020));

    result = executeQuery(String.format(
            "source=%s | eval f =  year('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(2020));
  }

  void verifyDateFormat(String date, String type, String format, String formatted) throws IOException {
    JSONObject result = executeQuery(String.format(
            "source=%s | eval f =  date_format(%s('%s'), '%s') | fields f",
            TEST_INDEX_DATE, type, date, format));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows(formatted));

    result = executeQuery(String.format(
            "source=%s | eval f =  date_format('%s', '%s') | fields f",
            TEST_INDEX_DATE, date, format));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows(formatted));
  }

  @Test
  public void testDateFormat() throws IOException {
    String timestamp = "1998-01-31 13:14:15.012345";
    String timestampFormat = "%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M "
        + "%m %p %r %S %s %T %% %P";
    String timestampFormatted = "Sat Jan 01 31st 31 31 12345 13 01 01 14 031 13 1 "
        + "January 01 PM 01:14:15 PM 15 15 13:14:15 % P";
    verifyDateFormat(timestamp, "timestamp", timestampFormat, timestampFormatted);

    String date = "1998-01-31";
    String dateFormat = "%U %u %V %v %W %w %X %x %Y %y";
    String dateFormatted = "4 4 4 4 Saturday 6 1998 1998 1998 98";
    verifyDateFormat(date, "date", dateFormat, dateFormatted);
  }


  @Test
  public void testDateFormatISO8601() throws IOException {
    String timestamp = "1998-01-31 13:14:15.012345";
    String timestampFormat = "%Y-%m-%dT%TZ";
    String timestampFormatted = "1998-01-31T13:14:15Z";
    verifyDateFormat(timestamp, "timestamp", timestampFormat, timestampFormatted);

    String date = "1998-01-31";
    String dateFormat = "%Y-%m-%dT%TZ";
    String dateFormatted = "1998-01-31T00:00:00Z";
    verifyDateFormat(date, "date", dateFormat, dateFormatted);
  }

  @Test
  public void testMakeTime() throws IOException {
    var result = executeQuery(String.format(
            "source=%s | eval f1 = MAKETIME(20, 30, 40), f2 = MAKETIME(20.2, 49.5, 42.100502) | fields f1, f2", TEST_INDEX_DATE));
    verifySchema(result, schema("f1", null, "time"), schema("f2", null, "time"));
    verifySome(result.getJSONArray("datarows"), rows("20:30:40", "20:50:42.100502"));
  }

  @Test
  public void testMakeDate() throws IOException {
    var result = executeQuery(String.format(
            "source=%s | eval f1 = MAKEDATE(1945, 5.9), f2 = MAKEDATE(1984, 1984) | fields f1, f2", TEST_INDEX_DATE));
    verifySchema(result, schema("f1", null, "date"), schema("f2", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("1945-01-06", "1989-06-06"));
  }

  private List<ImmutableMap<Object, Object>> nowLikeFunctionsData() {
    return List.of(
      ImmutableMap.builder()
              .put("name", "now")
              .put("hasFsp", false)
              .put("hasShortcut", false)
              .put("constValue", true)
              .put("referenceGetter", (Supplier<Temporal>) LocalDateTime::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse)
              .put("serializationPattern", "uuuu-MM-dd HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "current_timestamp")
              .put("hasFsp", false)
              .put("hasShortcut", true)
              .put("constValue", true)
              .put("referenceGetter", (Supplier<Temporal>) LocalDateTime::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse)
              .put("serializationPattern", "uuuu-MM-dd HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "localtimestamp")
              .put("hasFsp", false)
              .put("hasShortcut", true)
              .put("constValue", true)
              .put("referenceGetter", (Supplier<Temporal>) LocalDateTime::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse)
              .put("serializationPattern", "uuuu-MM-dd HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "localtime")
              .put("hasFsp", false)
              .put("hasShortcut", true)
              .put("constValue", true)
              .put("referenceGetter", (Supplier<Temporal>) LocalDateTime::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse)
              .put("serializationPattern", "uuuu-MM-dd HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "sysdate")
              .put("hasFsp", true)
              .put("hasShortcut", false)
              .put("constValue", false)
              .put("referenceGetter", (Supplier<Temporal>) LocalDateTime::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse)
              .put("serializationPattern", "uuuu-MM-dd HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "curtime")
              .put("hasFsp", false)
              .put("hasShortcut", false)
              .put("constValue", false)
              .put("referenceGetter", (Supplier<Temporal>) LocalTime::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalTime::parse)
              .put("serializationPattern", "HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "current_time")
              .put("hasFsp", false)
              .put("hasShortcut", true)
              .put("constValue", false)
              .put("referenceGetter", (Supplier<Temporal>) LocalTime::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalTime::parse)
              .put("serializationPattern", "HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "curdate")
              .put("hasFsp", false)
              .put("hasShortcut", false)
              .put("constValue", false)
              .put("referenceGetter", (Supplier<Temporal>) LocalDate::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDate::parse)
              .put("serializationPattern", "uuuu-MM-dd")
              .build(),
      ImmutableMap.builder()
              .put("name", "current_date")
              .put("hasFsp", false)
              .put("hasShortcut", true)
              .put("constValue", false)
              .put("referenceGetter", (Supplier<Temporal>) LocalDate::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDate::parse)
              .put("serializationPattern", "uuuu-MM-dd")
              .build()
    );
  }

  private long getDiff(Temporal sample, Temporal reference) {
    if (sample instanceof LocalDate) {
      return Period.between((LocalDate) sample, (LocalDate) reference).getDays();
    }
    return Duration.between(sample, reference).toSeconds();
  }

  @Test
  public void testNowLikeFunctions() throws IOException {
    // Integration test framework sets for OpenSearch instance a random timezone.
    // If server's TZ doesn't match localhost's TZ, time measurements for `now` would differ.
    // We should set localhost's TZ now and recover the value back in the end of the test.
    var testTz = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone(System.getProperty("user.timezone")));

    for (var funcData : nowLikeFunctionsData()) {
      String name = (String) funcData.get("name");
      Boolean hasFsp = (Boolean) funcData.get("hasFsp");
      Boolean hasShortcut = (Boolean) funcData.get("hasShortcut");
      Boolean constValue = (Boolean) funcData.get("constValue");
      Supplier<Temporal> referenceGetter = (Supplier<Temporal>) funcData.get("referenceGetter");
      BiFunction<CharSequence, DateTimeFormatter, Temporal> parser =
              (BiFunction<CharSequence, DateTimeFormatter, Temporal>) funcData.get("parser");
      String serializationPatternStr = (String) funcData.get("serializationPattern");

      var serializationPattern = new DateTimeFormatterBuilder()
              .appendPattern(serializationPatternStr)
              .optionalStart()
              .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
              .toFormatter();

      Temporal reference = referenceGetter.get();
      double delta = 2d; // acceptable time diff, secs
      if (reference instanceof LocalDate)
        delta = 1d; // Max date delta could be 1 if test runs on the very edge of two days
                    // We ignore probability of a test run on edge of month or year to simplify the checks

      var calls = new ArrayList<String>() {{
        add(name + "()");
      }};
      if (hasShortcut)
        calls.add(name);
      if (hasFsp)
        calls.add(name + "(0)");

      // Column order is: func(), func, func(0)
      //                   shortcut ^    fsp ^
      // Query looks like:
      //    source=people2 | eval `now()`=now() | fields `now()`;
      JSONObject result = executeQuery("source=" + TEST_INDEX_PEOPLE2
          + " | eval " + calls.stream().map(c -> String.format("`%s`=%s", c, c)).collect(Collectors.joining(","))
          + " | fields " + calls.stream().map(c -> String.format("`%s`", c)).collect(Collectors.joining(",")));

      var rows = result.getJSONArray("datarows");
      JSONArray firstRow = rows.getJSONArray(0);
      for (int i = 0; i < rows.length(); i++) {
        var row = rows.getJSONArray(i);
        if (constValue)
          assertTrue(firstRow.similar(row));

        int column = 0;
        assertEquals(0,
            getDiff(reference, parser.apply(row.getString(column++), serializationPattern)), delta);

        if (hasShortcut) {
          assertEquals(0,
              getDiff(reference, parser.apply(row.getString(column++), serializationPattern)), delta);
        }
        if (hasFsp) {
          assertEquals(0,
              getDiff(reference, parser.apply(row.getString(column), serializationPattern)), delta);
        }
      }
    }
    TimeZone.setDefault(testTz);
  }

  @Test
  public void testFromUnixTime() throws IOException {
    var result = executeQuery(String.format(
        "source=%s | eval f1 = FROM_UNIXTIME(200300400), f2 = FROM_UNIXTIME(12224.12), "
        + "f3 = FROM_UNIXTIME(1662601316, '%%T') | fields f1, f2, f3", TEST_INDEX_DATE));
    verifySchema(result,
        schema("f1", null, "datetime"),
        schema("f2", null, "datetime"),
        schema("f3", null, "string"));
    verifySome(result.getJSONArray("datarows"),
        rows("1976-05-07 07:00:00", "1970-01-01 03:23:44.12", "01:41:56"));
  }

  @Test
  public void testUnixTimeStamp() throws IOException {
    var result = executeQuery(String.format(
        "source=%s | eval f1 = UNIX_TIMESTAMP(MAKEDATE(1984, 1984)), "
        + "f2 = UNIX_TIMESTAMP(TIMESTAMP('2003-12-31 12:00:00')), "
        + "f3 = UNIX_TIMESTAMP(20771122143845) | fields f1, f2, f3", TEST_INDEX_DATE));
    verifySchema(result,
        schema("f1", null, "double"),
        schema("f2", null, "double"),
        schema("f3", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(613094400d, 1072872000d, 3404817525d));
  }
}
