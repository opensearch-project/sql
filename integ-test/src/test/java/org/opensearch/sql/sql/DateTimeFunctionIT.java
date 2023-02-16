/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_CALCS;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PEOPLE2;
import static org.opensearch.sql.legacy.plugin.RestSqlAction.QUERY_API_ENDPOINT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;
import static org.opensearch.sql.util.TestUtils.getResponseBody;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.time.temporal.Temporal;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.legacy.SQLIntegTestCase;

public class DateTimeFunctionIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
    loadIndex(Index.CALCS);
    loadIndex(Index.PEOPLE2);
    loadIndex(Index.CALCS);
  }

  // Integration test framework sets for OpenSearch instance a random timezone.
  // If server's TZ doesn't match localhost's TZ, time measurements for some tests would differ.
  // We should set localhost's TZ now and recover the value back in the end of the test.
  private final TimeZone testTz = TimeZone.getDefault();
  private final TimeZone systemTz = TimeZone.getTimeZone(System.getProperty("user.timezone"));

  @Before
  public void setTimeZone() {
    TimeZone.setDefault(systemTz);
  }

  @After
  public void resetTimeZone() {
    TimeZone.setDefault(testTz);
  }

  @Test
  public void testDateInGroupBy() throws IOException{
    JSONObject result =
        executeQuery(String.format("SELECT DATE(birthdate) FROM %s GROUP BY DATE(birthdate)",TEST_INDEX_BANK) );
    verifySchema(result,
        schema("DATE(birthdate)", null, "date"));
    verifyDataRows(result,
        rows("2017-10-23"),
        rows("2017-11-20"),
        rows("2018-06-23"),
        rows("2018-11-13"),
        rows("2018-06-27"),
        rows("2018-08-19"),
        rows("2018-08-11"));
  }

  @Test
  public void testDateWithHavingClauseOnly() throws IOException {
    JSONObject result =
        executeQuery(String.format("SELECT (TO_DAYS(DATE('2050-01-01')) - 693961) FROM %s HAVING (COUNT(1) > 0)",TEST_INDEX_BANK) );
    verifySchema(result,
        schema("(TO_DAYS(DATE('2050-01-01')) - 693961)", null, "long"));
    verifyDataRows(result, rows(54787));
  }

  @Test
  public void testAddDateWithDays() throws IOException {
    var result = executeQuery("select adddate(date('2020-09-16'), 1)");
    verifySchema(result, schema("adddate(date('2020-09-16'), 1)", null, "date"));
    verifyDataRows(result, rows("2020-09-17"));

    result = executeQuery("select adddate(timestamp('2020-09-16 17:30:00'), 1)");
    verifySchema(result, schema("adddate(timestamp('2020-09-16 17:30:00'), 1)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-17 17:30:00"));

    result = executeQuery("select adddate(DATETIME('2020-09-16 07:40:00'), 1)");
    verifySchema(result, schema("adddate(DATETIME('2020-09-16 07:40:00'), 1)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-17 07:40:00"));

    result = executeQuery("select adddate(TIME('07:40:00'), 0)");
    verifySchema(result, schema("adddate(TIME('07:40:00'), 0)", null, "datetime"));
    verifyDataRows(result, rows(LocalDate.now() + " 07:40:00"));
  }

  @Test
  public void testAddDateWithInterval() throws IOException {
    JSONObject result =
        executeQuery("select adddate(timestamp('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("adddate(timestamp('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-17 17:30:00"));

    result = executeQuery("select adddate(DATETIME('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("adddate(DATETIME('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-17 17:30:00"));

    result = executeQuery("select adddate(date('2020-09-16'), interval 1 day)");
    verifySchema(result,
        schema("adddate(date('2020-09-16'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-17 00:00:00"));

    result = executeQuery("select adddate(date('2020-09-16'), interval 1 hour)");
    verifySchema(result,
        schema("adddate(date('2020-09-16'), interval 1 hour)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-16 01:00:00"));

    result = executeQuery("select adddate(TIME('07:40:00'), interval 1 day)");
    verifySchema(result,
        schema("adddate(TIME('07:40:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result,
        rows(LocalDate.now().plusDays(1).atTime(LocalTime.of(7, 40)).atZone(systemTz.toZoneId())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result = executeQuery("select adddate(TIME('07:40:00'), interval 1 hour)");
    verifySchema(result,
        schema("adddate(TIME('07:40:00'), interval 1 hour)", null, "datetime"));
    verifyDataRows(result,
        rows(LocalDate.now().atTime(LocalTime.of(8, 40)).atZone(systemTz.toZoneId())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
  }

  @Test
  public void testDateAdd() throws IOException {
    JSONObject result =
        executeQuery("select date_add(timestamp('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("date_add(timestamp('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-17 17:30:00"));

    result = executeQuery("select date_add(DATETIME('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("date_add(DATETIME('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-17 17:30:00"));

    result = executeQuery("select date_add(date('2020-09-16'), interval 1 day)");
    verifySchema(result,
        schema("date_add(date('2020-09-16'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-17 00:00:00"));

    result = executeQuery("select date_add(date('2020-09-16'), interval 1 hour)");
    verifySchema(result,
        schema("date_add(date('2020-09-16'), interval 1 hour)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-16 01:00:00"));

    result = executeQuery("select date_add(TIME('07:40:00'), interval 1 day)");
    verifySchema(result,
        schema("date_add(TIME('07:40:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result,
        rows(LocalDate.now().plusDays(1).atTime(LocalTime.of(7, 40)).atZone(systemTz.toZoneId())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result = executeQuery("select date_add(TIME('07:40:00'), interval 1 hour)");
    verifySchema(result,
        schema("date_add(TIME('07:40:00'), interval 1 hour)", null, "datetime"));
    verifyDataRows(result,
        rows(LocalDate.now().atTime(LocalTime.of(8, 40)).atZone(systemTz.toZoneId())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result = executeQuery(String.format("SELECT DATE_ADD(birthdate, INTERVAL 1 YEAR) FROM %s",
        TEST_INDEX_BANK));

    verifySchema(result,
        schema("DATE_ADD(birthdate, INTERVAL 1 YEAR)", null, "datetime"));
    verifyDataRows(result,
        rows("2018-10-23 00:00:00"),
        rows("2018-11-20 00:00:00"),
        rows("2019-06-23 00:00:00"),
        rows("2019-11-13 23:33:20"),
        rows("2019-06-27 00:00:00"),
        rows("2019-08-19 00:00:00"),
        rows("2019-08-11 00:00:00"));
  }

  @Test
  public void testDateSub() throws IOException {
    JSONObject result =
        executeQuery("select date_sub(timestamp('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("date_sub(timestamp('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 17:30:00"));

    result = executeQuery("select date_sub(DATETIME('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("date_sub(DATETIME('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 17:30:00"));

    result = executeQuery("select date_sub(date('2020-09-16'), interval 1 day)");
    verifySchema(result,
        schema("date_sub(date('2020-09-16'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 00:00:00"));

    result = executeQuery("select date_sub(date('2020-09-16'), interval 1 hour)");
    verifySchema(result,
        schema("date_sub(date('2020-09-16'), interval 1 hour)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 23:00:00"));

    result = executeQuery("select date_sub(TIME('07:40:00'), interval 1 day)");
    verifySchema(result,
        schema("date_sub(TIME('07:40:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result,
        rows(LocalDate.now().plusDays(-1).atTime(LocalTime.of(7, 40)).atZone(systemTz.toZoneId())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result = executeQuery("select date_sub(TIME('07:40:00'), interval 1 hour)");
    verifySchema(result,
        schema("date_sub(TIME('07:40:00'), interval 1 hour)", null, "datetime"));
    verifyDataRows(result,
        rows(LocalDate.now().atTime(LocalTime.of(6, 40)).atZone(systemTz.toZoneId())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
  }

  @Test
  public void testDay() throws IOException {
    JSONObject result = executeQuery("select day(date('2020-09-16'))");
    verifySchema(result, schema("day(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(16));

    result = executeQuery("select day('2020-09-16')");
    verifySchema(result, schema("day('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(16));
  }

  @Test
  public void testDayName() throws IOException {
    JSONObject result = executeQuery("select dayname(date('2020-09-16'))");
    verifySchema(result, schema("dayname(date('2020-09-16'))", null, "keyword"));
    verifyDataRows(result, rows("Wednesday"));

    result = executeQuery("select dayname('2020-09-16')");
    verifySchema(result, schema("dayname('2020-09-16')", null, "keyword"));
    verifyDataRows(result, rows("Wednesday"));
  }

  @Test
  public void testDayOfMonth() throws IOException {
    JSONObject result = executeQuery("select dayofmonth(date('2020-09-16'))");
    verifySchema(result, schema("dayofmonth(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(16));

    result = executeQuery("select dayofmonth('2020-09-16')");
    verifySchema(result, schema("dayofmonth('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(16));
  }

  @Test
  public void testDayOfMonthWithUnderscores() throws IOException {
    JSONObject result = executeQuery("select day_of_month(date('2020-09-16'))");
    verifySchema(result, schema("day_of_month(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(16));

    result = executeQuery("select day_of_month('2020-09-16')");
    verifySchema(result, schema("day_of_month('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(16));
  }

  @Test
  public void testDayOfMonthAliasesReturnTheSameResults() throws IOException {
    JSONObject result1 = executeQuery("SELECT dayofmonth(date('2022-11-22'))");
    JSONObject result2 = executeQuery("SELECT day_of_month(date('2022-11-22'))");
    verifyDataRows(result1, rows(22));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofmonth(CAST(date0 AS date)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_month(CAST(date0 AS date)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofmonth(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_month(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofmonth(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_month(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofmonth(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_month(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));
  }
  @Test
  public void testDayOfWeek() throws IOException {
    JSONObject result = executeQuery("select dayofweek(date('2020-09-16'))");
    verifySchema(result, schema("dayofweek(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(4));

    result = executeQuery("select dayofweek('2020-09-16')");
    verifySchema(result, schema("dayofweek('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(4));
  }

  @Test
  public void testDayOfWeekWithUnderscores() throws IOException {
    JSONObject result = executeQuery("select day_of_week(date('2020-09-16'))");
    verifySchema(result, schema("day_of_week(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(4));

    result = executeQuery("select day_of_week('2020-09-16')");
    verifySchema(result, schema("day_of_week('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(4));
  }

  @Test
  public void testDayOfWeekAliasesReturnTheSameResults() throws IOException {
    JSONObject result1 = executeQuery("SELECT dayofweek(date('2022-11-22'))");
    JSONObject result2 = executeQuery("SELECT day_of_week(date('2022-11-22'))");
    verifyDataRows(result1, rows(3));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofweek(CAST(date0 AS date)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_week(CAST(date0 AS date)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofweek(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_week(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofweek(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_week(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofweek(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_week(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));
  }

  @Test
  public void testDayOfYear() throws IOException {
    JSONObject result = executeQuery("select dayofyear(date('2020-09-16'))");
    verifySchema(result, schema("dayofyear(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(260));

    result = executeQuery("select dayofyear('2020-09-16')");
    verifySchema(result, schema("dayofyear('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(260));
  }

  @Test
  public void testDayOfYearWithUnderscores() throws IOException {
    JSONObject result = executeQuery("select day_of_year(date('2020-09-16'))");
    verifySchema(result, schema("day_of_year(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(260));

    result = executeQuery("select day_of_year(datetime('2020-09-16 00:00:00'))");
    verifySchema(result, schema("day_of_year(datetime('2020-09-16 00:00:00'))", null, "integer"));
    verifyDataRows(result, rows(260));

    result = executeQuery("select day_of_year(timestamp('2020-09-16 00:00:00'))");
    verifySchema(result, schema("day_of_year(timestamp('2020-09-16 00:00:00'))", null, "integer"));
    verifyDataRows(result, rows(260));

    result = executeQuery("select day_of_year('2020-09-16')");
    verifySchema(result, schema("day_of_year('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(260));
  }

  @Test
  public void testDayOfYearAlternateSyntaxesReturnTheSameResults() throws IOException {
    JSONObject result1 = executeQuery("SELECT dayofyear(date('2022-11-22'))");
    JSONObject result2 = executeQuery("SELECT day_of_year(date('2022-11-22'))");
    verifyDataRows(result1, rows(326));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofyear(CAST(date0 AS date)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_year(CAST(date0 AS date)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofyear(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_year(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofyear(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_year(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT dayofyear(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT day_of_year(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));
  }
  @Test
  public void testFromDays() throws IOException {
    JSONObject result = executeQuery("select from_days(738049)");
    verifySchema(result, schema("from_days(738049)", null, "date"));
    verifyDataRows(result, rows("2020-09-16"));
  }

  @Test
  public void testHour() throws IOException {
    JSONObject result = executeQuery("select hour(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema("hour(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(17));

    result = executeQuery("select hour(time('17:30:00'))");
    verifySchema(result, schema("hour(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(17));

    result = executeQuery("select hour('2020-09-16 17:30:00')");
    verifySchema(result, schema("hour('2020-09-16 17:30:00')", null, "integer"));
    verifyDataRows(result, rows(17));

    result = executeQuery("select hour('17:30:00')");
    verifySchema(result, schema("hour('17:30:00')", null, "integer"));
    verifyDataRows(result, rows(17));
  }

  @Test
  public void testHourOfDayWithUnderscores() throws IOException {
    JSONObject result = executeQuery("select hour_of_day(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema(
        "hour_of_day(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(17));

    result = executeQuery("select hour_of_day(datetime('2020-09-16 17:30:00'))");
    verifySchema(result, schema(
        "hour_of_day(datetime('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(17));

    result = executeQuery("select hour_of_day(time('17:30:00'))");
    verifySchema(result, schema("hour_of_day(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(17));

    result = executeQuery("select hour_of_day('2020-09-16 17:30:00')");
    verifySchema(result, schema("hour_of_day('2020-09-16 17:30:00')", null, "integer"));
    verifyDataRows(result, rows(17));

    result = executeQuery("select hour_of_day('17:30:00')");
    verifySchema(result, schema("hour_of_day('17:30:00')", null, "integer"));
    verifyDataRows(result, rows(17));
  }

  @Test
  public void testHourFunctionAliasesReturnTheSameResults() throws IOException {
    JSONObject result1 = executeQuery("SELECT hour('11:30:00')");
    JSONObject result2 = executeQuery("SELECT hour_of_day('11:30:00')");
    verifyDataRows(result1, rows(11));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT hour(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT hour_of_day(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT hour(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT hour_of_day(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT hour(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT hour_of_day(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));
  }

  @Test
  public void testLastDay() throws IOException {
    JSONObject result = executeQuery(
        String.format("SELECT last_day(cast(date0 as date)) FROM %s LIMIT 3",
            TEST_INDEX_CALCS));
    verifyDataRows(result,
        rows("2004-04-30"),
        rows("1972-07-31"),
        rows("1975-11-30"));

    result = executeQuery(
        String.format("SELECT last_day(datetime(cast(date0 AS string))) FROM %s LIMIT 3",
            TEST_INDEX_CALCS));
    verifyDataRows(result,
        rows("2004-04-30"),
        rows("1972-07-31"),
        rows("1975-11-30"));

    result = executeQuery(
        String.format("SELECT last_day(cast(date0 AS timestamp)) FROM %s LIMIT 3",
            TEST_INDEX_CALCS));
    verifyDataRows(result,
        rows("2004-04-30"),
        rows("1972-07-31"),
        rows("1975-11-30"));
  }

  @Test
  public void testMicrosecond() throws IOException {
    JSONObject result = executeQuery("select microsecond(timestamp('2020-09-16 17:30:00.123456'))");
    verifySchema(result,
        schema("microsecond(timestamp('2020-09-16 17:30:00.123456'))", null, "integer"));
    verifyDataRows(result, rows(123456));

    // Explicit timestamp value with less than 6 microsecond digits
    result = executeQuery("select microsecond(timestamp('2020-09-16 17:30:00.1234'))");
    verifySchema(result,
        schema("microsecond(timestamp('2020-09-16 17:30:00.1234'))", null, "integer"));
    verifyDataRows(result, rows(123400));

    result = executeQuery("select microsecond(time('17:30:00.000010'))");
    verifySchema(result, schema("microsecond(time('17:30:00.000010'))", null, "integer"));
    verifyDataRows(result, rows(10));

    // Explicit time value with less than 6 microsecond digits
    result = executeQuery("select microsecond(time('17:30:00.1234'))");
    verifySchema(result, schema("microsecond(time('17:30:00.1234'))", null, "integer"));
    verifyDataRows(result, rows(123400));

    result = executeQuery("select microsecond('2020-09-16 17:30:00.123456')");
    verifySchema(result, schema("microsecond('2020-09-16 17:30:00.123456')", null, "integer"));
    verifyDataRows(result, rows(123456));

    // Implicit timestamp value with less than 6 microsecond digits
    result = executeQuery("select microsecond('2020-09-16 17:30:00.1234')");
    verifySchema(result, schema("microsecond('2020-09-16 17:30:00.1234')", null, "integer"));
    verifyDataRows(result, rows(123400));

    result = executeQuery("select microsecond('17:30:00.000010')");
    verifySchema(result, schema("microsecond('17:30:00.000010')", null, "integer"));
    verifyDataRows(result, rows(10));

    // Implicit time value with less than 6 microsecond digits
    result = executeQuery("select microsecond('17:30:00.1234')");
    verifySchema(result, schema("microsecond('17:30:00.1234')", null, "integer"));
    verifyDataRows(result, rows(123400));
  }

  @Test
  public void testMinute() throws IOException {
    JSONObject result = executeQuery("select minute(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema("minute(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(30));

    result = executeQuery("select minute(time('17:30:00'))");
    verifySchema(result, schema("minute(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(30));

    result = executeQuery("select minute('2020-09-16 17:30:00')");
    verifySchema(result, schema("minute('2020-09-16 17:30:00')", null, "integer"));
    verifyDataRows(result, rows(30));

    result = executeQuery("select minute('17:30:00')");
    verifySchema(result, schema("minute('17:30:00')", null, "integer"));
    verifyDataRows(result, rows(30));
  }


  @Test
  public void testMinuteOfDay() throws IOException {
    JSONObject result = executeQuery("select minute_of_day(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema("minute_of_day(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(1050));

    result = executeQuery("select minute_of_day(datetime('2020-09-16 17:30:00'))");
    verifySchema(result, schema("minute_of_day(datetime('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(1050));

    result = executeQuery("select minute_of_day(time('17:30:00'))");
    verifySchema(result, schema("minute_of_day(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(1050));

    result = executeQuery("select minute_of_day('2020-09-16 17:30:00')");
    verifySchema(result, schema("minute_of_day('2020-09-16 17:30:00')", null, "integer"));
    verifyDataRows(result, rows(1050));

    result = executeQuery("select minute_of_day('17:30:00')");
    verifySchema(result, schema("minute_of_day('17:30:00')", null, "integer"));
    verifyDataRows(result, rows(1050));
  }

  @Test
  public void testMinuteOfHour() throws IOException {
    JSONObject result = executeQuery("select minute_of_hour(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema(
        "minute_of_hour(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(30));

    result = executeQuery("select minute_of_hour(time('17:30:00'))");
    verifySchema(result, schema("minute_of_hour(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(30));

    result = executeQuery("select minute_of_hour('2020-09-16 17:30:00')");
    verifySchema(result, schema("minute_of_hour('2020-09-16 17:30:00')", null, "integer"));
    verifyDataRows(result, rows(30));

    result = executeQuery("select minute_of_hour('17:30:00')");
    verifySchema(result, schema("minute_of_hour('17:30:00')", null, "integer"));
    verifyDataRows(result, rows(30));
  }

  @Test
  public void testMinuteFunctionAliasesReturnTheSameResults() throws IOException {
    JSONObject result1 = executeQuery("SELECT minute('11:30:00')");
    JSONObject result2 = executeQuery("SELECT minute_of_hour('11:30:00')");
    verifyDataRows(result1, rows(30));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT minute(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT minute_of_hour(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT minute(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT minute_of_hour(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT minute(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT minute_of_hour(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));
  }

  @Test
  public void testMonth() throws IOException {
    JSONObject result = executeQuery("select month(date('2020-09-16'))");
    verifySchema(result, schema("month(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(9));

    result = executeQuery("select month('2020-09-16')");
    verifySchema(result, schema("month('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(9));
  }

  @Test
  public void testMonthOfYearTypes() throws IOException {
    JSONObject result = executeQuery("select month_of_year(date('2020-09-16'))");
    verifySchema(result, schema("month_of_year(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(9));

    result = executeQuery("select month_of_year(datetime('2020-09-16 00:00:00'))");
    verifySchema(result, schema("month_of_year(datetime('2020-09-16 00:00:00'))", null, "integer"));
    verifyDataRows(result, rows(9));

    result = executeQuery("select month_of_year(timestamp('2020-09-16 00:00:00'))");
    verifySchema(result, schema("month_of_year(timestamp('2020-09-16 00:00:00'))", null, "integer"));
    verifyDataRows(result, rows(9));

    result = executeQuery("select month_of_year('2020-09-16')");
    verifySchema(result, schema("month_of_year('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(9));
  }

  @Test
  public void testMonthAlternateSyntaxesReturnTheSameResults() throws IOException {
    JSONObject result1 = executeQuery("SELECT month(date('2022-11-22'))");
    JSONObject result2 = executeQuery("SELECT month_of_year(date('2022-11-22'))");
    verifyDataRows(result1, rows(11));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT month(CAST(date0 AS date)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT month_of_year(CAST(date0 AS date)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT month(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT month_of_year(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT month(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT month_of_year(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT month(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT month_of_year(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));
  }

  @Test
  public void testMonthName() throws IOException {
    JSONObject result = executeQuery("select monthname(date('2020-09-16'))");
    verifySchema(result, schema("monthname(date('2020-09-16'))", null, "keyword"));
    verifyDataRows(result, rows("September"));

    result = executeQuery("select monthname('2020-09-16')");
    verifySchema(result, schema("monthname('2020-09-16')", null, "keyword"));
    verifyDataRows(result, rows("September"));
  }

  @Test
  public void testQuarter() throws IOException {
    JSONObject result = executeQuery("select quarter(date('2020-09-16'))");
    verifySchema(result, schema("quarter(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(3));

    result = executeQuery("select quarter('2020-09-16')");
    verifySchema(result, schema("quarter('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(3));
  }

  @Test
  public void testSecond() throws IOException {
    JSONObject result = executeQuery("select second(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema("second(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(0));

    result = executeQuery("select second(time('17:30:00'))");
    verifySchema(result, schema("second(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(0));

    result = executeQuery("select second('2020-09-16 17:30:00')");
    verifySchema(result, schema("second('2020-09-16 17:30:00')", null, "integer"));
    verifyDataRows(result, rows(0));

    result = executeQuery("select second('17:30:00')");
    verifySchema(result, schema("second('17:30:00')", null, "integer"));
    verifyDataRows(result, rows(0));
  }

  public void testSecondOfMinute() throws IOException {
    JSONObject result = executeQuery("select second_of_minute(timestamp('2020-09-16 17:30:00'))");
    verifySchema(result, schema("second_of_minute(timestamp('2020-09-16 17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(0));

    result = executeQuery("select second_of_minute(time('17:30:00'))");
    verifySchema(result, schema("second_of_minute(time('17:30:00'))", null, "integer"));
    verifyDataRows(result, rows(0));

    result = executeQuery("select second_of_minute('2020-09-16 17:30:00')");
    verifySchema(result, schema("second_of_minute('2020-09-16 17:30:00')", null, "integer"));
    verifyDataRows(result, rows(0));

    result = executeQuery("select second_of_minute('17:30:00')");
    verifySchema(result, schema("second_of_minute('17:30:00')", null, "integer"));
    verifyDataRows(result, rows(0));
  }

  @Test
  public void testSecondFunctionAliasesReturnTheSameResults() throws IOException {
    JSONObject result1 = executeQuery("SELECT second('2022-11-22 12:23:34')");
    JSONObject result2 = executeQuery("SELECT second_of_minute('2022-11-22 12:23:34')");
    verifyDataRows(result1, rows(34));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT second(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT second_of_minute(datetime(CAST(time0 AS STRING))) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT second(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT second_of_minute(CAST(time0 AS STRING)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));

    result1 = executeQuery(String.format(
        "SELECT second(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result2 = executeQuery(String.format(
        "SELECT second_of_minute(CAST(datetime0 AS timestamp)) FROM %s", TEST_INDEX_CALCS));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));
  }

  @Test
  public void testSubDateWithDays() throws IOException {
    var result =
        executeQuery("select subdate(date('2020-09-16'), 1)");
    verifySchema(result,
        schema("subdate(date('2020-09-16'), 1)", null, "date"));
    verifyDataRows(result, rows("2020-09-15"));

    result =
        executeQuery("select subdate(timestamp('2020-09-16 17:30:00'), 1)");
    verifySchema(result,
        schema("subdate(timestamp('2020-09-16 17:30:00'), 1)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 17:30:00"));

    result = executeQuery("select subdate(DATETIME('2020-09-16 07:40:00'), 1)");
    verifySchema(result, schema("subdate(DATETIME('2020-09-16 07:40:00'), 1)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 07:40:00"));

    result = executeQuery("select subdate(TIME('07:40:00'), 0)");
    verifySchema(result, schema("subdate(TIME('07:40:00'), 0)", null, "datetime"));
    verifyDataRows(result, rows(LocalDate.now() + " 07:40:00"));
  }

  @Test
  public void testSubDateWithInterval() throws IOException {
    JSONObject result =
        executeQuery("select subdate(timestamp('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("subdate(timestamp('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 17:30:00"));

    result = executeQuery("select subdate(DATETIME('2020-09-16 17:30:00'), interval 1 day)");
    verifySchema(result,
        schema("subdate(DATETIME('2020-09-16 17:30:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 17:30:00"));

    result = executeQuery("select subdate(date('2020-09-16'), interval 1 day)");
    verifySchema(result,
        schema("subdate(date('2020-09-16'), interval 1 day)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 00:00:00"));

    result = executeQuery("select subdate(date('2020-09-16'), interval 1 hour)");
    verifySchema(result,
        schema("subdate(date('2020-09-16'), interval 1 hour)", null, "datetime"));
    verifyDataRows(result, rows("2020-09-15 23:00:00"));

    result = executeQuery("select subdate(TIME('07:40:00'), interval 1 day)");
    verifySchema(result,
        schema("subdate(TIME('07:40:00'), interval 1 day)", null, "datetime"));
    verifyDataRows(result,
        rows(LocalDate.now().plusDays(-1).atTime(LocalTime.of(7, 40)).atZone(systemTz.toZoneId())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result = executeQuery("select subdate(TIME('07:40:00'), interval 1 hour)");
    verifySchema(result,
        schema("subdate(TIME('07:40:00'), interval 1 hour)", null, "datetime"));
    verifyDataRows(result,
        rows(LocalDate.now().atTime(LocalTime.of(6, 40)).atZone(systemTz.toZoneId())
            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
  }

  @Test
  public void testTimeToSec() throws IOException {
    JSONObject result = executeQuery("select time_to_sec(time('17:30:00'))");
    verifySchema(result, schema("time_to_sec(time('17:30:00'))", null, "long"));
    verifyDataRows(result, rows(63000));

    result = executeQuery("select time_to_sec('17:30:00')");
    verifySchema(result, schema("time_to_sec('17:30:00')", null, "long"));
    verifyDataRows(result, rows(63000));
  }

  @Test
  public void testToDays() throws IOException {
    JSONObject result = executeQuery("select to_days(date('2020-09-16'))");
    verifySchema(result, schema("to_days(date('2020-09-16'))", null, "long"));
    verifyDataRows(result, rows(738049));

    result = executeQuery("select to_days('2020-09-16')");
    verifySchema(result, schema("to_days('2020-09-16')", null, "long"));
    verifyDataRows(result, rows(738049));
  }

  @Test
  public void testYear() throws IOException {
    JSONObject result = executeQuery("select year(date('2020-09-16'))");
    verifySchema(result, schema("year(date('2020-09-16'))", null, "integer"));
    verifyDataRows(result, rows(2020));

    result = executeQuery("select year('2020-09-16')");
    verifySchema(result, schema("year('2020-09-16')", null, "integer"));
    verifyDataRows(result, rows(2020));
  }

  private void week(String date, int mode, int expectedResult, String functionName) throws IOException {
    JSONObject result = executeQuery(StringUtils.format("select %s(date('%s'), %d)", functionName, date,
        mode));
    verifySchema(result,
        schema(StringUtils.format("%s(date('%s'), %d)", functionName, date, mode), null, "integer"));
    verifyDataRows(result, rows(expectedResult));
  }

  @Test
  public void testWeek() throws IOException {
    JSONObject result = executeQuery("select week(date('2008-02-20'))");
    verifySchema(result, schema("week(date('2008-02-20'))", null, "integer"));
    verifyDataRows(result, rows(7));

    week("2008-02-20", 0, 7, "week");
    week("2008-02-20", 1, 8, "week");
    week("2008-12-31", 1, 53, "week");
    week("2000-01-01", 0, 0, "week");
    week("2000-01-01", 2, 52, "week");
  }

  @Test
  public void testWeekOfYearUnderscores() throws IOException {
    JSONObject result = executeQuery("select week_of_year(date('2008-02-20'))");
    verifySchema(result, schema("week_of_year(date('2008-02-20'))", null, "integer"));
    verifyDataRows(result, rows(7));

    week("2008-02-20", 0, 7, "week_of_year");
    week("2008-02-20", 1, 8, "week_of_year");
    week("2008-12-31", 1, 53, "week_of_year");
    week("2000-01-01", 0, 0, "week_of_year");
    week("2000-01-01", 2, 52, "week_of_year");
  }

  @Test
  public void testWeekOfYear() throws IOException {
    JSONObject result = executeQuery("select weekofyear(date('2008-02-20'))");
    verifySchema(result, schema("weekofyear(date('2008-02-20'))", null, "integer"));
    verifyDataRows(result, rows(7));

    week("2008-02-20", 0, 7, "weekofyear");
    week("2008-02-20", 1, 8, "weekofyear");
    week("2008-12-31", 1, 53, "weekofyear");
    week("2000-01-01", 0, 0, "weekofyear");
    week("2000-01-01", 2, 52, "week_of_year");
  }

  private void compareWeekResults(String arg, String table) throws IOException {
    JSONObject result1 = executeQuery(String.format(
        "SELECT week(%s) FROM %s", arg, table));
    JSONObject result2 = executeQuery(String.format(
        "SELECT week_of_year(%s) FROM %s", arg, table));
    JSONObject result3 = executeQuery(String.format(
        "SELECT weekofyear(%s) FROM %s", arg, table));

    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));
    result1.getJSONArray("datarows").similar(result3.getJSONArray("datarows"));
  }

  @Test
  public void testWeekAlternateSyntaxesReturnTheSameResults() throws IOException {
    JSONObject result1 = executeQuery("SELECT week(date('2022-11-22'))");
    JSONObject result2 = executeQuery("SELECT week_of_year(date('2022-11-22'))");
    JSONObject result3 = executeQuery("SELECT weekofyear(date('2022-11-22'))");
    verifyDataRows(result1, rows(47));
    result1.getJSONArray("datarows").similar(result2.getJSONArray("datarows"));
    result1.getJSONArray("datarows").similar(result3.getJSONArray("datarows"));

    compareWeekResults("CAST(date0 AS date)", TEST_INDEX_CALCS);
    compareWeekResults("datetime(CAST(time0 AS STRING))", TEST_INDEX_CALCS);
    compareWeekResults("CAST(time0 AS STRING)", TEST_INDEX_CALCS);
    compareWeekResults("CAST(datetime0 AS timestamp)", TEST_INDEX_CALCS);
  }

  void verifyDateFormat(String date, String type, String format, String formatted) throws IOException {
    String query = String.format("date_format(%s('%s'), '%s')", type, date, format);
    JSONObject result = executeQuery("select " + query);
    verifySchema(result, schema(query, null, "keyword"));
    verifyDataRows(result, rows(formatted));

    query = String.format("date_format('%s', '%s')", date, format);
    result = executeQuery("select " + query);
    verifySchema(result, schema(query, null, "keyword"));
    verifyDataRows(result, rows(formatted));
  }

  @Test
  public void testDateFormat() throws IOException {
    String timestamp = "1998-01-31 13:14:15.012345";
    String timestampFormat = "%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M "
        + "%m %p %r %S %s %T %% %P";
    String timestampFormatted = "Sat Jan 01 31st 31 31 012345 13 01 01 14 031 13 1 "
        + "January 01 PM 01:14:15 PM 15 15 13:14:15 % P";
    verifyDateFormat(timestamp, "timestamp", timestampFormat, timestampFormatted);

    String date = "1998-01-31";
    String dateFormat = "%U %u %V %v %W %w %X %x %Y %y";
    String dateFormatted = "4 4 4 4 Saturday 6 1998 1998 1998 98";
    verifyDateFormat(date, "date", dateFormat, dateFormatted);
  }

  @Test
  public void testMakeTime() throws IOException {
    var result = executeQuery(
        "select MAKETIME(20, 30, 40) as f1, MAKETIME(20.2, 49.5, 42.100502) as f2");
    verifySchema(result,
        schema("MAKETIME(20, 30, 40)", "f1", "time"),
        schema("MAKETIME(20.2, 49.5, 42.100502)", "f2", "time"));
    verifyDataRows(result, rows("20:30:40", "20:50:42.100502"));
  }

  @Test
  public void testMakeDate() throws IOException {
    var result = executeQuery(
        "select MAKEDATE(1945, 5.9) as f1, MAKEDATE(1984, 1984) as f2");
    verifySchema(result,
        schema("MAKEDATE(1945, 5.9)", "f1", "date"),
        schema("MAKEDATE(1984, 1984)", "f2", "date"));
    verifyDataRows(result, rows("1945-01-06", "1989-06-06"));
  }

  public static LocalDateTime utcDateTimeNow() {
    ZonedDateTime zonedDateTime =
        LocalDateTime.now().atZone(TimeZone.getDefault().toZoneId());
    return zonedDateTime.withZoneSameInstant(ZoneId.of("UTC")).toLocalDateTime();
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
              .put("hasShortcut", false)
              .put("constValue", true)
              .put("referenceGetter", (Supplier<Temporal>) LocalDateTime::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse)
              .put("serializationPattern", "uuuu-MM-dd HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "localtimestamp")
              .put("hasFsp", false)
              .put("hasShortcut", false)
              .put("constValue", true)
              .put("referenceGetter", (Supplier<Temporal>) LocalDateTime::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse)
              .put("serializationPattern", "uuuu-MM-dd HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "localtime")
              .put("hasFsp", false)
              .put("hasShortcut", false)
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
              .put("hasShortcut", false)
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
              .put("hasShortcut", false)
              .put("constValue", false)
              .put("referenceGetter", (Supplier<Temporal>) LocalDate::now)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDate::parse)
              .put("serializationPattern", "uuuu-MM-dd")
              .build(),
      ImmutableMap.builder()
              .put("name", "utc_date")
              .put("hasFsp", false)
              .put("hasShortcut", false)
              .put("constValue", true)
              .put("referenceGetter", (Supplier<Temporal>) (()-> utcDateTimeNow().toLocalDate()))
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDate::parse)
              .put("serializationPattern", "uuuu-MM-dd")
              .build(),
      ImmutableMap.builder()
              .put("name", "utc_time")
              .put("hasFsp", false)
              .put("hasShortcut", false)
              .put("constValue", true)
              .put("referenceGetter", (Supplier<Temporal>) (()-> utcDateTimeNow().toLocalTime()))
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalTime::parse)
              .put("serializationPattern", "HH:mm:ss")
              .build(),
      ImmutableMap.builder()
              .put("name", "utc_timestamp")
              .put("hasFsp", false)
              .put("hasShortcut", false)
              .put("constValue", true)
              .put("referenceGetter", (Supplier<Temporal>) DateTimeFunctionIT::utcDateTimeNow)
              .put("parser", (BiFunction<CharSequence, DateTimeFormatter, Temporal>) LocalDateTime::parse)
              .put("serializationPattern", "uuuu-MM-dd HH:mm:ss")
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
      JSONObject result = executeQuery("select " + String.join(", ", calls) + " from " + TEST_INDEX_PEOPLE2);

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
  }

  @Test
  public void testFromUnixTime() throws IOException {
    var result = executeQuery(
        "select FROM_UNIXTIME(200300400) f1, FROM_UNIXTIME(12224.12) f2, "
        + "FROM_UNIXTIME(1662601316, '%T') f3");
    verifySchema(result,
        schema("FROM_UNIXTIME(200300400)", "f1",  "datetime"),
        schema("FROM_UNIXTIME(12224.12)", "f2", "datetime"),
        schema("FROM_UNIXTIME(1662601316, '%T')", "f3", "keyword"));
    verifySome(result.getJSONArray("datarows"),
        rows("1976-05-07 07:00:00", "1970-01-01 03:23:44.12", "01:41:56"));
  }

  @Test
  public void testGetFormatAsArgument() throws IOException{
    var result = executeQuery("SELECT DATE_FORMAT('2003-10-03',GET_FORMAT(DATE,'USA'))");
    verifyDataRows(result, rows("10.03.2003"));
  }

  @Test
  public void testUnixTimeStamp() throws IOException {
    var result = executeQuery(
        "select UNIX_TIMESTAMP(MAKEDATE(1984, 1984)) f1, "
        + "UNIX_TIMESTAMP(TIMESTAMP('2003-12-31 12:00:00')) f2, "
        + "UNIX_TIMESTAMP(20771122143845) f3");
    verifySchema(result,
        schema("UNIX_TIMESTAMP(MAKEDATE(1984, 1984))", "f1", "double"),
        schema("UNIX_TIMESTAMP(TIMESTAMP('2003-12-31 12:00:00'))", "f2", "double"),
        schema("UNIX_TIMESTAMP(20771122143845)", "f3", "double"));
    verifySome(result.getJSONArray("datarows"), rows(613094400d, 1072872000d, 3404817525d));
  }

  @Test
  public void testPeriodAdd() throws IOException {
    var result = executeQuery(
        "select PERIOD_ADD(200801, 2) as f1, PERIOD_ADD(200801, -12) as f2");
    verifySchema(result,
        schema("PERIOD_ADD(200801, 2)", "f1", "integer"),
        schema("PERIOD_ADD(200801, -12)", "f2", "integer"));
    verifyDataRows(result, rows(200803, 200701));
  }

  @Test
  public void testPeriodDiff() throws IOException {
    var result = executeQuery(
        "select PERIOD_DIFF(200802, 200703) as f1, PERIOD_DIFF(200802, 201003) as f2");
    verifySchema(result,
        schema("PERIOD_DIFF(200802, 200703)", "f1", "integer"),
        schema("PERIOD_DIFF(200802, 201003)", "f2", "integer"));
    verifyDataRows(result, rows(11, -25));
  }

  public void testAddTime() throws IOException {
    var result = executeQuery("SELECT"
        + " ADDTIME(DATE('2008-12-12'), DATE('2008-11-15')) AS `'2008-12-12' + 0`,"
        + " ADDTIME(TIME('23:59:59'), DATE('2004-01-01')) AS `'23:59:59' + 0`,"
        + " ADDTIME(DATE('2004-01-01'), TIME('23:59:59')) AS `'2004-01-01' + '23:59:59'`,"
        + " ADDTIME(TIME('10:20:30'), TIME('00:05:42')) AS `'10:20:30' + '00:05:42'`,"
        + " ADDTIME(TIMESTAMP('1999-12-31 15:42:13'), DATETIME('1961-04-12 09:07:00')) AS `'15:42:13' + '09:07:00'`");
    verifySchema(result,
        schema("ADDTIME(DATE('2008-12-12'), DATE('2008-11-15'))", "'2008-12-12' + 0", "datetime"),
        schema("ADDTIME(TIME('23:59:59'), DATE('2004-01-01'))", "'23:59:59' + 0", "time"),
        schema("ADDTIME(DATE('2004-01-01'), TIME('23:59:59'))", "'2004-01-01' + '23:59:59'", "datetime"),
        schema("ADDTIME(TIME('10:20:30'), TIME('00:05:42'))", "'10:20:30' + '00:05:42'", "time"),
        schema("ADDTIME(TIMESTAMP('1999-12-31 15:42:13'), DATETIME('1961-04-12 09:07:00'))", "'15:42:13' + '09:07:00'", "datetime"));
    verifyDataRows(result, rows("2008-12-12 00:00:00", "23:59:59", "2004-01-01 23:59:59", "10:26:12", "2000-01-01 00:49:13"));
  }

  @Test
  public void testSubTime() throws IOException {
    var result = executeQuery("SELECT"
        + " SUBTIME(DATE('2008-12-12'), DATE('2008-11-15')) AS `'2008-12-12' - 0`,"
        + " SUBTIME(TIME('23:59:59'), DATE('2004-01-01')) AS `'23:59:59' - 0`,"
        + " SUBTIME(DATE('2004-01-01'), TIME('23:59:59')) AS `'2004-01-01' - '23:59:59'`,"
        + " SUBTIME(TIME('10:20:30'), TIME('00:05:42')) AS `'10:20:30' - '00:05:42'`,"
        + " SUBTIME(TIMESTAMP('1999-12-31 15:42:13'), DATETIME('1961-04-12 09:07:00')) AS `'15:42:13' - '09:07:00'`");
    verifySchema(result,
        schema("SUBTIME(DATE('2008-12-12'), DATE('2008-11-15'))", "'2008-12-12' - 0", "datetime"),
        schema("SUBTIME(TIME('23:59:59'), DATE('2004-01-01'))", "'23:59:59' - 0", "time"),
        schema("SUBTIME(DATE('2004-01-01'), TIME('23:59:59'))", "'2004-01-01' - '23:59:59'", "datetime"),
        schema("SUBTIME(TIME('10:20:30'), TIME('00:05:42'))", "'10:20:30' - '00:05:42'", "time"),
        schema("SUBTIME(TIMESTAMP('1999-12-31 15:42:13'), DATETIME('1961-04-12 09:07:00'))", "'15:42:13' - '09:07:00'", "datetime"));
    verifyDataRows(result, rows("2008-12-12 00:00:00", "23:59:59", "2003-12-31 00:00:01", "10:14:48", "1999-12-31 06:35:13"));
  }

  public void testDateDiff() throws IOException {
    var result = executeQuery("SELECT"
        + " DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59')) AS `'2000-01-02' - '2000-01-01'`,"
        + " DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01 00:00:00')) AS `'2001-02-01' - '2004-01-01'`,"
        + " DATEDIFF(TIMESTAMP('2004-01-01 00:00:00'), DATETIME('2002-02-01 14:25:30')) AS `'2004-01-01' - '2002-02-01'`,"
        + " DATEDIFF(TIME('23:59:59'), TIME('00:00:00')) AS `today - today`");
    verifySchema(result,
        schema("DATEDIFF(TIMESTAMP('2000-01-02 00:00:00'), TIMESTAMP('2000-01-01 23:59:59'))", "'2000-01-02' - '2000-01-01'", "long"),
        schema("DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01 00:00:00'))", "'2001-02-01' - '2004-01-01'", "long"),
        schema("DATEDIFF(TIMESTAMP('2004-01-01 00:00:00'), DATETIME('2002-02-01 14:25:30'))", "'2004-01-01' - '2002-02-01'", "long"),
        schema("DATEDIFF(TIME('23:59:59'), TIME('00:00:00'))", "today - today", "long"));
    verifyDataRows(result, rows(1, -1064, 699, 0));
  }

  @Test
  public void testTimeDiff() throws IOException {
    var result = executeQuery("select TIMEDIFF('23:59:59', '13:00:00') as f");
    verifySchema(result, schema("TIMEDIFF('23:59:59', '13:00:00')", "f", "time"));
    verifyDataRows(result, rows("10:59:59"));
  }

  void verifyTimeFormat(String time, String type, String format, String formatted) throws IOException {
    String query = String.format("time_format(%s('%s'), '%s')", type, time, format);
    JSONObject result = executeQuery("select " + query);
    verifySchema(result, schema(query, null, "keyword"));
    verifyDataRows(result, rows(formatted));

    query = String.format("time_format('%s', '%s')", time, format);
    result = executeQuery("select " + query);
    verifySchema(result, schema(query, null, "keyword"));
    verifyDataRows(result, rows(formatted));
  }

  @Test
  public void testTimeFormat() throws IOException {
    String timestamp = "1998-01-31 13:14:15.012345";
    String timestampFormat = "%f %H %h %I %i %p %r %S %s %T";
    String timestampFormatted = "012345 13 01 01 14 PM 01:14:15 PM 15 15 13:14:15";
    verifyTimeFormat(timestamp, "timestamp", timestampFormat, timestampFormatted);
  }

  protected JSONObject executeQuery(String query) throws IOException {
    Request request = new Request("POST", QUERY_API_ENDPOINT);
    request.setJsonEntity(String.format(Locale.ROOT, "{\n" + "  \"query\": \"%s\"\n" + "}", query));

    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);

    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }
}
