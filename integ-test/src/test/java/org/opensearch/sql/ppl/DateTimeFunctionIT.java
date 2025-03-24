/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DATE;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;
import static org.opensearch.sql.util.MatcherUtils.verifySome;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.common.utils.StringUtils;

@SuppressWarnings("unchecked")
public class DateTimeFunctionIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.DATE);
    loadIndex(Index.PEOPLE2);
    loadIndex(Index.BANK);
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
  public void testAddDateWithDays() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval " + " f = adddate(date('2020-09-16'), 1)" + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = adddate(timestamp('2020-09-16 17:30:00'), 1)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = adddate(DATETIME('2020-09-16 07:40:00'), 1)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 07:40:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval " + " f = adddate(TIME('07:40:00'), 0)" + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(LocalDate.now() + " 07:40:00"));
  }

  @Test
  public void testAddDateWithInterval() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = adddate(timestamp('2020-09-16 17:30:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = adddate(DATETIME('2020-09-16 17:30:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = adddate(date('2020-09-16'), interval 1 day) "
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 00:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = adddate(date('2020-09-16'), interval 1 hour)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-16 01:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = adddate(TIME('07:40:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            LocalDate.now()
                .plusDays(1)
                .atTime(LocalTime.of(7, 40))
                .atZone(systemTz.toZoneId())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = adddate(TIME('07:40:00'), interval 1 hour)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            LocalDate.now()
                .atTime(LocalTime.of(8, 40))
                .atZone(systemTz.toZoneId())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
  }

  @Test
  public void testConvertTZ() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2008-05-15 12:00:00','+00:00','+10:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-05-15 22:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 00:00:00','-00:00','+00:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 00:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 00:00:00','+10:00','+11:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 01:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 11:34:50','-08:00','+09:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-13 04:34:50"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 11:34:50','+09:00','+09:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 11:34:50"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 11:34:50','-12:00','+12:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-13 11:34:50"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 13:00:00','+09:30','+05:45') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2021-05-12 09:15:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-30 11:34:50','-17:00','+08:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-05-12 11:34:50','-12:00','+15:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void testDateAdd() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_add(timestamp('2020-09-16 17:30:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_add(DATETIME('2020-09-16 17:30:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_add(date('2020-09-16'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-17 00:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_add(date('2020-09-16'), interval 1 hour)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-16 01:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_add(TIME('07:40:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            LocalDate.now()
                .plusDays(1)
                .atTime(LocalTime.of(7, 40))
                .atZone(systemTz.toZoneId())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_add(TIME('07:40:00'), interval 1 hour)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            LocalDate.now()
                .atTime(LocalTime.of(8, 40))
                .atZone(systemTz.toZoneId())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result =
        executeQuery(
            String.format(
                "source=%s | eval " + " f = DATE_ADD(birthdate, INTERVAL 1 YEAR)" + " | fields f",
                TEST_INDEX_BANK));
    verifySchema(result, schema("f", null, "datetime"));
    verifyDataRows(
        result,
        rows("2018-10-23 00:00:00"),
        rows("2018-11-20 00:00:00"),
        rows("2019-06-23 00:00:00"),
        rows("2019-11-13 23:33:20"),
        rows("2019-06-27 00:00:00"),
        rows("2019-08-19 00:00:00"),
        rows("2019-08-11 00:00:00"));
  }

  @Test
  public void testDateTime() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-12-25 05:30:00+00:00', 'America/Los_Angeles')"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-24 21:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-12-25 05:30:00+00:00', '+01:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-25 06:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-12-25 05:30:00-05:00', '+05:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-25 15:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2004-02-28 23:00:00-10:00', '+10:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2004-02-29 19:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2003-02-28 23:00:00-10:00', '+10:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2003-03-01 19:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-12-25 05:30:00+00:00', '+14:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-12-25 19:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00+10:00', '-10:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2007-12-31 06:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00+10:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-01-01 02:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2008-01-01 02:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00+15:00', '-12:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00+10:00', '-14:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = DATETIME('2008-01-01 02:00:00', '-14:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void testDateSub() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_sub(timestamp('2020-09-16 17:30:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_sub(DATETIME('2020-09-16 17:30:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_sub(date('2020-09-16'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 00:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_sub(date('2020-09-16'), interval 1 hour)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 23:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_sub(TIME('07:40:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            LocalDate.now()
                .plusDays(-1)
                .atTime(LocalTime.of(7, 40))
                .atZone(systemTz.toZoneId())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = date_sub(TIME('07:40:00'), interval 1 hour)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            LocalDate.now()
                .atTime(LocalTime.of(6, 40))
                .atZone(systemTz.toZoneId())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
  }

  @Test
  public void testDay() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  day(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));

    result =
        executeQuery(
            String.format("source=%s | eval f =  day('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));
  }

  @Test
  public void testDay_of_week() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  day_of_week(date('2020-09-16')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(4));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  day_of_week('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(4));
  }

  @Test
  public void testDay_of_month() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  day_of_month(date('2020-09-16')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  day_of_month('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));
  }

  @Test
  public void testDay_of_year() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  day_of_year(date('2020-09-16')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(260));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  day_of_year('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(260));
  }

  @Test
  public void testDayName() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  dayname(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows("Wednesday"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  dayname('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows("Wednesday"));
  }

  @Test
  public void testDayOfMonth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  dayofmonth(date('2020-09-16')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  dayofmonth('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(16));
  }

  @Test
  public void testDayOfWeek() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  dayofweek(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(4));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  dayofweek('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(4));
  }

  @Test
  public void testDayOfYear() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  dayofyear(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(260));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  dayofyear('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(260));
  }

  @Test
  public void testFromDays() throws IOException {
    JSONObject result =
        executeQuery(
            String.format("source=%s | eval f =  from_days(738049) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-16"));
  }

  @Test
  public void testHour() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  hour(timestamp('2020-09-16 17:30:00')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  hour(time('17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  hour('2020-09-16 17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));

    result =
        executeQuery(
            String.format("source=%s | eval f =  hour('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));
  }

  @Test
  public void testHour_of_day() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  hour_of_day(timestamp('2020-09-16 17:30:00')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  hour_of_day(time('17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  hour_of_day('2020-09-16 17:30:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  hour_of_day('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(17));
  }

  @Test
  public void testMicrosecond() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  microsecond(timestamp('2020-09-16 17:30:00.123456')) |"
                    + " fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123456));

    // Explicit timestamp value with less than 6 microsecond digits
    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  microsecond(timestamp('2020-09-16 17:30:00.1234')) | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123400));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  microsecond(time('17:30:00.000010')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(10));

    // Explicit time value with less than 6 microsecond digits
    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  microsecond(time('17:30:00.1234')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123400));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  microsecond('2020-09-16 17:30:00.123456') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123456));

    // Implicit timestamp value with less than 6 microsecond digits
    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  microsecond('2020-09-16 17:30:00.1234') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123400));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  microsecond('17:30:00.000010') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(10));

    // Implicit time value with less than 6 microsecond digits
    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  microsecond('17:30:00.1234') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(123400));
  }

  @Test
  public void testMinute() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute(timestamp('2020-09-16 17:30:00')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute(time('17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute('2020-09-16 17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));

    result =
        executeQuery(
            String.format("source=%s | eval f =  minute('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));
  }

  @Test
  public void testMinute_of_hour() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute_of_hour(timestamp('2020-09-16 17:30:00')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute_of_hour(time('17:30:00')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute_of_hour('2020-09-16 17:30:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute_of_hour('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(30));
  }

  @Test
  public void testMinute_of_day() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute_of_day(timestamp('2020-09-16 17:30:00')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(1050));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute_of_day(time('17:30:00')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(1050));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute_of_day('2020-09-16 17:30:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(1050));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  minute_of_day('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(1050));
  }

  @Test
  public void testMonth() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  month(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(9));

    result =
        executeQuery(
            String.format("source=%s | eval f =  month('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(9));
  }

  @Test
  public void testMonth_of_year() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  month_of_year(date('2020-09-16')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(9));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  month_of_year('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(9));
  }

  @Test
  public void testMonthName() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  monthname(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows("September"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  monthname('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows("September"));
  }

  @Test
  public void testQuarter() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  quarter(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(3));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  quarter('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(3));
  }

  @Test
  public void testSecond() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  second(timestamp('2020-09-16 17:30:00')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  second(time('17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  second('2020-09-16 17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));

    result =
        executeQuery(
            String.format("source=%s | eval f =  second('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));
  }

  @Test
  public void testSecond_of_minute() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  second_of_minute(timestamp('2020-09-16 17:30:00')) | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  second_of_minute(time('17:30:00')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  second_of_minute('2020-09-16 17:30:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  second_of_minute('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(0));
  }

  @Test
  public void testSubDateDays() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval " + " f = subdate(date('2020-09-16'), 1)" + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = subdate(timestamp('2020-09-16 17:30:00'), 1)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval " + " f = subdate(date('2020-09-16'), 1)" + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval " + " f = subdate(TIME('07:40:00'), 0)" + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(LocalDate.now() + " 07:40:00"));
  }

  @Test
  public void testSubDateInterval() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = subdate(timestamp('2020-09-16 17:30:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = subdate(DATETIME('2020-09-16 17:30:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 17:30:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = subdate(date('2020-09-16'), interval 1 day) "
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 00:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = subdate(date('2020-09-16'), interval 1 hour)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2020-09-15 23:00:00"));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = subdate(TIME('07:40:00'), interval 1 day)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            LocalDate.now()
                .plusDays(-1)
                .atTime(LocalTime.of(7, 40))
                .atZone(systemTz.toZoneId())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));

    result =
        executeQuery(
            String.format(
                "source=%s | eval "
                    + " f = subdate(TIME('07:40:00'), interval 1 hour)"
                    + " | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            LocalDate.now()
                .atTime(LocalTime.of(6, 40))
                .atZone(systemTz.toZoneId())
                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))));
  }

  @Test
  public void testTimeToSec() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  time_to_sec(time('17:30:00')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(63000));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  time_to_sec('17:30:00') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(63000));
  }

  @Test
  public void testToDays() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  to_days(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(738049));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f =  to_days('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(738049));
  }

  private void week(String date, int mode, int expectedResult) throws IOException {
    JSONObject result =
        executeQuery(
            StringUtils.format(
                "source=%s | eval f = week(date('%s'), %d) | fields f",
                TEST_INDEX_DATE, date, mode));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(expectedResult));
  }

  @Test
  public void testWeek() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
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
  public void testWeek_of_year() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = week_of_year(date('2008-02-20')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(7));
  }

  @Test
  public void testYear() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f =  year(date('2020-09-16')) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(2020));

    result =
        executeQuery(
            String.format("source=%s | eval f =  year('2020-09-16') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(2020));
  }

  void verifyDateFormat(String date, String type, String format, String formatted)
      throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = date_format(%s('%s'), '%s') | fields f",
                TEST_INDEX_DATE, type, date, format));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows(formatted));

    result =
        executeQuery(
            String.format(
                "source=%s | eval f = date_format('%s', '%s') | fields f",
                TEST_INDEX_DATE, date, format));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows(formatted));
  }

  @Test
  public void testDateFormat() throws IOException {
    String timestamp = "1998-01-31 13:14:15.012345";
    String timestampFormat =
        "%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M " + "%m %p %r %S %s %T %% %P";
    String timestampFormatted =
        "Sat Jan 01 31st 31 31 012345 13 01 01 14 031 13 1 "
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
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f1 = MAKETIME(20, 30, 40), f2 = MAKETIME(20.2, 49.5, 42.100502) |"
                    + " fields f1, f2",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f1", null, "time"), schema("f2", null, "time"));
    verifySome(result.getJSONArray("datarows"), rows("20:30:40", "20:50:42.100502"));
  }

  @Test
  public void testMakeDate() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f1 = MAKEDATE(1945, 5.9), f2 = MAKEDATE(1984, 1984) | fields f1,"
                    + " f2",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f1", null, "date"), schema("f2", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("1945-01-06", "1989-06-06"));
  }

  @Test
  public void testAddTime() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval `'2008-12-12' + 0` = ADDTIME(DATE('2008-12-12'),"
                    + " DATE('2008-11-15')), `'23:59:59' + 0` = ADDTIME(TIME('23:59:59'),"
                    + " DATE('2004-01-01')), `'2004-01-01' + '23:59:59'` ="
                    + " ADDTIME(DATE('2004-01-01'), TIME('23:59:59')), `'10:20:30' + '00:05:42'` ="
                    + " ADDTIME(TIME('10:20:30'), TIME('00:05:42')), `'15:42:13' + '09:07:00'` ="
                    + " ADDTIME(TIMESTAMP('1999-12-31 15:42:13'), DATETIME('1961-04-12 09:07:00'))"
                    + " | fields `'2008-12-12' + 0`, `'23:59:59' + 0`, `'2004-01-01' + '23:59:59'`,"
                    + " `'10:20:30' + '00:05:42'`, `'15:42:13' + '09:07:00'`",
                TEST_INDEX_DATE));
    verifySchema(
        result,
        schema("'2008-12-12' + 0", null, "datetime"),
        schema("'23:59:59' + 0", null, "time"),
        schema("'2004-01-01' + '23:59:59'", null, "datetime"),
        schema("'10:20:30' + '00:05:42'", null, "time"),
        schema("'15:42:13' + '09:07:00'", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            "2008-12-12 00:00:00",
            "23:59:59",
            "2004-01-01 23:59:59",
            "10:26:12",
            "2000-01-01 00:49:13"));
  }

  @Test
  public void testSubTime() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval `'2008-12-12' - 0` = SUBTIME(DATE('2008-12-12'),"
                    + " DATE('2008-11-15')), `'23:59:59' - 0` = SUBTIME(TIME('23:59:59'),"
                    + " DATE('2004-01-01')), `'2004-01-01' - '23:59:59'` ="
                    + " SUBTIME(DATE('2004-01-01'), TIME('23:59:59')), `'10:20:30' - '00:05:42'` ="
                    + " SUBTIME(TIME('10:20:30'), TIME('00:05:42')), `'15:42:13' - '09:07:00'` ="
                    + " SUBTIME(TIMESTAMP('1999-12-31 15:42:13'), DATETIME('1961-04-12 09:07:00'))"
                    + " | fields `'2008-12-12' - 0`, `'23:59:59' - 0`, `'2004-01-01' - '23:59:59'`,"
                    + " `'10:20:30' - '00:05:42'`, `'15:42:13' - '09:07:00'`",
                TEST_INDEX_DATE));
    verifySchema(
        result,
        schema("'2008-12-12' - 0", null, "datetime"),
        schema("'23:59:59' - 0", null, "time"),
        schema("'2004-01-01' - '23:59:59'", null, "datetime"),
        schema("'10:20:30' - '00:05:42'", null, "time"),
        schema("'15:42:13' - '09:07:00'", null, "datetime"));
    verifySome(
        result.getJSONArray("datarows"),
        rows(
            "2008-12-12 00:00:00",
            "23:59:59",
            "2003-12-31 00:00:01",
            "10:14:48",
            "1999-12-31 06:35:13"));
  }

  @Test
  public void testFromUnixTime() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f1 = FROM_UNIXTIME(200300400), f2 = FROM_UNIXTIME(12224.12), "
                    + "f3 = FROM_UNIXTIME(1662601316, '%%T') | fields f1, f2, f3",
                TEST_INDEX_DATE));
    verifySchema(
        result,
        schema("f1", null, "datetime"),
        schema("f2", null, "datetime"),
        schema("f3", null, "string"));
    verifySome(
        result.getJSONArray("datarows"),
        rows("1976-05-07 07:00:00", "1970-01-01 03:23:44.12", "01:41:56"));
  }

  @Test
  public void testUnixTimeStamp() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f1 = UNIX_TIMESTAMP(MAKEDATE(1984, 1984)), "
                    + "f2 = UNIX_TIMESTAMP(TIMESTAMP('2003-12-31 12:00:00')), "
                    + "f3 = UNIX_TIMESTAMP(20771122143845) | fields f1, f2, f3",
                TEST_INDEX_DATE));
    verifySchema(
        result,
        schema("f1", null, "double"),
        schema("f2", null, "double"),
        schema("f3", null, "double"));
    verifySome(result.getJSONArray("datarows"), rows(613094400d, 1072872000d, 3404817525d));
  }

  @Test
  public void testPeriodAdd() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f1 = PERIOD_ADD(200801, 2), f2 = PERIOD_ADD(200801, -12) | fields"
                    + " f1, f2",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f1", null, "integer"), schema("f2", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(200803, 200701));
  }

  @Test
  public void testPeriodDiff() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f1 = PERIOD_DIFF(200802, 200703), f2 = PERIOD_DIFF(200802,"
                    + " 201003) | fields f1, f2",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f1", null, "integer"), schema("f2", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(11, -25));
  }

  public void testDateDiff() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval `'2000-01-02' - '2000-01-01'` = DATEDIFF(TIMESTAMP('2000-01-02"
                    + " 00:00:00'), TIMESTAMP('2000-01-01 23:59:59')), `'2001-02-01' -"
                    + " '2004-01-01'` = DATEDIFF(DATE('2001-02-01'), TIMESTAMP('2004-01-01"
                    + " 00:00:00')), `'2004-01-01' - '2002-02-01'` = DATEDIFF(TIMESTAMP('2004-01-01"
                    + " 00:00:00'), DATETIME('2002-02-01 14:25:30')), `today - today` ="
                    + " DATEDIFF(TIME('23:59:59'), TIME('00:00:00')) | fields `'2000-01-02' -"
                    + " '2000-01-01'`, `'2001-02-01' - '2004-01-01'`, `'2004-01-01' -"
                    + " '2002-02-01'`, `today - today`",
                TEST_INDEX_DATE));
    verifySchema(
        result,
        schema("'2000-01-02' - '2000-01-01'", null, "long"),
        schema("'2001-02-01' - '2004-01-01'", null, "long"),
        schema("'2004-01-01' - '2002-02-01'", null, "long"),
        schema("today - today", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(1, -1064, 699, 0));
  }

  @Test
  public void testTimeDiff() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f = TIMEDIFF('23:59:59', '13:00:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "time"));
    verifySome(result.getJSONArray("datarows"), rows("10:59:59"));
  }

  @Test
  public void testGetFormat() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f = date_format('2003-10-03', get_format(DATE,'USA')) | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "string"));
    verifySome(result.getJSONArray("datarows"), rows("10.03.2003"));
  }

  @Test
  public void testLastDay() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f = last_day('2003-10-03') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "date"));
    verifySome(result.getJSONArray("datarows"), rows("2003-10-31"));
  }

  @Test
  public void testSecToTime() throws IOException {
    var result =
        executeQuery(
            String.format("source=%s | eval f = sec_to_time(123456) | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "time"));
    verifySome(result.getJSONArray("datarows"), rows("10:17:36"));
  }

  @Test
  public void testYearWeek() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f1 = yearweek('2003-10-03') | eval f2 = yearweek('2003-10-03', 3)"
                    + " | fields f1, f2",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f1", null, "integer"), schema("f2", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(200339, 200340));
  }

  @Test
  public void testWeekDay() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f = weekday('2003-10-03') | fields f", TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "integer"));
    verifySome(result.getJSONArray("datarows"), rows(4));
  }

  @Test
  public void testToSeconds() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f1 = to_seconds(date('2008-10-07')) | "
                    + "eval f2 = to_seconds('2020-09-16 07:40:00') | "
                    + "eval f3 = to_seconds(DATETIME('2020-09-16 07:40:00')) | fields f1, f2, f3",
                TEST_INDEX_DATE));
    verifySchema(
        result, schema("f1", null, "long"), schema("f2", null, "long"), schema("f3", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(63390556800L, 63767461200L, 63767461200L));
  }

  @Test
  public void testStrToDate() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f = str_to_date('01,5,2013', '%s') | fields f",
                TEST_INDEX_DATE, "%d,%m,%Y"));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2013-05-01 00:00:00"));
    // two digits year case
    result =
        executeQuery(
            String.format(
                "source=%s | eval f = str_to_date('1-May-13', '%s') | fields f",
                TEST_INDEX_DATE, "%d-%b-%y"));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2013-05-01 00:00:00"));
  }

  @Test
  public void testTimeStampAdd() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f = timestampadd(YEAR, 15, '2001-03-06 00:00:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows("2016-03-06 00:00:00"));
  }

  @Test
  public void testTimestampDiff() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f = timestampdiff(YEAR, '1997-01-01 00:00:00', '2001-03-06"
                    + " 00:00:00') | fields f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "datetime"));
    verifySome(result.getJSONArray("datarows"), rows(4));
  }

  @Test
  public void testExtract() throws IOException {
    var result =
        executeQuery(
            String.format(
                "source=%s | eval f1 = extract(YEAR FROM '1997-01-01 00:00:00') | eval f2 ="
                    + " extract(MINUTE FROM time('10:17:36')) | fields f1, f2",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f1", null, "long"), schema("f2", null, "long"));
    verifySome(result.getJSONArray("datarows"), rows(1997L, 17L));
  }
}
