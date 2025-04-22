/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.standalone;

import static org.opensearch.sql.expression.datetime.DateTimeFunctions.exprYearweek;
import static org.opensearch.sql.expression.datetime.DateTimeFunctions.formatNow;
import static org.opensearch.sql.legacy.TestsConstants.*;
import static org.opensearch.sql.util.MatcherUtils.*;
import static org.opensearch.sql.util.MatcherUtils.rows;

import java.io.IOException;
import java.sql.Date;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import org.hamcrest.Matchers;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.expression.function.FunctionProperties;

public class CalcitePPLDateTimeBuiltinFunctionIT extends CalcitePPLIntegTestCase {
  @Override
  public void init() throws IOException {
    super.init();
    loadIndex(Index.STATE_COUNTRY);
    loadIndex(Index.STATE_COUNTRY_WITH_NULL);
    loadIndex(Index.DATE_FORMATS);
    loadIndex(Index.BANK_WITH_NULL_VALUES);
    loadIndex(Index.DATE);
    loadIndex(Index.PEOPLE2);
    loadIndex(Index.BANK);
    initRelativeDocs();
  }

  private static String getFormattedLocalDate() {
    return LocalDateTime.now(ZoneId.systemDefault())
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
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
  public void testDate() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval `DATE('2020-08-26')` = DATE('2020-08-26') | eval"
                    + " `DATE(TIMESTAMP('2020-08-26 13:49:00'))` = DATE(TIMESTAMP('2020-08-26"
                    + " 13:49:00')) | eval `DATE('2020-08-26 13:49')` = DATE('2020-08-26 13:49') "
                    + "| eval d = DATE(strict_date_time)"
                    + "| fields `DATE('2020-08-26')`, `DATE(TIMESTAMP('2020-08-26 13:49:00'))`,"
                    + " `DATE('2020-08-26 13:49')`, d | head 1",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("DATE('2020-08-26')", "date"),
        schema("DATE(TIMESTAMP('2020-08-26 13:49:00'))", "date"),
        schema("DATE('2020-08-26 13:49')", "date"),
        schema("d", "date"));

    verifyDataRows(
        actual,
        rows(
            Date.valueOf("2020-08-26"),
            Date.valueOf("2020-08-26"),
            Date.valueOf("2020-08-26"),
            Date.valueOf("1984-04-12")));
  }

  @Test
  public void testTimestamp() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval `TIMESTAMP('2020-08-26 13:49:00')` ="
                    + " TIMESTAMP('2020-08-26 13:49:00')| eval `TIMESTAMP(DATE('2020-08-26"
                    + " 13:49:00'))` = TIMESTAMP(DATE('2020-08-26 13:49:00'))| eval"
                    + " `TIMESTAMP(TIMESTAMP('2020-08-26 13:49:00'))` ="
                    + " TIMESTAMP(TIMESTAMP('2020-08-26 13:49:00'))| eval"
                    + " `TIMESTAMP(TIME('2020-08-26 13:49:00'))` = TIMESTAMP(TIME('2020-08-26"
                    + " 13:49:00'))| eval `TIMESTAMP('2020-08-26 13:49:00', 2020-08-26 00:10:10)` ="
                    + " TIMESTAMP('2020-08-26 13:49:00', '2020-08-26 00:10:10')| eval"
                    + " `TIMESTAMP('2020-08-26 13:49:00', TIMESTAMP(2020-08-26 00:10:10))` ="
                    + " TIMESTAMP('2020-08-26 13:49:00', TIMESTAMP('2020-08-26 00:10:10'))| eval"
                    + " `TIMESTAMP('2020-08-26 13:49:00', DATE(2020-08-26 00:10:10))` ="
                    + " TIMESTAMP('2020-08-26 13:49:00', DATE('2020-08-26 00:10:10'))| eval"
                    + " `TIMESTAMP('2020-08-26 13:49:00', TIME(00:10:10))` = TIMESTAMP('2020-08-26"
                    + " 13:49:00', TIME('00:10:10')), ts = TIMESTAMP('2009-12-12"
                    + " 13:40:04.123456789')| fields `TIMESTAMP('2020-08-26 13:49:00')`,"
                    + " `TIMESTAMP(DATE('2020-08-26 13:49:00'))`, `TIMESTAMP(TIMESTAMP('2020-08-26"
                    + " 13:49:00'))`,  `TIMESTAMP(TIME('2020-08-26 13:49:00'))`,"
                    + " `TIMESTAMP('2020-08-26 13:49:00', 2020-08-26 00:10:10)`,"
                    + " `TIMESTAMP('2020-08-26 13:49:00', TIMESTAMP(2020-08-26 00:10:10))`,"
                    + " `TIMESTAMP('2020-08-26 13:49:00', DATE(2020-08-26 00:10:10))`,"
                    + " `TIMESTAMP('2020-08-26 13:49:00', TIME(00:10:10))`, ts",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        actual,
        schema("TIMESTAMP('2020-08-26 13:49:00')", "timestamp"),
        schema("TIMESTAMP(DATE('2020-08-26 13:49:00'))", "timestamp"),
        schema("TIMESTAMP(TIMESTAMP('2020-08-26 13:49:00'))", "timestamp"),
        schema("TIMESTAMP(TIME('2020-08-26 13:49:00'))", "timestamp"),
        schema("TIMESTAMP('2020-08-26 13:49:00', 2020-08-26 00:10:10)", "timestamp"),
        schema("TIMESTAMP('2020-08-26 13:49:00', TIMESTAMP(2020-08-26 00:10:10))", "timestamp"),
        schema("TIMESTAMP('2020-08-26 13:49:00', DATE(2020-08-26 00:10:10))", "timestamp"),
        schema("TIMESTAMP('2020-08-26 13:49:00', TIME(00:10:10))", "timestamp"),
        schema("ts", "timestamp"));

    verifyDataRows(
        actual,
        rows(
            "2020-08-26 13:49:00",
            "2020-08-26 00:00:00",
            "2020-08-26 13:49:00",
            getFormattedLocalDate() + " 13:49:00",
            "2020-08-26 13:59:10",
            "2020-08-26 13:59:10",
            "2020-08-26 13:49:00",
            "2020-08-26 13:59:10",
            "2009-12-12 13:40:04.123456789"));
  }

  @Test
  public void testTime() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval `TIME('2020-08-26 13:49:00')` = TIME('2020-08-26 13:49:00')| eval"
                    + " `TIME('2020-08-26 13:49')` = TIME('2020-08-26 13:49')| eval `TIME('13:49')`"
                    + " = TIME('13:49')| eval `TIME('13:49:00.123')` = TIME('13:49:00.123')| eval"
                    + " `TIME(TIME('13:49:00'))` = TIME(TIME('13:49:00'))| eval"
                    + " `TIME(TIMESTAMP('2024-08-06 13:49:00'))` = TIME(TIMESTAMP('2024-08-06"
                    + " 13:49:00'))| eval `TIME(DATE('2024-08-06 13:49:00'))` ="
                    + " TIME(DATE('2024-08-06 13:49:00')), t = TIME('13:49:00.123456789')"
                    + " | fields `TIME('2020-08-26 13:49:00')`,"
                    + " `TIME('2020-08-26 13:49')`, `TIME('13:49')`,  `TIME('13:49:00.123')`,"
                    + " `TIME(TIME('13:49:00'))`, `TIME(TIMESTAMP('2024-08-06 13:49:00'))`,"
                    + " `TIME(DATE('2024-08-06 13:49:00'))`, t | head 1",
                TEST_INDEX_STATE_COUNTRY));

    verifySchema(
        actual,
        schema("TIME('2020-08-26 13:49:00')", "time"),
        schema("TIME('2020-08-26 13:49')", "time"),
        schema("TIME('13:49')", "time"),
        schema("TIME('13:49:00.123')", "time"),
        schema("TIME(TIME('13:49:00'))", "time"),
        schema("TIME(TIMESTAMP('2024-08-06 13:49:00'))", "time"),
        schema("TIME(DATE('2024-08-06 13:49:00'))", "time"),
        schema("t", "time"));

    verifyDataRows(
        actual,
        rows(
            "13:49:00",
            "13:49:00",
            "13:49:00",
            "13:49:00.123",
            "13:49:00",
            "13:49:00",
            "00:00:00",
            "13:49:00.123456789"));
  }

  @Test
  public void testDateSubAndCount() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where strict_date_optional_time > DATE_SUB(TIMESTAMP('1984-04-12"
                    + " 20:07:00'), INTERVAL 12 HOUR) | stats COUNT() AS CNT ",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("CNT", "long"));

    // relative ones
    verifyDataRows(actual, rows(7));
  }

  @Test
  public void testTimeStrToDate() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where YEAR(strict_date_optional_time) < 2000| eval demo ="
                    + " str_to_date(\"01,5,2013\", \"%%d,%%m,%%Y\")| where"
                    + " str_to_date(\"01,5,2013\", \"%%d,%%m,%%Y\")='2013-05-01 00:00:00'| eval s2d"
                    + " =  STR_TO_DATE('2010-09-10 12:56:45.123456', '%%Y-%%m-%%d %%T.%%f')| fields"
                    + " demo, s2d | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("demo", "timestamp"), schema("s2d", "timestamp"));
    verifyDataRows(actual, rows("2013-05-01 00:00:00", "2010-09-10 12:56:45"));
  }

  @Test
  public void testTimeFormat() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where YEAR(strict_date_optional_time) < 2000| eval"
                    + " timestamp=TIME_FORMAT(strict_date_optional_time, '%%h') | eval"
                    + " time=TIME_FORMAT(time, '%%h')| eval date=TIME_FORMAT(date, '%%h')| eval"
                    + " string_value=TIME_FORMAT('1998-01-31 13:14:15.012345','%%h %%i %%f' ) |"
                    + " where TIME_FORMAT(strict_date_optional_time, '%%h')='09'| fields timestamp,"
                    + " time, date, string_value | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("timestamp", "string"),
        schema("time", "string"),
        schema("date", "string"),
        schema("string_value", "string"));
    verifyDataRows(actual, rows("09", "09", "12", "01 14 012345"));
  }

  @Test
  public void testTimeToSec() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where YEAR(strict_date_optional_time) < 2000"
                    + "| eval timestamp=TIME_TO_SEC(strict_date_optional_time) "
                    + "| eval time=TIME_TO_SEC(time)"
                    + "| eval date=TIME_TO_SEC(date)"
                    + "| eval long_value=TIME_TO_SEC('22:23:00') "
                    + "| where TIME_TO_SEC('22:23:00')=80580"
                    + "| fields timestamp, time, date, long_value | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("timestamp", "long"),
        schema("time", "long"),
        schema("date", "long"),
        schema("long_value", "long"));
    verifyDataRows(actual, rows(32862, 32862, 0, 80580));
  }

  @Test
  public void testSecToTime() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where YEAR(strict_date_optional_time) < 2000"
                    + "| eval long_value=SEC_TO_TIME(3601) "
                    + "| eval double_value=SEC_TO_TIME(1234.123) "
                    + "| fields long_value, double_value | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("long_value", "time"), schema("double_value", "time"));
    verifyDataRows(actual, rows("01:00:01", "00:20:34.123"));
  }

  @Test
  public void testToSeconds() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where YEAR(strict_date_optional_time) < 2000"
                    + "| eval timestamp=to_seconds(strict_date_optional_time) "
                    + "| eval date=to_seconds(date)"
                    + "| eval string_value=to_seconds('2008-10-07')"
                    + "| eval long_value = to_seconds(950228)"
                    + "| where to_seconds(strict_date_optional_time) > 62617795199"
                    + "| fields timestamp, date, string_value, long_value | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("timestamp", "long"),
        schema("date", "long"),
        schema("string_value", "long"),
        schema("long_value", "long"));
    verifyDataRows(actual, rows(62617828062L, 62617795200L, 63390556800L, 62961148800L));
  }

  @Test
  public void testToDays() {
    ZonedDateTime utcNow = ZonedDateTime.now(ZoneOffset.UTC);
    LocalDate utcDate = utcNow.toLocalDate();

    // Reference date: year 0
    LocalDate baseDate = LocalDate.of(0, 1, 1);

    // Calculate days since year 0
    long daysSinceYearZero = ChronoUnit.DAYS.between(baseDate, utcDate);
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where YEAR(strict_date_optional_time) < 2000"
                    + "| eval timestamp=to_days(strict_date_optional_time) "
                    + "| eval date=to_days(date)"
                    + "| eval string_value=to_days('2008-10-07')"
                    + "| where to_days(strict_date_optional_time) = 724743"
                    + "| fields timestamp, date, string_value | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("timestamp", "long"),
        schema("date", "long"),
        schema("string_value", "long"));
    verifyDataRows(actual, rows(724743, 724743, 733687));
  }

  @Test
  public void testFromUnixTimeTwoArgument() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| eval from_unix = FROM_UNIXTIME(1220249547, '%%T')"
                    + "| fields from_unix | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("from_unix", "string"));
    verifyDataRows(actual, rows("06:12:27"));
  }

  @Test
  public void testUnixTimeStampAndFromUnixTime() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| eval from_unix = from_unixtime(1220249547)"
                    + "| eval to_unix = unix_timestamp(from_unix)"
                    // + "| where unix_timestamp(from_unixtime(1700000001)) > 1700000000 " // don't
                    // do
                    // filter
                    + "| fields from_unix, to_unix | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("from_unix", "timestamp"), schema("to_unix", "double"));
    verifyDataRows(actual, rows("2008-09-01 06:12:27", 1220249547.0));
  }

  @Test
  public void testWeekAndWeekOfYear() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields  strict_date_optional_time| where"
                    + " YEAR(strict_date_optional_time) < 2000| eval"
                    + " `WEEK(DATE(strict_date_optional_time))` ="
                    + " WEEK(DATE(strict_date_optional_time))| eval"
                    + " `WEEK_OF_YEAR(DATE(strict_date_optional_time))` ="
                    + " WEEK_OF_YEAR(DATE(strict_date_optional_time))| eval"
                    + " `WEEK(DATE(strict_date_optional_time), 1)` ="
                    + " WEEK(DATE(strict_date_optional_time), 1)| eval"
                    + " `WEEK_OF_YEAR(DATE(strict_date_optional_time), 1)` ="
                    + " WEEK_OF_YEAR(DATE(strict_date_optional_time), 1)| eval"
                    + " `WEEK(DATE('2008-02-20'))` = WEEK(DATE('2008-02-20')),"
                    + " `WEEK(DATE('2008-02-20'), 1)` = WEEK(DATE('2008-02-20'), 1)| fields"
                    + " `WEEK(DATE(strict_date_optional_time))`,"
                    + " `WEEK_OF_YEAR(DATE(strict_date_optional_time))`,"
                    + " `WEEK(DATE(strict_date_optional_time), 1)`,"
                    + " `WEEK_OF_YEAR(DATE(strict_date_optional_time), 1)`,"
                    + " `WEEK(DATE('2008-02-20'))`, `WEEK(DATE('2008-02-20'), 1)`| head 1 ",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("WEEK(DATE(strict_date_optional_time))", "integer"),
        schema("WEEK_OF_YEAR(DATE(strict_date_optional_time))", "integer"),
        schema("WEEK(DATE(strict_date_optional_time), 1)", "integer"),
        schema("WEEK_OF_YEAR(DATE(strict_date_optional_time), 1)", "integer"),
        schema("WEEK(DATE('2008-02-20'))", "integer"),
        schema("WEEK(DATE('2008-02-20'), 1)", "integer"));

    verifyDataRows(actual, rows(15, 15, 15, 15, 7, 8));
  }

  @Test
  public void testWeekAndWeekOfYearWithFilter() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | fields  strict_date_optional_time"
                    + "| where YEAR(strict_date_optional_time) < 2000"
                    + "| where WEEK(DATE(strict_date_optional_time)) = 15"
                    + "| stats COUNT() AS CNT "
                    + "| head 1 ",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(actual, schema("CNT", "long"));

    verifyDataRows(actual, rows(2));
  }

  @Test
  public void testWeekDay() {
    int currentWeekDay =
        formatNow(new FunctionProperties().getQueryStartClock()).getDayOfWeek().getValue() - 1;
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where YEAR(strict_date_optional_time) < 2000| eval"
                    + " timestamp=weekday(TIMESTAMP(strict_date_optional_time)),"
                    + " time=weekday(TIME(strict_date_optional_time)),"
                    + " date=weekday(DATE(strict_date_optional_time))| eval `weekday('2020-08-26')`"
                    + " = weekday('2020-08-26') | fields timestamp, time, date,"
                    + " `weekday('2020-08-26')`| head 1 ",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("timestamp", "integer"),
        schema("time", "integer"),
        schema("date", "integer"),
        schema("weekday('2020-08-26')", "integer"));

    verifyDataRows(actual, rows(3, currentWeekDay, 3, 2));
  }

  @Test
  public void testYearWeek() {
    int currentYearWeek =
        exprYearweek(
                new ExprDateValue(
                    LocalDateTime.now(new FunctionProperties().getQueryStartClock()).toLocalDate()),
                new ExprIntegerValue(0))
            .integerValue();
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where YEAR(strict_date_optional_time) < 2000| eval"
                    + " timestamp=YEARWEEK(TIMESTAMP(strict_date_optional_time)),"
                    + " date=YEARWEEK(DATE(strict_date_optional_time))| eval"
                    + " `YEARWEEK('2020-08-26')` = YEARWEEK('2020-08-26') | eval"
                    + " `YEARWEEK('2019-01-05', 1)` = YEARWEEK('2019-01-05', 1) | eval"
                    + " time=YEARWEEK(time) | fields timestamp, time, date,"
                    + " `YEARWEEK('2020-08-26')`, `YEARWEEK('2019-01-05', 1)`| head 1 ",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("timestamp", "integer"),
        schema("time", "integer"),
        schema("date", "integer"),
        schema("YEARWEEK('2020-08-26')", "integer"),
        schema("YEARWEEK('2019-01-05', 1)", "integer"));

    verifyDataRows(actual, rows(198415, currentYearWeek, 198415, 202034, 201901));
  }

  @Test
  public void testYearWeekWithFilter() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| where YEARWEEK(strict_date_optional_time) < 200000"
                    + "| stats COUNT() AS CNT"
                    + "| head 1 ",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(actual, schema("CNT", "long"));

    verifyDataRows(actual, rows(2));
  }

  @Test
  public void testYear() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where YEAR(strict_date_optional_time) = 1984 | eval"
                    + " timestamp=YEAR(TIMESTAMP(strict_date_optional_time)),"
                    + " date=YEAR(DATE(strict_date_optional_time))| eval `YEAR('2020-08-26')` ="
                    + " YEAR('2020-08-26') | fields timestamp, date, `YEAR('2020-08-26')`| head 1 ",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("timestamp", "integer"),
        schema("date", "integer"),
        schema("YEAR('2020-08-26')", "integer"));

    verifyDataRows(actual, rows(1984, 1984, 2020));
  }

  private void initRelativeDocs() throws IOException {
    List<String> relativeList = List.of("NOW", "TMR", "+month", "-2wk", "-1d@d");
    int index = 0;
    for (String time : relativeList) {
      Request request =
          new Request(
              "PUT",
              "/opensearch-sql_test_index_date_formats/_doc/%s?refresh=true".formatted(index));
      request.setJsonEntity(
          "{\"strict_date_optional_time\":\"%s\"}".formatted(convertTimeExpression(time)));

      index++;
      client().performRequest(request);
    }
  }

  private String convertTimeExpression(String expression) {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
    ZonedDateTime result = now;

    switch (expression) {
      case "NOW":
        break;
      case "TMR": // Tomorrow
        result = now.plusDays(1).truncatedTo(ChronoUnit.DAYS);
        break;
      case "+month": // In one month
        result = now.plusMonths(1);
        break;
      case "-2wk": // Two weeks ago
        result = now.minusWeeks(2);
        break;
      case "-1d@d": // Yesterday
        result = now.minusDays(1).truncatedTo(ChronoUnit.DAYS);
        break;
      default:
        throw new IllegalArgumentException("Unknown time expression: " + expression);
    }

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    return result.format(formatter);
  }

  @Test
  public void testAddDateAndSubDateWithConditionsAndRename() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval lower = SUBDATE(strict_date_optional_time_nanos, 3),"
                    + " upper = ADDDATE(date, 1), ts = ADDDATE(date, INTERVAL 1 DAY) | where"
                    + " strict_date < upper | rename strict_date as d | fields lower, upper, d, ts",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("lower", "timestamp"),
        schema("upper", "date"),
        schema("d", "date"),
        schema("ts", "timestamp"));
    verifyDataRows(
        actual,
        rows("1984-04-09 09:07:42.000123456", "1984-04-13", "1984-04-12", "1984-04-13 00:00:00"));
  }

  @Test
  public void testDateAddAndSub() {
    String expectedDate = getFormattedLocalDate();

    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| eval t1 = DATE_ADD(strict_date_optional_time, INTERVAL 1 HOUR) "
                    + "| eval t2 = DATE_ADD(strict_date_optional_time, INTERVAL 1 DAY) "
                    + "| eval t3 = DATE_ADD(strict_date, INTERVAL 1 HOUR) "
                    + "| eval t4 = DATE_ADD('2020-08-26 01:01:01', INTERVAL 1 DAY) "
                    + "| eval t5 = DATE_ADD(time, INTERVAL 1 HOUR) "
                    + "| eval t6 = DATE_ADD(time, INTERVAL 5 HOUR) "
                    + "| eval t7 = DATE_ADD(strict_date, INTERVAL 2 YEAR)"
                    + "| eval t8 = DATE_ADD(DATE('2020-01-30'), INTERVAL 1 MONTH)" // edge case
                    + "| eval t9 = DATE_ADD(DATE('2020-11-30'), INTERVAL 1 QUARTER)" // rare case
                    + "| eval t10 = DATE_SUB(date, INTERVAL 31 DAY)"
                    + "| eval t11 = DATE_SUB(basic_date_time, INTERVAL 1 HOUR)"
                    + "| fields t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11 "
                    + "| head 1",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("t1", "timestamp"),
        schema("t2", "timestamp"),
        schema("t3", "timestamp"),
        schema("t4", "timestamp"),
        schema("t5", "timestamp"),
        schema("t6", "timestamp"),
        schema("t7", "timestamp"),
        schema("t8", "timestamp"),
        schema("t9", "timestamp"),
        schema("t10", "timestamp"),
        schema("t11", "timestamp"));

    verifyDataRows(
        actual,
        rows(
            "1984-04-12 10:07:42",
            "1984-04-13 09:07:42",
            "1984-04-12 01:00:00",
            "2020-08-27 01:01:01",
            expectedDate + " 10:07:42",
            expectedDate + " 14:07:42",
            "1986-04-12 00:00:00",
            "2020-02-29 00:00:00",
            "2021-02-28 00:00:00",
            "1984-03-12 00:00:00",
            "1984-04-12 08:07:42"));
  }

  @Test
  public void testDateAddWithComparisonAndConditions() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where date > DATE('1984-04-11') | eval tomorrow = DATE_ADD(date,"
                    + " INTERVAL 1 DAY) | fields date, tomorrow",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(actual, schema("date", "date"), schema("tomorrow", "timestamp"));
    verifyDataRows(
        actual,
        rows("1984-04-12", "1984-04-13 00:00:00"),
        rows("1984-04-12", "1984-04-13 00:00:00"));
  }

  @org.junit.Test
  public void nullDateTimeInvalidDateValueFebruary() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "source=%s | eval f = convert_tz('2021-02-30 10:00:00','+00:00','+00:00') | fields"
                    + " f",
                TEST_INDEX_DATE));
    verifySchema(result, schema("f", null, "timestamp"));
    verifySome(result.getJSONArray("datarows"), rows(new Object[] {null}));
  }

  @Test
  public void testComparisonBetweenDateAndTimestamp() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where date > TIMESTAMP('1984-04-11 00:00:00') | stats COUNT() AS cnt",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("cnt", "long"));
    verifyDataRows(actual, rows(2));
  }

  @Test
  public void testAddSubTime() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval t1 = ADDTIME(date, date) "
                    + "| eval t2 = ADDTIME(time, date) "
                    + "| eval t3 = SUBTIME(date, time)"
                    + "| eval t4 = ADDTIME(time, time)"
                    + "| eval t5 = SUBTIME(date_time, date_time)"
                    + "| eval t6 = SUBTIME(strict_date_optional_time_nanos, date_time)"
                    + "| fields t1, t2, t3, t4, t5, t6",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("t1", "timestamp"),
        schema("t2", "time"),
        schema("t3", "timestamp"),
        schema("t4", "time"),
        schema("t5", "timestamp"),
        schema("t6", "timestamp"));
    verifyDataRows(
        actual,
        rows(
            "1984-04-12 00:00:00",
            "09:07:42",
            "1984-04-11 14:52:18",
            "18:15:24",
            "1984-04-12 00:00:00",
            "1984-04-12 00:00:00.000123456"));
  }

  /** HOUR, HOUR_OF_DAY, DATE */
  @Test
  public void testHourAndDateWithConditions() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where incomplete_1 > DATE('2000-10-01') | eval t1 = HOUR(date_time),"
                    + " t2 = HOUR_OF_DAY(time), t3 = HOUR('23:14:00'), t4 = HOUR('2023-12-31"
                    + " 16:03:00') | head 1 | fields t1, t2, t3, t4",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("t1", "integer"),
        schema("t2", "integer"),
        schema("t3", "integer"),
        schema("t4", "integer"));
    verifyDataRows(actual, rows(9, 9, 23, 16));
  }

  /** MONTH, MONTH_OF_YEAR */
  @Test
  public void testMonth() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where MONTH(date) > MONTH('2003-03-10') | head 1 |eval m1 ="
                    + " MONTH(date), m2 = MONTH_OF_YEAR(date_time), m3 = MONTH('2023-01-12"
                    + " 10:11:12') | fields m1, m2, m3",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(actual, schema("m1", "integer"), schema("m2", "integer"), schema("m3", "integer"));
    verifyDataRows(actual, rows(4, 4, 1));
  }

  /**
   * CURDATE, CURTIME, CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, NOW, LOCALTIMESTAMP, LOCALTIME
   */
  @Test
  public void testCurrentDateTimeWithComparison() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval cd = CURDATE(), ct = CURTIME(), cdt = CURRENT_DATE(), ctm ="
                    + " CURRENT_TIME(), cts = CURRENT_TIMESTAMP(), now = NOW(), lt = LOCALTIME(),"
                    + " lts = LOCALTIMESTAMP() | where lt = lts and lts = now | where now >= cd and"
                    + " now = cts | fields cd, ct, cdt, ctm, cts, now, lt, lts",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("cd", "date"),
        schema("ct", "time"),
        schema("cdt", "date"),
        schema("ctm", "time"),
        schema("cts", "timestamp"),
        schema("now", "timestamp"),
        schema("lt", "timestamp"),
        schema("lts", "timestamp"));

    // Should return all rows in the index
    verifyNumOfRows(actual, 7);
  }

  @Test
  public void testUtc() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval utc = UTC_TIMESTAMP(), utc1 = UTC_DATE(), utc2 ="
                    + " UTC_TIME() | fields utc, utc1, utc2",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual, schema("utc", "timestamp"), schema("utc1", "date"), schema("utc2", "time"));
  }

  @Test
  public void testSysdate() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval d1 = SYSDATE(), d2 = SYSDATE(3), d3 = SYSDATE(6)|eval"
                    + " df1 = DATE_FORMAT(d1, '%%Y-%%m-%%d %%T.%%f'), df2 = DATE_FORMAT(d2,"
                    + " '%%Y-%%m-%%d %%T.%%f'), df3 = DATE_FORMAT(d3, '%%Y-%%m-%%d %%T.%%f') |"
                    + " fields d1, d2, d3, df1, df2, df3",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("d1", "timestamp"),
        schema("d2", "timestamp"),
        schema("d3", "timestamp"),
        schema("df1", "string"),
        schema("df2", "string"),
        schema("df3", "string"));

    final String DATETIME_P0_PATTERN = "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}$";
    final String DATETIME_P3_PATTERN = "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{1,3}$";
    final String DATETIME_P6_PATTERN = "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{1,6}$";
    final String DATETIME_P0_FMT_PATTERN = "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.000000$";
    final String DATETIME_P3_FMT_PATTERN =
        "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{3}000$";
    final String DATETIME_P6_FMT_PATTERN = "^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}\\.\\d{6}$";
    verify(
        actual.getJSONArray("datarows").getJSONArray(0),
        Matchers.matchesPattern(DATETIME_P0_PATTERN),
        Matchers.matchesPattern(DATETIME_P3_PATTERN),
        Matchers.matchesPattern(DATETIME_P6_PATTERN),
        Matchers.matchesPattern(DATETIME_P0_FMT_PATTERN),
        Matchers.matchesPattern(DATETIME_P3_FMT_PATTERN),
        Matchers.matchesPattern(DATETIME_P6_FMT_PATTERN));
  }

  /**
   * DAY, DAY_OF_MONTH, DAYOFMONTH, DAY_OF_WEEK, DAYOFWEEK, DAY_OF_YEAR, DAYOFYEAR f.t. ADDDATE,
   * SUBDATE
   */
  @Test
  public void testDayOfAndAddSubDateWithConditions() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where DAY(date) > 11 and DAY_OF_YEAR(date) < 104 | where"
                    + " DAY_OF_WEEK(date) = 5 | eval d1 = DAY_OF_MONTH(ADDDATE(date, 1)), d2 ="
                    + " DAYOFMONTH(SUBDATE(date, 3)) | eval d3 = DAY_OF_WEEK('1984-04-12'), d4 ="
                    + " DAYOFWEEK(ADDDATE(date, INTERVAL 1 DAY)),d5 = DAY_OF_YEAR(date_time) | head"
                    + " 1 | fields d1, d2, d3, d4, d5",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("d1", "integer"),
        schema("d2", "integer"),
        schema("d3", "integer"),
        schema("d4", "integer"),
        schema("d5", "integer"));
    verifyDataRows(actual, rows(13, 9, 5, 6, 103));
  }

  @Test
  public void testDayName() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval d1 = DAYNAME(date), d2 = DAYNAME('1984-04-12'), d3 ="
                    + " DAYNAME(date_time),m1 = MONTHNAME(date), m2 = MONTHNAME('1984-04-12"
                    + " 10:07:42')"
                    + "| fields d1, d2, d3, m1, m2",
                TEST_INDEX_DATE_FORMATS));
  }

  /**
   * DAYNAME, MONTHNAME, LAST_DAY, MAKEDATE
   *
   * <p>DAYNAME(STRING/DATE/TIMESTAMP) -> STRING MONTHNAME(STRING/DATE/TIMESTAMP) -> STRING
   * LAST_DAY(DATE/STRING/TIMESTAMP/TIME) -> DATE (last day of the month as a DATE for a valid
   * argument.) MAKE_DATE(DOUBLE, DOUBLE) -> DATE (Create a date from the year and day of year.)
   */
  @Test
  public void testDayNameAndMonthNameAndMakeDate() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval d1 = DAYNAME(date), d2 = DAYNAME('1984-04-12'), d3 ="
                    + " DAYNAME(date_time),m1 = MONTHNAME(date), m2 = MONTHNAME('1984-04-12"
                    + " 10:07:42'),ld1 = LAST_DAY(date), ld2 = LAST_DAY('1984-04-12'), ld3 ="
                    + " LAST_DAY('1984-04-12 10:07:42'),md1 = MAKEDATE(2020, 1), md2 ="
                    + " MAKEDATE(2020, 366), md3 = MAKEDATE(2020, 367) | eval m3 = MONTHNAME(md2),"
                    + " ld4 = LAST_DAY(md3), ld5 = LAST_DAY(time) | fields d1, d2, d3, m1, m2, m3,"
                    + " ld1, ld2, ld3, ld4, ld5, md1, md2, md3",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("d1", "string"),
        schema("d2", "string"),
        schema("d3", "string"),
        schema("m1", "string"),
        schema("m2", "string"),
        schema("m3", "string"),
        schema("ld1", "date"),
        schema("ld2", "date"),
        schema("ld3", "date"),
        schema("ld4", "date"),
        schema("ld5", "date"),
        schema("md1", "date"),
        schema("md2", "date"),
        schema("md3", "date"));

    final String thu = DayOfWeek.THURSDAY.getDisplayName(TextStyle.FULL, Locale.getDefault());
    final String apr = Month.APRIL.getDisplayName(TextStyle.FULL, Locale.getDefault());
    final String dec = Month.DECEMBER.getDisplayName(TextStyle.FULL, Locale.getDefault());
    LocalDate today = LocalDate.now(ZoneId.systemDefault());
    LocalDate lastDayOfToday =
        LocalDate.of(
            today.getYear(), today.getMonth(), today.getMonth().length(today.isLeapYear()));
    String formattedLastDayOfToday =
        lastDayOfToday.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    verifyDataRows(
        actual,
        rows(
            thu,
            thu,
            thu,
            apr,
            apr,
            dec,
            "1984-04-30",
            "1984-04-30",
            "1984-04-30",
            "2021-01-31",
            formattedLastDayOfToday,
            "2020-01-01",
            "2020-12-31",
            "2021-01-01"));
  }

  /**
   * MAKE_DATE(DOUBLE, DOUBLE) -> DATE (Create a date from the year and day of year.) Returns a
   * date, given year and day-of-year values. dayofyear must be greater than 0 or the result is
   * NULL. The result is also NULL if either argument is NULL. Arguments are rounded to an integer.
   *
   * <p>Limitations: - Zero year interpreted as 2000; - Negative year is not accepted; - day-of-year
   * should be greater than zero; - day-of-year could be greater than 365/366, calculation switches
   * to the next year(s) (see example).
   */
  @Test
  public void testMakeDateWithNullIO() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where firstname = 'Virginia' | eval md1 = MAKEDATE(2020, 1), md2 ="
                    + " MAKEDATE(2020, 366), md3 = MAKEDATE(2020, 367),md4 = MAKEDATE(0, 78), md5 ="
                    + " MAKEDATE(2008, 0), md6 = MAKEDATE(age, 70) | fields md1, md2, md3, md4,"
                    + " md5, md6",
                TEST_INDEX_BANK_WITH_NULL_VALUES));

    verifySchema(
        actual,
        schema("md1", "date"),
        schema("md2", "date"),
        schema("md3", "date"),
        schema("md4", "date"),
        schema("md5", "date"),
        schema("md6", "date"));
    verifyDataRows(
        actual, rows("2020-01-01", "2020-12-31", "2021-01-01", "2000-03-18", null, null));
  }

  /**
   * DATE_FORMAT: (STRING/DATE/TIME/TIMESTAMP) -> STRING formats the date argument using the
   * specifiers in the format argument FROM_DAYS: (Integer/Long) -> DATE from_days(N) returns the
   * date value given the day number N. DATETIME: (TIMESTAMP, STRING) -> TIMESTAMP (TIMESTAMP) ->
   * TIMESTAMP Converts the datetime to a new timezone
   */
  @Test
  public void testDateFormatAndDatetimeAndFromDays() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval d1 = DATE_FORMAT(date, '%%Y-%%m-%%d'), d2 ="
                    + " DATE_FORMAT('1984-04-12', '%%Y-%%b-%%D %%r'),d3 = DATE_FORMAT(date_time,"
                    + " '%%d.%%m.%%y %%l:%%i %%p'), d4 = DATE_FORMAT(time, '%%T'),d5 ="
                    + " DATE_FORMAT('2020-08-26 13:49:00', '%%a %%c %%e %%H %%h %%j %%k %%M %%S %%s"
                    + " %%W %%w %%'),d6 = FROM_DAYS(737000), d9 = DATETIME(date_time, '+08:00'),"
                    + " d10 = DATETIME('1984-04-12 09:07:42', '+00:00')| eval d11 = DATE_FORMAT(d9,"
                    + " '%%U %%X %%V'), d12 = DATE_FORMAT(d10, '%%u %%v %%x'), d13 ="
                    + " DATE_FORMAT(strict_date_time, '%%T.%%f')| fields d1, d2, d3, d4, d5, d6,"
                    + " d9, d10, d11, d12, d13",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("d1", "string"),
        schema("d2", "string"),
        schema("d3", "string"),
        schema("d4", "string"),
        schema("d5", "string"),
        schema("d6", "date"),
        schema("d9", "timestamp"),
        schema("d10", "timestamp"),
        schema("d11", "string"),
        schema("d12", "string"),
        schema("d13", "string"));

    Instant expectedInstant =
        LocalDateTime.parse("1984-04-12T09:07:42").atZone(ZoneOffset.systemDefault()).toInstant();
    LocalDateTime offsetUTC = LocalDateTime.ofInstant(expectedInstant, ZoneOffset.UTC);
    LocalDateTime offsetPlus8 = LocalDateTime.ofInstant(expectedInstant, ZoneId.of("+08:00"));
    String expectedDatetimeAtUTC =
        offsetUTC.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    String expectedDatetimeAtPlus8 =
        offsetPlus8.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

    verifyDataRows(
        actual,
        rows(
            "1984-04-12",
            "1984-Apr-12th 12:00:00 AM",
            "12.04.84 9:07 AM",
            "09:07:42",
            "Wed 08 26 13 01 239 13 August 00 00 Wednesday 3 %",
            "2017-11-02",
            expectedDatetimeAtPlus8,
            expectedDatetimeAtUTC,
            "15 1984 15",
            "15 15 1984",
            "09:07:42.000123"));
  }

  @Test
  public void testDateDiffAndMakeTime() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval d1 = DATEDIFF(date, ADDDATE(date, INTERVAL 1 DAY)), "
                    + "d2 = DATEDIFF(date, SUBDATE(date, INTERVAL 50 DAY)), "
                    + "d3 = DATEDIFF(date, TIME('20:59')), "
                    + "d4 = DATEDIFF(date_time, SUBDATE(date, 1024)), "
                    + "d5 = DATEDIFF(date_time, TIMESTAMP('2020-08-26 13:49:00')), "
                    + "d6 = DATEDIFF(date_time, time), "
                    + "d7 = DATEDIFF(MAKETIME(20, 30, 40), date),"
                    + "d8 = DATEDIFF(time, date_time), "
                    + "d9 = DATEDIFF(TIME('13:20:00'), time),"
                    + "t = MAKETIME(20.2, 49.5, 42.100502)"
                    + "| fields d1, d2, d3, d4, d5, d6, d7, d8, d9, t",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("d1", "long"),
        schema("d2", "long"),
        schema("d3", "long"),
        schema("d4", "long"),
        schema("d5", "long"),
        schema("d6", "long"),
        schema("d7", "long"),
        schema("d8", "long"),
        schema("d9", "long"),
        schema("t", "time"));

    LocalDate today = LocalDate.now(ZoneId.systemDefault());
    long dateDiffWithToday = ChronoUnit.DAYS.between(LocalDate.parse("1984-04-12"), today);
    verifyDataRows(
        actual,
        rows(
            -1,
            50,
            -dateDiffWithToday,
            1024,
            -13285,
            -dateDiffWithToday,
            dateDiffWithToday,
            dateDiffWithToday,
            0,
            "20:50:42.100502"));
  }

  @Test
  public void testTimestampDiffAndTimestampAdd() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval d1 = TIMESTAMPDIFF(DAY, SUBDATE(date_time, INTERVAL 1"
                    + " DAY), date), d2 = TIMESTAMPDIFF(HOUR, date_time, TIMESTAMPADD(DAY, 1,"
                    + " date_time)), d3 = TIMESTAMPDIFF(MINUTE, date, date_time), d4 ="
                    + " TIMESTAMPDIFF(SECOND, date_time, ADDDATE(date_time, INTERVAL 1 HOUR)), d5 ="
                    + " TIMESTAMPDIFF(MINUTE, time, TIME('12:30:00')), d6 = TIMESTAMPDIFF(WEEK,"
                    + " '1999-12-31 00:00:00', TIMESTAMPADD(HOUR, -24, date_time)), d7 ="
                    + " TIMESTAMPDIFF(MONTH, TIMESTAMPADD(YEAR, 5, '1994-12-10 13:49:02'),"
                    + " ADDDATE(date_time, 1)), d8 = TIMESTAMPDIFF(QUARTER, MAKEDATE(2008, 153),"
                    + " date), d9 = TIMESTAMPDIFF(YEAR, date, '2013-06-19 00:00:00'), t ="
                    + " TIMESTAMPADD(MICROSECOND, 1, date_time) | eval d10 ="
                    + " TIMESTAMPDIFF(MICROSECOND, t, date_time) | fields d1, d2, d3, d4, d5, d6,"
                    + " d7, d8, d9, t, d10",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("d1", "long"),
        schema("d2", "long"),
        schema("d3", "long"),
        schema("d4", "long"),
        schema("d5", "long"),
        schema("d6", "long"),
        schema("d7", "long"),
        schema("d8", "long"),
        schema("d9", "long"),
        schema("t", "timestamp"),
        schema("d10", "long"));

    verifyDataRows(
        actual, rows(0, 24, 547, 3600, 202, -820, -187, -96, 29, "1984-04-12 09:07:42.000001", -1));
  }

  @Test
  public void testPeriodAddAndPeriodDiff() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval p1 = PERIOD_ADD(200801, 3), "
                    + "p2 = PERIOD_ADD(199307, -13), "
                    + "p3 = PERIOD_DIFF(200802, 200703), "
                    + "p4 = PERIOD_DIFF(200802, 201003) "
                    + "| fields  p1, p2, p3, p4",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("p1", "integer"),
        schema("p2", "integer"),
        schema("p3", "integer"),
        schema("p4", "integer"));

    verifyDataRows(actual, rows(200804, 199206, 11, -25));
  }

  @Test
  public void testMinuteOfHourAndMinuteOfDay() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval m1 = MINUTE_OF_HOUR(date_time), "
                    + "m2 = MINUTE(time), "
                    + "m3 = MINUTE_OF_DAY(strict_date_time), "
                    + "m4 = MINUTE_OF_DAY(time), "
                    + "m5 = MINUTE('2009-10-19 23:40:27.123456'), "
                    + "m6 = MINUTE_OF_HOUR('16:20:39') "
                    + "| fields m1, m2, m3, m4, m5, m6",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(
        actual,
        schema("m1", "integer"),
        schema("m2", "integer"),
        schema("m3", "integer"),
        schema("m4", "integer"),
        schema("m5", "integer"),
        schema("m6", "integer"));

    verifyDataRows(actual, rows(7, 7, 547, 547, 40, 20));
  }

  @Test
  public void testTimeDiff() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source = %s | head 1 | eval t1 = TIMEDIFF('23:59:59', '13:00:00'),"
                    + "t2 = TIMEDIFF(time, '13:00:00'),"
                    + "t3 = TIMEDIFF(time, time) "
                    + "| fields t1, t2, t3",
                TEST_INDEX_DATE_FORMATS));

    verifySchema(actual, schema("t1", "time"), schema("t2", "time"), schema("t3", "time"));

    verifyDataRows(actual, rows("10:59:59", "20:07:42", "00:00:00"));
  }

  @Test
  public void testQuarter() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| eval `QUARTER(DATE('2020-08-26'))` = QUARTER(DATE('2020-08-26')) "
                    + "| eval quarter2 = QUARTER(basic_date) "
                    + "| eval timestampQuarter2 = QUARTER(basic_date_time) "
                    + "| fields `QUARTER(DATE('2020-08-26'))`, quarter2, timestampQuarter2 "
                    + "| head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("QUARTER(DATE('2020-08-26'))", "integer"),
        schema("quarter2", "integer"),
        schema("timestampQuarter2", "integer"));
    verifyDataRows(actual, rows(3, 2, 2));
  }

  @Test
  public void testSecond() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| eval s = SECOND(TIMESTAMP('01:02:03')) "
                    + "| eval secondForTime = SECOND(basic_time) "
                    + "| eval secondForDate = SECOND(basic_date) "
                    + "| eval secondForTimestamp = SECOND(strict_date_optional_time_nanos) "
                    + "| fields s, secondForTime, secondForDate, secondForTimestamp "
                    + "| head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("s", "integer"),
        schema("secondForTime", "integer"),
        schema("secondForDate", "integer"),
        schema("secondForTimestamp", "integer"));
    verifyDataRows(actual, rows(3, 42, 0, 42));
  }

  @Test
  public void testSecondOfMinute() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval s = second_of_minute(TIMESTAMP('01:02:03')) | eval secondForTime"
                    + " = second_of_minute(basic_time) | eval secondForDate ="
                    + " second_of_minute(basic_date) | eval secondForTimestamp ="
                    + " second_of_minute(strict_date_optional_time_nanos) | fields s,"
                    + " secondForTime, secondForDate, secondForTimestamp | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("s", "integer"),
        schema("secondForTime", "integer"),
        schema("secondForDate", "integer"),
        schema("secondForTimestamp", "integer"));
    verifyDataRows(actual, rows(3, 42, 0, 42));
  }

  @Test
  public void testConvertTz() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 | eval r1 = convert_tz('2008-05-15 12:00:00', '+00:00',"
                    + " '+10:00') | eval r2 = convert_tz(TIMESTAMP('2008-05-15 12:00:00'),"
                    + " '+00:00', '+10:00') | eval r3 = convert_tz(date_time, '+00:00', '+10:00') |"
                    + " eval r4 = convert_tz('2008-05-15 12:00:00', '-00:00', '+00:00') | eval r5 ="
                    + " convert_tz('2008-05-15 12:00:00', '+10:00', '+11:00') | eval r6 ="
                    + " convert_tz('2021-05-12 11:34:50', '-08:00', '+09:00') | eval r7 ="
                    + " convert_tz('2021-05-12 11:34:50', '-12:00', '+12:00') | eval r8 ="
                    + " convert_tz('2021-05-12 13:00:00', '+09:30', '+05:45') | eval r9 ="
                    + " convert_tz(strict_date_time, '+09:00', '+05:00')| fields r1, r2, r3, r4,"
                    + " r5, r6, r7, r8, r9",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("r1", "timestamp"),
        schema("r2", "timestamp"),
        schema("r3", "timestamp"),
        schema("r4", "timestamp"),
        schema("r5", "timestamp"),
        schema("r6", "timestamp"),
        schema("r7", "timestamp"),
        schema("r8", "timestamp"),
        schema("r9", "timestamp"));
    verifyDataRows(
        actual,
        rows(
            "2008-05-15 22:00:00",
            "2008-05-15 22:00:00",
            "1984-04-12 19:07:42",
            "2008-05-15 12:00:00",
            "2008-05-15 13:00:00",
            "2021-05-13 04:34:50",
            "2021-05-13 11:34:50",
            "2021-05-12 09:15:00",
            "1984-04-12 05:07:42.000123456"));
  }

  @Test
  public void testConvertTzWithInvalidResult() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| eval r1 = convert_tz('2021-05-30 11:34:50', '-17:00', '+08:00') "
                    + "| eval r2 = convert_tz('2021-05-12 11:34:50', '-12:00', '+15:00') "
                    + "| eval r3 = convert_tz('2021-05-12 11:34:50', '-12:00', 'test') "
                    + "| fields r1, r2, r3"
                    + "| head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual, schema("r1", "timestamp"), schema("r2", "timestamp"), schema("r3", "timestamp"));
    verifyDataRows(actual, rows(null, null, null));
  }

  @Test
  public void testGetFormat() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s "
                    + "| eval r1 = GET_FORMAT(DATE, 'USA') "
                    + "| eval r2 = GET_FORMAT(TIME, 'INTERNAL') "
                    + "| eval r3 = GET_FORMAT(TIMESTAMP, 'EUR') "
                    + "| eval r4 = GET_FORMAT(TIMESTAMP, 'UTC') "
                    + "| fields r1, r2, r3, r4"
                    + "| head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("r1", "string"),
        schema("r2", "string"),
        schema("r3", "string"),
        schema("r4", "string"));
    verifyDataRows(actual, rows("%m.%d.%Y", "%H%i%s", "%Y-%m-%d %H.%i.%s", null));
  }

  @Test
  public void testExtractWithSimpleFormats() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval r1 = extract(YEAR FROM '1997-01-01 00:00:00') | eval r2 ="
                    + " extract(YEAR FROM strict_date_optional_time_nanos) | eval r3 = extract(year"
                    + " FROM basic_date) | eval r4 = extract(QUARTER FROM"
                    + " strict_date_optional_time_nanos) | eval r5 = extract(quarter FROM"
                    + " basic_date) | eval r6 = extract(MONTH FROM strict_date_optional_time_nanos)"
                    + " | eval r7 = extract(month FROM basic_date) | eval r8 = extract(WEEK FROM"
                    + " strict_date_optional_time_nanos) | eval r9 = extract(week FROM basic_date)"
                    + " | eval r10 = extract(DAY FROM strict_date_optional_time_nanos) | eval r11 ="
                    + " extract(day FROM basic_date) | eval r12 = extract(HOUR FROM"
                    + " strict_date_optional_time_nanos) | eval r13 = extract(hour FROM basic_time)"
                    + " | eval r14 = extract(MINUTE FROM strict_date_optional_time_nanos) | eval"
                    + " r15 = extract(minute FROM basic_time) | eval r16 = extract(SECOND FROM"
                    + " strict_date_optional_time_nanos) | eval r17 = extract(second FROM"
                    + " basic_time) | eval r19 ="
                    + " extract(day FROM '1984-04-12') | eval r20 = extract(MICROSECOND FROM"
                    + " timestamp('1984-04-12 09:07:42.123456789')) | fields r1, r2, r3, r4, r5,"
                    + " r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r19, r20 |"
                    + " head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("r1", "long"),
        schema("r2", "long"),
        schema("r3", "long"),
        schema("r4", "long"),
        schema("r5", "long"),
        schema("r6", "long"),
        schema("r7", "long"),
        schema("r8", "long"),
        schema("r9", "long"),
        schema("r10", "long"),
        schema("r11", "long"),
        schema("r12", "long"),
        schema("r13", "long"),
        schema("r14", "long"),
        schema("r15", "long"),
        schema("r16", "long"),
        schema("r17", "long"),
        schema("r19", "long"),
        schema("r20", "long"));
    verifyDataRows(
        actual, rows(1997, 1984, 1984, 2, 2, 4, 4, 15, 15, 12, 12, 9, 9, 7, 7, 42, 42, 12, 123456));
  }

  @Test
  public void testTpchQueryDate() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | where strict_date <= subdate(date('1998-12-01'), 90) | stats COUNT()",
                TEST_INDEX_DATE_FORMATS));
    verifyDataRows(actual, rows(2));
  }

  @Test
  public void testExtractWithComplexFormats() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | eval r1 = extract(YEAR_MONTH FROM '1997-01-01 00:00:00') | eval r2 ="
                    + " extract(DAY_HOUR FROM strict_date_optional_time_nanos) | eval r3 ="
                    + " extract(DAY_HOUR FROM basic_date) | eval r4 = extract(DAY_MINUTE FROM"
                    + " strict_date_optional_time_nanos) | eval r5 = extract(DAY_MINUTE FROM"
                    + " basic_date) | eval r6 = extract(DAY_SECOND FROM"
                    + " strict_date_optional_time_nanos) | eval r7 = extract(DAY_SECOND FROM"
                    + " basic_date) | eval r8 = extract(HOUR_MINUTE FROM"
                    + " strict_date_optional_time_nanos) | eval r9 = extract(HOUR_MINUTE FROM"
                    + " basic_time) | eval r10 = extract(HOUR_SECOND FROM"
                    + " strict_date_optional_time_nanos) | eval r11 = extract(HOUR_SECOND FROM"
                    + " basic_time) | eval r12 = extract(MINUTE_SECOND FROM"
                    + " strict_date_optional_time_nanos) | eval r13 = extract(MINUTE_SECOND FROM"
                    + " basic_time) | eval r14 = extract(DAY_MICROSECOND FROM"
                    + " strict_date_optional_time_nanos) | eval r15 = extract(HOUR_MICROSECOND FROM"
                    + " strict_date_optional_time_nanos) | eval r16 = extract(MINUTE_MICROSECOND"
                    + " FROM strict_date_optional_time_nanos) | eval r17 ="
                    + " extract(SECOND_MICROSECOND FROM strict_date_optional_time_nanos) | eval r18"
                    + " = extract(MICROSECOND FROM strict_date_optional_time_nanos) | fields r1,"
                    + " r2, r3, r4, r5, r6, r7, r8, r9, r10, r11, r12, r13, r14, r15, r16, r17, r18"
                    + " | head 1",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("r1", "long"),
        schema("r2", "long"),
        schema("r3", "long"),
        schema("r4", "long"),
        schema("r5", "long"),
        schema("r6", "long"),
        schema("r7", "long"),
        schema("r8", "long"),
        schema("r9", "long"),
        schema("r10", "long"),
        schema("r11", "long"),
        schema("r12", "long"),
        schema("r13", "long"),
        schema("r14", "long"),
        schema("r15", "long"),
        schema("r16", "long"),
        schema("r17", "long"),
        schema("r18", "long"));
    verifyDataRows(
        actual,
        rows(
            199701,
            1209,
            1200,
            120907,
            120000,
            12090742,
            12000000,
            907,
            907,
            90742,
            90742,
            742,
            742,
            12090742000123L,
            90742000123L,
            742000123,
            42000123,
            123));
  }

  @Test
  public void testMicrosecond() {
    JSONObject actual =
        executeQuery(
            String.format(
                "source=%s | head 1 |  eval m1 = MICROSECOND(date_time), m2 = MICROSECOND(time), m3"
                    + " = MICROSECOND(date), m4 = MICROSECOND('13:45:22.123456789'), m5 ="
                    + " MICROSECOND('2012-09-13 13:45:22.123456789')| fields m1, m2, m3, m4, m5",
                TEST_INDEX_DATE_FORMATS));
    verifySchema(
        actual,
        schema("m1", "integer"),
        schema("m2", "integer"),
        schema("m3", "integer"),
        schema("m4", "integer"),
        schema("m5", "integer"));
    verifyDataRows(actual, rows(0, 0, 0, 123456, 123456));
  }
}
