/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static java.time.DayOfWeek.SUNDAY;
import static java.time.temporal.TemporalAdjusters.nextOrSame;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.model.ExprValueUtils.longValue;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;
import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import com.google.common.collect.ImmutableList;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;

class DateTimeFunctionTest extends ExpressionTestBase {

  final List<DateFormatTester> dateFormatTesters =
      ImmutableList.of(
          new DateFormatTester(
              "1998-01-31 13:14:15.012345",
              ImmutableList.of(
                  "%H",
                  "%I", "%k", "%l", "%i", "%p", "%r", "%S", "%T", " %M", "%W", "%D", "%Y", "%y",
                  "%a", "%b", "%j", "%m", "%d", "%h", "%s", "%w", "%f", "%q", "%"),
              ImmutableList.of(
                  "13",
                  "01",
                  "13",
                  "1",
                  "14",
                  "PM",
                  "01:14:15 PM",
                  "15",
                  "13:14:15",
                  " January",
                  "Saturday",
                  "31st",
                  "1998",
                  "98",
                  "Sat",
                  "Jan",
                  "031",
                  "01",
                  "31",
                  "01",
                  "15",
                  "6",
                  "012345",
                  "q",
                  "%")),
          new DateFormatTester("1999-12-01", ImmutableList.of("%D"), ImmutableList.of("1st")),
          new DateFormatTester("1999-12-02", ImmutableList.of("%D"), ImmutableList.of("2nd")),
          new DateFormatTester("1999-12-03", ImmutableList.of("%D"), ImmutableList.of("3rd")),
          new DateFormatTester("1999-12-04", ImmutableList.of("%D"), ImmutableList.of("4th")),
          new DateFormatTester("1999-12-11", ImmutableList.of("%D"), ImmutableList.of("11th")),
          new DateFormatTester("1999-12-12", ImmutableList.of("%D"), ImmutableList.of("12th")),
          new DateFormatTester("1999-12-13", ImmutableList.of("%D"), ImmutableList.of("13th")),
          new DateFormatTester(
              "1999-12-31",
              ImmutableList.of("%x", "%v", "%X", "%V", "%u", "%U"),
              ImmutableList.of("1999", "52", "1999", "52", "52", "52")),
          new DateFormatTester(
              "2000-01-01",
              ImmutableList.of("%x", "%v", "%X", "%V", "%u", "%U"),
              ImmutableList.of("1999", "52", "1999", "52", "0", "0")),
          new DateFormatTester(
              "1998-12-31",
              ImmutableList.of("%x", "%v", "%X", "%V", "%u", "%U"),
              ImmutableList.of("1998", "52", "1998", "52", "52", "52")),
          new DateFormatTester(
              "1999-01-01",
              ImmutableList.of("%x", "%v", "%X", "%V", "%u", "%U"),
              ImmutableList.of("1998", "52", "1998", "52", "0", "0")),
          new DateFormatTester(
              "2020-01-04", ImmutableList.of("%x", "%X"), ImmutableList.of("2020", "2019")),
          new DateFormatTester(
              "2008-12-31",
              ImmutableList.of("%v", "%V", "%u", "%U"),
              ImmutableList.of("53", "52", "53", "52")),
          new DateFormatTester(
              "1998-01-31 13:14:15.012345",
              ImmutableList.of("%Y-%m-%dT%TZ"),
              ImmutableList.of("1998-01-31T13:14:15Z")),
          new DateFormatTester(
              "1998-01-31 13:14:15.012345",
              ImmutableList.of("%Y-%m-%da %T a"),
              ImmutableList.of("1998-01-31PM 13:14:15 PM")),
          new DateFormatTester(
              "1998-01-31 13:14:15.012345",
              ImmutableList.of("%Y-%m-%db %T b"),
              ImmutableList.of("1998-01-31b 13:14:15 b")));

  @AllArgsConstructor
  private class DateFormatTester {
    private final String date;
    private final List<String> formatterList;
    private final List<String> formattedList;
    private static final String DELIMITER = "|";

    String getFormatter() {
      return String.join(DELIMITER, formatterList);
    }

    String getFormatted() {
      return String.join(DELIMITER, formattedList);
    }

    FunctionExpression getDateFormatExpression() {
      return DSL.date_format(functionProperties, DSL.literal(date), DSL.literal(getFormatter()));
    }
  }

  @Test
  public void date() {
    FunctionExpression expr = DSL.date(DSL.literal("2020-08-17"));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-17"), eval(expr));
    assertEquals("date(\"2020-08-17\")", expr.toString());

    expr = DSL.date(DSL.literal(new ExprDateValue("2020-08-17")));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-17"), eval(expr));
    assertEquals("date(DATE '2020-08-17')", expr.toString());

    expr = DSL.date(DSL.literal(new ExprDateValue("2020-08-17 12:12:00")));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-17 12:12:00"), eval(expr));
    assertEquals("date(DATE '2020-08-17')", expr.toString());

    expr = DSL.date(DSL.literal(new ExprDateValue("2020-08-17 12:12")));
    assertEquals(DATE, expr.type());
    assertEquals(new ExprDateValue("2020-08-17 12:12"), eval(expr));
    assertEquals("date(DATE '2020-08-17')", expr.toString());
  }

  @Test
  public void day() {
    FunctionExpression expression = DSL.day(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("day(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(7), eval(expression));

    expression = DSL.day(DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("day(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(7), eval(expression));
  }

  @Test
  public void dayName() {
    FunctionExpression expression = DSL.dayname(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(STRING, expression.type());
    assertEquals("dayname(DATE '2020-08-07')", expression.toString());
    assertEquals(stringValue("Friday"), eval(expression));

    expression = DSL.dayname(DSL.literal("2020-08-07"));
    assertEquals(STRING, expression.type());
    assertEquals("dayname(\"2020-08-07\")", expression.toString());
    assertEquals(stringValue("Friday"), eval(expression));
  }

  @Test
  public void dayOfMonth() {
    FunctionExpression expression =
        DSL.dayofmonth(functionProperties, DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofmonth(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(7), eval(expression));

    expression = DSL.dayofmonth(functionProperties, DSL.literal("2020-07-08"));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofmonth(\"2020-07-08\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));
  }

  private void testDayOfMonthWithUnderscores(FunctionExpression dateExpression, int dayOfMonth) {
    assertEquals(INTEGER, dateExpression.type());
    assertEquals(integerValue(dayOfMonth), eval(dateExpression));
  }

  @Test
  public void dayOfMonthWithUnderscores() {
    FunctionExpression expression1 =
        DSL.dayofmonth(functionProperties, DSL.literal(new ExprDateValue("2020-08-07")));
    FunctionExpression expression2 = DSL.dayofmonth(functionProperties, DSL.literal("2020-07-08"));

    assertAll(
        () -> testDayOfMonthWithUnderscores(expression1, 7),
        () -> assertEquals("dayofmonth(DATE '2020-08-07')", expression1.toString()),
        () -> testDayOfMonthWithUnderscores(expression2, 8),
        () -> assertEquals("dayofmonth(\"2020-07-08\")", expression2.toString()));
  }

  @Test
  public void testDayOfMonthWithTimeType() {
    FunctionExpression expression =
        DSL.day_of_month(functionProperties, DSL.literal(new ExprTimeValue("12:23:34")));

    assertEquals(INTEGER, eval(expression).type());
    assertEquals(
        LocalDate.now(functionProperties.getQueryStartClock()).getDayOfMonth(),
        eval(expression).integerValue());
    assertEquals("day_of_month(TIME '12:23:34')", expression.toString());
  }

  private void testInvalidDayOfMonth(String date) {
    FunctionExpression expression =
        DSL.day_of_month(functionProperties, DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test
  public void dayOfMonthWithUnderscoresLeapYear() {
    // Feb. 29 of a leap year
    testDayOfMonthWithUnderscores(
        DSL.day_of_month(functionProperties, DSL.literal("2020-02-29")), 29);

    // Feb. 29 of a non-leap year
    assertThrows(SemanticCheckException.class, () -> testInvalidDayOfMonth("2021-02-29"));
  }

  @Test
  public void dayOfMonthWithUnderscoresInvalidArguments() {
    assertAll(
        // 40th day of the month
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidDayOfMonth("2021-02-40")),
        // 13th month of the year
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidDayOfMonth("2021-13-40")),
        // incorrect format
        () ->
            assertThrows(
                SemanticCheckException.class, () -> testInvalidDayOfMonth("asdfasdfasdf")));
  }

  private void dayOfWeekQuery(FunctionExpression dateExpression, int dayOfWeek, String testExpr) {
    assertEquals(INTEGER, dateExpression.type());
    assertEquals(integerValue(dayOfWeek), eval(dateExpression));
    assertEquals(testExpr, dateExpression.toString());
  }

  @Test
  public void dayOfWeek() {
    FunctionExpression expression1 =
        DSL.dayofweek(functionProperties, DSL.literal(new ExprDateValue("2020-08-07")));
    FunctionExpression expression2 =
        DSL.dayofweek(functionProperties, DSL.literal(new ExprDateValue("2020-08-09")));
    FunctionExpression expression3 = DSL.dayofweek(functionProperties, DSL.literal("2020-08-09"));
    FunctionExpression expression4 =
        DSL.dayofweek(functionProperties, DSL.literal("2020-08-09 01:02:03"));

    assertAll(
        () -> dayOfWeekQuery(expression1, 6, "dayofweek(DATE '2020-08-07')"),
        () -> dayOfWeekQuery(expression2, 1, "dayofweek(DATE '2020-08-09')"),
        () -> dayOfWeekQuery(expression3, 1, "dayofweek(\"2020-08-09\")"),
        () -> dayOfWeekQuery(expression4, 1, "dayofweek(\"2020-08-09 01:02:03\")"));
  }

  private void dayOfWeekWithUnderscoresQuery(
      FunctionExpression dateExpression, int dayOfWeek, String testExpr) {
    assertEquals(INTEGER, dateExpression.type());
    assertEquals(integerValue(dayOfWeek), eval(dateExpression));
    assertEquals(testExpr, dateExpression.toString());
  }

  @Test
  public void dayOfWeekWithUnderscores() {
    FunctionExpression expression1 =
        DSL.day_of_week(functionProperties, DSL.literal(new ExprDateValue("2020-08-07")));
    FunctionExpression expression2 =
        DSL.day_of_week(functionProperties, DSL.literal(new ExprDateValue("2020-08-09")));
    FunctionExpression expression3 = DSL.day_of_week(functionProperties, DSL.literal("2020-08-09"));
    FunctionExpression expression4 =
        DSL.day_of_week(functionProperties, DSL.literal("2020-08-09 01:02:03"));

    assertAll(
        () -> dayOfWeekWithUnderscoresQuery(expression1, 6, "day_of_week(DATE '2020-08-07')"),
        () -> dayOfWeekWithUnderscoresQuery(expression2, 1, "day_of_week(DATE '2020-08-09')"),
        () -> dayOfWeekWithUnderscoresQuery(expression3, 1, "day_of_week(\"2020-08-09\")"),
        () ->
            dayOfWeekWithUnderscoresQuery(expression4, 1, "day_of_week(\"2020-08-09 01:02:03\")"));
  }

  @Test
  public void testDayOfWeekWithTimeType() {
    FunctionExpression expression =
        DSL.day_of_week(functionProperties, DSL.literal(new ExprTimeValue("12:23:34")));

    assertAll(
        () -> assertEquals(INTEGER, eval(expression).type()),
        () ->
            assertEquals(
                (LocalDate.now(functionProperties.getQueryStartClock()).getDayOfWeek().getValue()
                        % 7)
                    + 1,
                eval(expression).integerValue()),
        () -> assertEquals("day_of_week(TIME '12:23:34')", expression.toString()));
  }

  private void testInvalidDayOfWeek(String date) {
    FunctionExpression expression =
        DSL.day_of_week(functionProperties, DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test
  public void dayOfWeekWithUnderscoresLeapYear() {
    assertAll(
        // Feb. 29 of a leap year
        () ->
            dayOfWeekWithUnderscoresQuery(
                DSL.day_of_week(functionProperties, DSL.literal("2020-02-29")),
                7,
                "day_of_week(\"2020-02-29\")"),
        // day after Feb. 29 of a leap year
        () ->
            dayOfWeekWithUnderscoresQuery(
                DSL.day_of_week(functionProperties, DSL.literal("2020-03-01")),
                1,
                "day_of_week(\"2020-03-01\")"),
        // Feb. 28 of a non-leap year
        () ->
            dayOfWeekWithUnderscoresQuery(
                DSL.day_of_week(functionProperties, DSL.literal("2021-02-28")),
                1,
                "day_of_week(\"2021-02-28\")"),
        // Feb. 29 of a non-leap year
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidDayOfWeek("2021-02-29")));
  }

  @Test
  public void dayOfWeekWithUnderscoresInvalidArgument() {
    assertAll(
        // 40th day of the month
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidDayOfWeek("2021-02-40")),

        // 13th month of the year
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidDayOfWeek("2021-13-29")),

        // incorrect format
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidDayOfWeek("asdfasdf")));
  }

  @Test
  public void dayOfYear() {
    FunctionExpression expression = DSL.dayofyear(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofyear(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(220), eval(expression));

    expression = DSL.dayofyear(DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofyear(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(220), eval(expression));

    expression = DSL.dayofyear(DSL.literal("2020-08-07 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals("dayofyear(\"2020-08-07 01:02:03\")", expression.toString());
    assertEquals(integerValue(220), eval(expression));
  }

  private static Stream<Arguments> getTestDataForDayOfYear() {
    return Stream.of(
        Arguments.of(
            DSL.literal(new ExprDateValue("2020-08-07")), "day_of_year(DATE '2020-08-07')", 220),
        Arguments.of(
            DSL.literal(new ExprTimestampValue("2020-08-07 12:23:34")),
            "day_of_year(TIMESTAMP '2020-08-07 12:23:34')",
            220),
        Arguments.of(DSL.literal("2020-08-07"), "day_of_year(\"2020-08-07\")", 220),
        Arguments.of(
            DSL.literal("2020-08-07 01:02:03"), "day_of_year(\"2020-08-07 01:02:03\")", 220));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getTestDataForDayOfYear")
  public void dayOfYearWithUnderscores(
      LiteralExpression arg, String expectedString, int expectedResult) {
    validateStringFormat(DSL.day_of_year(functionProperties, arg), expectedString, expectedResult);
  }

  @Test
  public void testDayOfYearWithTimeType() {
    validateStringFormat(
        DSL.day_of_year(functionProperties, DSL.literal(new ExprTimeValue("12:23:34"))),
        "day_of_year(TIME '12:23:34')",
        LocalDate.now(functionProperties.getQueryStartClock()).getDayOfYear());
  }

  public void dayOfYearWithUnderscoresQuery(String date, int dayOfYear) {
    FunctionExpression expression =
        DSL.day_of_year(functionProperties, DSL.literal(new ExprDateValue(date)));
    assertAll(
        () -> assertEquals(INTEGER, expression.type()),
        () -> assertEquals(integerValue(dayOfYear), eval(expression)));
  }

  @Test
  public void dayOfYearWithUnderscoresDifferentArgumentFormats() {
    FunctionExpression expression1 =
        DSL.day_of_year(functionProperties, DSL.literal(new ExprDateValue("2020-08-07")));
    FunctionExpression expression2 = DSL.day_of_year(functionProperties, DSL.literal("2020-08-07"));
    FunctionExpression expression3 =
        DSL.day_of_year(functionProperties, DSL.literal("2020-08-07 01:02:03"));

    assertAll(
        () -> dayOfYearWithUnderscoresQuery("2020-08-07", 220),
        () -> assertEquals("day_of_year(DATE '2020-08-07')", expression1.toString()),
        () -> dayOfYearWithUnderscoresQuery("2020-08-07", 220),
        () -> assertEquals("day_of_year(\"2020-08-07\")", expression2.toString()),
        () -> dayOfYearWithUnderscoresQuery("2020-08-07 01:02:03", 220),
        () -> assertEquals("day_of_year(\"2020-08-07 01:02:03\")", expression3.toString()));
  }

  @Test
  public void dayOfYearWithUnderscoresCornerCaseDates() {
    assertAll(
        // 31st of December during non leap year (should be 365)
        () -> dayOfYearWithUnderscoresQuery("2019-12-31", 365),
        // Year 1200
        () -> dayOfYearWithUnderscoresQuery("1200-02-28", 59),
        // Year 4000
        () -> dayOfYearWithUnderscoresQuery("4000-02-28", 59));
  }

  @Test
  public void dayOfYearWithUnderscoresLeapYear() {
    assertAll(
        // 28th of Feb
        () -> dayOfYearWithUnderscoresQuery("2020-02-28", 59),

        // 29th of Feb during leap year
        () -> dayOfYearWithUnderscoresQuery("2020-02-29 23:59:59", 60),
        () -> dayOfYearWithUnderscoresQuery("2020-02-29", 60),

        // 1st of March during leap year
        () -> dayOfYearWithUnderscoresQuery("2020-03-01 00:00:00", 61),
        () -> dayOfYearWithUnderscoresQuery("2020-03-01", 61),

        // 1st of March during non leap year
        () -> dayOfYearWithUnderscoresQuery("2019-03-01", 60),

        // 31st of December during  leap year (should be 366)
        () -> dayOfYearWithUnderscoresQuery("2020-12-31", 366));
  }

  private void invalidDayOfYearQuery(String date) {
    FunctionExpression expression =
        DSL.day_of_year(functionProperties, DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test
  public void invalidDayOfYearArgument() {
    assertAll(
        // 29th of Feb non-leapyear
        () -> assertThrows(SemanticCheckException.class, () -> invalidDayOfYearQuery("2019-02-29")),

        // 13th month
        () -> assertThrows(SemanticCheckException.class, () -> invalidDayOfYearQuery("2019-13-15")),

        // incorrect format for type
        () ->
            assertThrows(
                SemanticCheckException.class, () -> invalidDayOfYearQuery("asdfasdfasdf")));
  }

  @Test
  public void from_days() {
    FunctionExpression expression = DSL.from_days(DSL.literal(new ExprLongValue(730669)));
    assertEquals(DATE, expression.type());
    assertEquals("from_days(730669)", expression.toString());
    assertEquals(new ExprDateValue("2000-07-03"), expression.valueOf());
  }

  private static Stream<Arguments> getTestDataForGetFormat() {
    return Stream.of(
        Arguments.of("DATE", "USA", "%m.%d.%Y"),
        Arguments.of("DATE", "JIS", "%Y-%m-%d"),
        Arguments.of("DATE", "ISO", "%Y-%m-%d"),
        Arguments.of("DATE", "EUR", "%d.%m.%Y"),
        Arguments.of("DATE", "INTERNAL", "%Y%m%d"),
        Arguments.of("TIME", "USA", "%h:%i:%s %p"),
        Arguments.of("TIME", "JIS", "%H:%i:%s"),
        Arguments.of("TIME", "ISO", "%H:%i:%s"),
        Arguments.of("TIME", "EUR", "%H.%i.%s"),
        Arguments.of("TIME", "INTERNAL", "%H%i%s"),
        Arguments.of("TIMESTAMP", "USA", "%Y-%m-%d %H.%i.%s"),
        Arguments.of("TIMESTAMP", "JIS", "%Y-%m-%d %H:%i:%s"),
        Arguments.of("TIMESTAMP", "ISO", "%Y-%m-%d %H:%i:%s"),
        Arguments.of("TIMESTAMP", "EUR", "%Y-%m-%d %H.%i.%s"),
        Arguments.of("TIMESTAMP", "INTERNAL", "%Y%m%d%H%i%s"));
  }

  private void getFormatQuery(
      LiteralExpression argType, LiteralExpression namedFormat, String expectedResult) {
    FunctionExpression expr = DSL.get_format(argType, namedFormat);
    assertEquals(STRING, expr.type());
    assertEquals(expectedResult, eval(expr).stringValue());
  }

  @ParameterizedTest(name = "{0}{1}")
  @MethodSource("getTestDataForGetFormat")
  public void testGetFormat(String arg, String format, String expectedResult) {
    getFormatQuery(DSL.literal(arg), DSL.literal(new ExprStringValue(format)), expectedResult);
  }

  @Test
  public void testGetFormatInvalidFormat() {
    FunctionExpression expr = DSL.get_format(DSL.literal("DATE"), DSL.literal("1SA"));
    assertEquals(nullValue(), eval(expr));
  }

  @Test
  public void hour() {
    FunctionExpression expression = DSL.hour(DSL.literal(new ExprTimeValue("01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(1), eval(expression));
    assertEquals("hour(TIME '01:02:03')", expression.toString());

    expression = DSL.hour(DSL.literal("01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(1), eval(expression));
    assertEquals("hour(\"01:02:03\")", expression.toString());

    expression = DSL.hour(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(1), expression.valueOf());
    assertEquals("hour(TIMESTAMP '2020-08-17 01:02:03')", expression.toString());

    expression = DSL.hour(DSL.literal("2020-08-17 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(1), expression.valueOf());
    assertEquals("hour(\"2020-08-17 01:02:03\")", expression.toString());
  }

  private void testInvalidMinuteOfDay(String date) {
    FunctionExpression expression = DSL.minute_of_day(DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test
  public void invalidMinuteOfDay() {
    assertThrows(
        SemanticCheckException.class, () -> testInvalidMinuteOfDay("2022-12-14 12:23:3400"));
    assertThrows(
        SemanticCheckException.class, () -> testInvalidMinuteOfDay("2022-12-14 12:2300:34"));
    assertThrows(
        SemanticCheckException.class, () -> testInvalidMinuteOfDay("2022-12-14 1200:23:34"));
    assertThrows(
        SemanticCheckException.class, () -> testInvalidMinuteOfDay("2022-12-1400 12:23:34"));
    assertThrows(
        SemanticCheckException.class, () -> testInvalidMinuteOfDay("2022-1200-14 12:23:34"));
    assertThrows(SemanticCheckException.class, () -> testInvalidMinuteOfDay("12:23:3400"));
    assertThrows(SemanticCheckException.class, () -> testInvalidMinuteOfDay("12:2300:34"));
    assertThrows(SemanticCheckException.class, () -> testInvalidMinuteOfDay("1200:23:34"));
    assertThrows(SemanticCheckException.class, () -> testInvalidMinuteOfDay("asdfasdfasdf"));
  }

  private void hourOfDayQuery(FunctionExpression dateExpression, int hour) {
    assertEquals(INTEGER, dateExpression.type());
    assertEquals(integerValue(hour), eval(dateExpression));
  }

  @Test
  public void hourOfDay() {
    FunctionExpression expression1 = DSL.hour_of_day(DSL.literal(new ExprTimeValue("01:02:03")));
    FunctionExpression expression2 = DSL.hour_of_day(DSL.literal("01:02:03"));
    FunctionExpression expression3 =
        DSL.hour_of_day(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")));
    FunctionExpression expression4 = DSL.hour_of_day(DSL.literal("2020-08-17 01:02:03"));

    assertAll(
        () -> hourOfDayQuery(expression1, 1),
        () -> assertEquals("hour_of_day(TIME '01:02:03')", expression1.toString()),
        () -> hourOfDayQuery(expression2, 1),
        () -> assertEquals("hour_of_day(\"01:02:03\")", expression2.toString()),
        () -> hourOfDayQuery(expression3, 1),
        () -> assertEquals("hour_of_day(TIMESTAMP '2020-08-17 01:02:03')", expression3.toString()),
        () -> hourOfDayQuery(expression4, 1),
        () -> assertEquals("hour_of_day(\"2020-08-17 01:02:03\")", expression4.toString()));
  }

  private void invalidHourOfDayQuery(String time) {
    FunctionExpression expression = DSL.hour_of_day(DSL.literal(new ExprTimeValue(time)));
    eval(expression);
  }

  @Test
  public void hourOfDayInvalidArguments() {
    assertAll(
        // Invalid Seconds
        () -> assertThrows(SemanticCheckException.class, () -> invalidHourOfDayQuery("12:23:61")),
        // Invalid Minutes
        () -> assertThrows(SemanticCheckException.class, () -> invalidHourOfDayQuery("12:61:34")),

        // Invalid Hours
        () -> assertThrows(SemanticCheckException.class, () -> invalidHourOfDayQuery("25:23:34")),

        // incorrect format
        () -> assertThrows(SemanticCheckException.class, () -> invalidHourOfDayQuery("asdfasdf")));
  }

  private void checkForExpectedDay(
      FunctionExpression functionExpression, String expectedDay, String testExpr) {
    assertEquals(DATE, functionExpression.type());
    assertEquals(new ExprDateValue(expectedDay), eval(functionExpression));
    assertEquals(testExpr, functionExpression.toString());
  }

  private static Stream<Arguments> getTestDataForLastDay() {
    return Stream.of(
        Arguments.of(new ExprDateValue("2017-01-20"), "2017-01-31", "last_day(DATE '2017-01-20')"),
        // Leap year
        Arguments.of(new ExprDateValue("2020-02-20"), "2020-02-29", "last_day(DATE '2020-02-20')"),
        // Non leap year
        Arguments.of(new ExprDateValue("2017-02-20"), "2017-02-28", "last_day(DATE '2017-02-20')"),
        Arguments.of(new ExprDateValue("2017-03-20"), "2017-03-31", "last_day(DATE '2017-03-20')"),
        Arguments.of(new ExprDateValue("2017-04-20"), "2017-04-30", "last_day(DATE '2017-04-20')"),
        Arguments.of(new ExprDateValue("2017-05-20"), "2017-05-31", "last_day(DATE '2017-05-20')"),
        Arguments.of(new ExprDateValue("2017-06-20"), "2017-06-30", "last_day(DATE '2017-06-20')"),
        Arguments.of(new ExprDateValue("2017-07-20"), "2017-07-31", "last_day(DATE '2017-07-20')"),
        Arguments.of(new ExprDateValue("2017-08-20"), "2017-08-31", "last_day(DATE '2017-08-20')"),
        Arguments.of(new ExprDateValue("2017-09-20"), "2017-09-30", "last_day(DATE '2017-09-20')"),
        Arguments.of(new ExprDateValue("2017-10-20"), "2017-10-31", "last_day(DATE '2017-10-20')"),
        Arguments.of(new ExprDateValue("2017-11-20"), "2017-11-30", "last_day(DATE '2017-11-20')"),
        Arguments.of(new ExprDateValue("2017-12-20"), "2017-12-31", "last_day(DATE '2017-12-20')"));
  }

  @ParameterizedTest(name = "{2}")
  @MethodSource("getTestDataForLastDay")
  public void testLastDay(ExprValue testedDateTime, String expectedResult, String expectedQuery) {
    checkForExpectedDay(
        DSL.last_day(functionProperties, DSL.literal(testedDateTime)),
        expectedResult,
        expectedQuery);
  }

  @Test
  public void testLastDayWithTimeType() {
    FunctionExpression expression =
        DSL.last_day(functionProperties, DSL.literal(new ExprTimeValue("12:23:34")));

    LocalDate expected = LocalDate.now(functionProperties.getQueryStartClock());
    LocalDate result = eval(expression).dateValue();

    assertAll(
        () -> assertEquals((expected.lengthOfMonth()), result.getDayOfMonth()),
        () -> assertEquals("last_day(TIME '12:23:34')", expression.toString()));
  }

  private void lastDay(String date) {
    FunctionExpression expression =
        DSL.day_of_week(functionProperties, DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test
  public void testLastDayInvalidArgument() {
    assertThrows(SemanticCheckException.class, () -> lastDay("asdfasdf"));
  }

  @Test
  public void microsecond() {
    FunctionExpression expression =
        DSL.microsecond(DSL.literal(new ExprTimeValue("01:02:03.123456")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(123456), eval(expression));
    assertEquals("microsecond(TIME '01:02:03.123456')", expression.toString());

    expression = DSL.microsecond(DSL.literal(new ExprTimeValue("01:02:03.00")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(0), eval(expression));
    assertEquals("microsecond(TIME '01:02:03')", expression.toString());

    expression = DSL.microsecond(DSL.literal("01:02:03.12"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(120000), eval(expression));
    assertEquals("microsecond(\"01:02:03.12\")", expression.toString());

    expression = DSL.microsecond(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03.123456")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(123456), expression.valueOf());
    assertEquals("microsecond(TIMESTAMP '2020-08-17 01:02:03.123456')", expression.toString());

    expression = DSL.microsecond(DSL.literal("2020-08-17 01:02:03.123456"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(123456), expression.valueOf());
    assertEquals("microsecond(\"2020-08-17 01:02:03.123456\")", expression.toString());

    expression = DSL.microsecond(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03.000010")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(10), expression.valueOf());
    assertEquals("microsecond(TIMESTAMP '2020-08-17 01:02:03.00001')", expression.toString());
  }

  @Test
  public void minute() {
    FunctionExpression expression = DSL.minute(DSL.literal(new ExprTimeValue("01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(2), eval(expression));
    assertEquals("minute(TIME '01:02:03')", expression.toString());

    expression = DSL.minute(DSL.literal("01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(2), eval(expression));
    assertEquals("minute(\"01:02:03\")", expression.toString());

    expression = DSL.minute(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(2), expression.valueOf());
    assertEquals("minute(TIMESTAMP '2020-08-17 01:02:03')", expression.toString());

    expression = DSL.minute(DSL.literal("2020-08-17 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(2), expression.valueOf());
    assertEquals("minute(\"2020-08-17 01:02:03\")", expression.toString());
  }

  private void testMinuteOfDay(String date, int value) {
    FunctionExpression expression = DSL.minute_of_day(DSL.literal(new ExprTimeValue(date)));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(value), eval(expression));
  }

  @Test
  public void minuteOfDay() {
    FunctionExpression expression = DSL.minute_of_day(DSL.literal(new ExprTimeValue("01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(62), eval(expression));
    assertEquals("minute_of_day(TIME '01:02:03')", expression.toString());

    expression = DSL.minute_of_day(DSL.literal("01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(62), eval(expression));
    assertEquals("minute_of_day(\"01:02:03\")", expression.toString());

    expression = DSL.minute_of_day(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(62), expression.valueOf());
    assertEquals("minute_of_day(TIMESTAMP '2020-08-17 01:02:03')", expression.toString());

    expression = DSL.minute_of_day(DSL.literal("2020-08-17 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(62), expression.valueOf());
    assertEquals("minute_of_day(\"2020-08-17 01:02:03\")", expression.toString());

    testMinuteOfDay("2020-08-17 23:59:59", 1439);
    testMinuteOfDay("2020-08-17 00:00:01", 0);
  }

  private void minuteOfHourQuery(FunctionExpression dateExpression, int minute, String testExpr) {
    assertAll(
        () -> assertEquals(INTEGER, dateExpression.type()),
        () -> assertEquals(integerValue(minute), eval(dateExpression)),
        () -> assertEquals(testExpr, dateExpression.toString()));
  }

  private static Stream<Arguments> getTestDataForMinuteOfHour() {
    return Stream.of(
        Arguments.of(
            DSL.literal(new ExprTimeValue("01:02:03")), 2, "minute_of_hour(TIME '01:02:03')"),
        Arguments.of(DSL.literal("01:02:03"), 2, "minute_of_hour(\"01:02:03\")"),
        Arguments.of(
            DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")),
            2,
            "minute_of_hour(TIMESTAMP '2020-08-17 01:02:03')"),
        Arguments.of(
            DSL.literal("2020-08-17 01:02:03"), 2, "minute_of_hour(\"2020-08-17 01:02:03\")"));
  }

  @ParameterizedTest(name = "{2}")
  @MethodSource("getTestDataForMinuteOfHour")
  public void minuteOfHour(LiteralExpression arg, int expectedResult, String expectedString) {
    minuteOfHourQuery(DSL.minute_of_hour(arg), expectedResult, expectedString);
  }

  private void invalidMinuteOfHourQuery(String time) {
    FunctionExpression expression = DSL.minute_of_hour(DSL.literal(new ExprTimeValue(time)));
    eval(expression);
  }

  @Test
  public void minuteOfHourInvalidArguments() {
    assertAll(
        // Invalid Seconds
        () ->
            assertThrows(SemanticCheckException.class, () -> invalidMinuteOfHourQuery("12:23:61")),

        // Invalid Minutes
        () ->
            assertThrows(SemanticCheckException.class, () -> invalidMinuteOfHourQuery("12:61:34")),

        // Invalid Hours
        () ->
            assertThrows(SemanticCheckException.class, () -> invalidMinuteOfHourQuery("25:23:34")),

        // incorrect format
        () ->
            assertThrows(SemanticCheckException.class, () -> invalidMinuteOfHourQuery("asdfasdf")));
  }

  @Test
  public void month() {
    FunctionExpression expression = DSL.month(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("month(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(8), eval(expression));

    expression = DSL.month(DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("month(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));

    expression = DSL.month(DSL.literal("2020-08-07 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals("month(\"2020-08-07 01:02:03\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));
  }

  private static Stream<Arguments> getTestDataForMonthOfYear() {
    return Stream.of(
        Arguments.of(
            DSL.literal(new ExprDateValue("2020-08-07")), "month_of_year(DATE '2020-08-07')", 8),
        Arguments.of(
            DSL.literal(new ExprTimestampValue("2020-08-07 12:23:34")),
            "month_of_year(TIMESTAMP '2020-08-07 12:23:34')",
            8),
        Arguments.of(DSL.literal("2020-08-07"), "month_of_year(\"2020-08-07\")", 8),
        Arguments.of(
            DSL.literal("2020-08-07 01:02:03"), "month_of_year(\"2020-08-07 01:02:03\")", 8));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getTestDataForMonthOfYear")
  public void monthOfYear(LiteralExpression arg, String expectedString, int expectedResult) {
    validateStringFormat(
        DSL.month_of_year(functionProperties, arg), expectedString, expectedResult);
  }

  @Test
  public void testMonthOfYearWithTimeType() {
    validateStringFormat(
        DSL.month_of_year(functionProperties, DSL.literal(new ExprTimeValue("12:23:34"))),
        "month_of_year(TIME '12:23:34')",
        LocalDate.now(functionProperties.getQueryStartClock()).getMonthValue());
  }

  private void invalidDatesQuery(String date) throws SemanticCheckException {
    FunctionExpression expression =
        DSL.month_of_year(functionProperties, DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test
  public void monthOfYearInvalidDates() {
    assertAll(
        () -> assertThrows(SemanticCheckException.class, () -> invalidDatesQuery("2019-01-50")),
        () -> assertThrows(SemanticCheckException.class, () -> invalidDatesQuery("2019-02-29")),
        () -> assertThrows(SemanticCheckException.class, () -> invalidDatesQuery("2019-02-31")),
        () -> assertThrows(SemanticCheckException.class, () -> invalidDatesQuery("2019-13-05")));
  }

  @Test
  public void monthOfYearAlternateArgumentSyntaxes() {
    FunctionExpression expression =
        DSL.month_of_year(functionProperties, DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("month_of_year(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(8), eval(expression));

    expression = DSL.month_of_year(functionProperties, DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("month_of_year(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));

    expression = DSL.month_of_year(functionProperties, DSL.literal("2020-08-07 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals("month_of_year(\"2020-08-07 01:02:03\")", expression.toString());
    assertEquals(integerValue(8), eval(expression));
  }

  @Test
  public void monthName() {
    FunctionExpression expression = DSL.monthname(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(STRING, expression.type());
    assertEquals("monthname(DATE '2020-08-07')", expression.toString());
    assertEquals(stringValue("August"), eval(expression));

    expression = DSL.monthname(DSL.literal("2020-08-07"));
    assertEquals(STRING, expression.type());
    assertEquals("monthname(\"2020-08-07\")", expression.toString());
    assertEquals(stringValue("August"), eval(expression));

    expression = DSL.monthname(DSL.literal("2020-08-07 01:02:03"));
    assertEquals(STRING, expression.type());
    assertEquals("monthname(\"2020-08-07 01:02:03\")", expression.toString());
    assertEquals(stringValue("August"), eval(expression));
  }

  @Test
  public void quarter() {
    FunctionExpression expression = DSL.quarter(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("quarter(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(3), eval(expression));

    expression = DSL.quarter(DSL.literal("2020-12-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("quarter(\"2020-12-07\")", expression.toString());
    assertEquals(integerValue(4), eval(expression));

    expression = DSL.quarter(DSL.literal("2020-12-07 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals("quarter(\"2020-12-07 01:02:03\")", expression.toString());
    assertEquals(integerValue(4), eval(expression));
  }

  private static Stream<Arguments> getTestDataForSecToTime() {
    return Stream.of(
        Arguments.of(1, "00:00:01"),
        Arguments.of(2378, "00:39:38"),
        Arguments.of(6897, "01:54:57"),
        Arguments.of(-82800, "01:00:00"),
        Arguments.of(-169200, "01:00:00"),
        Arguments.of(3600, "01:00:00"),
        Arguments.of(90000, "01:00:00"),
        Arguments.of(176400, "01:00:00"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getTestDataForSecToTime")
  public void testSecToTime(int seconds, String expected) {
    FunctionExpression expr = DSL.sec_to_time(DSL.literal(new ExprIntegerValue(seconds)));

    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue(expected), eval(expr));
  }

  private static Stream<Arguments> getTestDataForSecToTimeWithDecimal() {
    return Stream.of(
        Arguments.of(1.123, "00:00:01.123"),
        Arguments.of(1.00123, "00:00:01.00123"),
        Arguments.of(1.001023, "00:00:01.001023"),
        Arguments.of(1.000000042, "00:00:01.000000042"),
        Arguments.of(3.14, "00:00:03.14"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getTestDataForSecToTimeWithDecimal")
  public void testSecToTimeWithDecimal(double arg, String expected) {
    FunctionExpression expr = DSL.sec_to_time(DSL.literal(new ExprDoubleValue(arg)));

    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue(expected), eval(expr));
  }

  @Test
  public void second() {
    FunctionExpression expression = DSL.second(DSL.literal(new ExprTimeValue("01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(3), eval(expression));
    assertEquals("second(TIME '01:02:03')", expression.toString());

    expression = DSL.second(DSL.literal("01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(3), eval(expression));
    assertEquals("second(\"01:02:03\")", expression.toString());

    expression = DSL.second(DSL.literal("2020-08-17 01:02:03"));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(3), eval(expression));
    assertEquals("second(\"2020-08-17 01:02:03\")", expression.toString());

    expression = DSL.second(DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")));
    assertEquals(INTEGER, expression.type());
    assertEquals(integerValue(3), expression.valueOf());
    assertEquals("second(TIMESTAMP '2020-08-17 01:02:03')", expression.toString());
  }

  private void secondOfMinuteQuery(FunctionExpression dateExpression, int second, String testExpr) {
    assertEquals(INTEGER, dateExpression.type());
    assertEquals(integerValue(second), eval(dateExpression));
    assertEquals(testExpr, dateExpression.toString());
  }

  private static Stream<Arguments> getTestDataForSecondOfMinute() {
    return Stream.of(
        Arguments.of(
            DSL.literal(new ExprTimeValue("01:02:03")), 3, "second_of_minute(TIME '01:02:03')"),
        Arguments.of(DSL.literal("01:02:03"), 3, "second_of_minute(\"01:02:03\")"),
        Arguments.of(
            DSL.literal("2020-08-17 01:02:03"), 3, "second_of_minute(\"2020-08-17 01:02:03\")"),
        Arguments.of(
            DSL.literal(new ExprTimestampValue("2020-08-17 01:02:03")),
            3,
            "second_of_minute(TIMESTAMP '2020-08-17 01:02:03')"));
  }

  @ParameterizedTest(name = "{2}")
  @MethodSource("getTestDataForSecondOfMinute")
  public void secondOfMinute(LiteralExpression arg, int expectedResult, String expectedString) {
    secondOfMinuteQuery(DSL.second_of_minute(arg), expectedResult, expectedString);
  }

  private void invalidSecondOfMinuteQuery(String time) {
    FunctionExpression expression = DSL.second_of_minute(DSL.literal(new ExprTimeValue(time)));
    eval(expression);
  }

  @Test
  public void secondOfMinuteInvalidArguments() {
    assertAll(
        // Invalid Seconds
        () ->
            assertThrows(
                SemanticCheckException.class, () -> invalidSecondOfMinuteQuery("12:23:61")),
        // Invalid Minutes
        () ->
            assertThrows(
                SemanticCheckException.class, () -> invalidSecondOfMinuteQuery("12:61:34")),
        // Invalid Hours
        () ->
            assertThrows(
                SemanticCheckException.class, () -> invalidSecondOfMinuteQuery("25:23:34")),
        // incorrect format
        () ->
            assertThrows(
                SemanticCheckException.class, () -> invalidSecondOfMinuteQuery("asdfasdf")));
  }

  @Test
  public void time_to_sec() {
    FunctionExpression expression = DSL.time_to_sec(DSL.literal(new ExprTimeValue("22:23:00")));
    assertEquals(LONG, expression.type());
    assertEquals("time_to_sec(TIME '22:23:00')", expression.toString());
    assertEquals(longValue(80580L), eval(expression));

    expression = DSL.time_to_sec(DSL.literal("22:23:00"));
    assertEquals(LONG, expression.type());
    assertEquals("time_to_sec(\"22:23:00\")", expression.toString());
    assertEquals(longValue(80580L), eval(expression));
  }

  @Test
  public void time() {
    FunctionExpression expr = DSL.time(DSL.literal("01:01:01"));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("01:01:01"), eval(expr));
    assertEquals("time(\"01:01:01\")", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("01:01:01")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("01:01:01"), eval(expr));
    assertEquals("time(TIME '01:01:01')", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("01:01")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("01:01"), eval(expr));
    assertEquals("time(TIME '01:01:00')", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("2019-04-19 01:01:01")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("2019-04-19 01:01:01"), eval(expr));
    assertEquals("time(TIME '01:01:01')", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("2019-04-19 01:01")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("2019-04-19 01:01"), eval(expr));
    assertEquals("time(TIME '01:01:00')", expr.toString());

    expr = DSL.time(DSL.literal(new ExprTimeValue("01:01:01.0123")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("01:01:01.0123"), eval(expr));
    assertEquals("time(TIME '01:01:01.0123')", expr.toString());

    expr = DSL.time(DSL.date(DSL.literal("2020-01-02")));
    assertEquals(TIME, expr.type());
    assertEquals(new ExprTimeValue("00:00:00"), expr.valueOf());
  }

  @Test
  public void timestamp() {
    FunctionExpression expr = DSL.timestamp(DSL.literal("2020-08-17 01:01:01"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue("2020-08-17 01:01:01"), expr.valueOf());
    assertEquals("timestamp(cast_to_timestamp(\"2020-08-17 01:01:01\"))", expr.toString());

    expr = DSL.timestamp(DSL.literal(new ExprTimestampValue("2020-08-17 01:01:01")));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue("2020-08-17 01:01:01"), expr.valueOf());
    assertEquals("timestamp(TIMESTAMP '2020-08-17 01:01:01')", expr.toString());
  }

  private void weekQuery(String date, int mode, int expectedResult) {
    FunctionExpression expression =
        DSL.week(functionProperties, DSL.literal(new ExprDateValue(date)), DSL.literal(mode));
    assertEquals(INTEGER, expression.type());
    assertEquals(String.format("week(DATE '%s', %d)", date, mode), expression.toString());
    assertEquals(integerValue(expectedResult), eval(expression));
  }

  private void weekOfYearUnderscoresQuery(String date, int mode, int expectedResult) {
    FunctionExpression expression =
        DSL.week_of_year(
            functionProperties, DSL.literal(new ExprDateValue(date)), DSL.literal(mode));
    assertEquals(INTEGER, expression.type());
    assertEquals(String.format("week_of_year(DATE '%s', %d)", date, mode), expression.toString());
    assertEquals(integerValue(expectedResult), eval(expression));
  }

  private void weekOfYearQuery(String date, int mode, int expectedResult) {
    FunctionExpression expression =
        DSL.weekofyear(functionProperties, DSL.literal(new ExprDateValue(date)), DSL.literal(mode));
    assertEquals(INTEGER, expression.type());
    assertEquals(String.format("weekofyear(DATE '%s', %d)", date, mode), expression.toString());
    assertEquals(integerValue(expectedResult), eval(expression));
  }

  private static Stream<Arguments> getTestDataForWeek() {
    // Test the behavior of different modes passed into the 'week_of_year' function
    return Stream.of(
        Arguments.of("2019-01-05", 0, 0),
        Arguments.of("2019-01-05", 1, 1),
        Arguments.of("2019-01-05", 2, 52),
        Arguments.of("2019-01-05", 3, 1),
        Arguments.of("2019-01-05", 4, 1),
        Arguments.of("2019-01-05", 5, 0),
        Arguments.of("2019-01-05", 6, 1),
        Arguments.of("2019-01-05", 7, 53),
        Arguments.of("2019-01-06", 0, 1),
        Arguments.of("2019-01-06", 1, 1),
        Arguments.of("2019-01-06", 2, 1),
        Arguments.of("2019-01-06", 3, 1),
        Arguments.of("2019-01-06", 4, 2),
        Arguments.of("2019-01-06", 5, 0),
        Arguments.of("2019-01-06", 6, 2),
        Arguments.of("2019-01-06", 7, 53),
        Arguments.of("2019-01-07", 0, 1),
        Arguments.of("2019-01-07", 1, 2),
        Arguments.of("2019-01-07", 2, 1),
        Arguments.of("2019-01-07", 3, 2),
        Arguments.of("2019-01-07", 4, 2),
        Arguments.of("2019-01-07", 5, 1),
        Arguments.of("2019-01-07", 6, 2),
        Arguments.of("2019-01-07", 7, 1),
        Arguments.of("2000-01-01", 0, 0),
        Arguments.of("2000-01-01", 2, 52),
        Arguments.of("1999-12-31", 0, 52));
  }

  @ParameterizedTest(name = "{1}{2}")
  @MethodSource("getTestDataForWeek")
  public void testWeek(String date, int mode, int expected) {
    weekQuery(date, mode, expected);
    weekOfYearQuery(date, mode, expected);
    weekOfYearUnderscoresQuery(date, mode, expected);
  }

  private void validateStringFormat(
      FunctionExpression expr, String expectedString, int expectedResult) {
    assertAll(
        () -> assertEquals(INTEGER, expr.type()),
        () -> assertEquals(expectedString, expr.toString()),
        () -> assertEquals(integerValue(expectedResult), eval(expr)));
  }

  private static Stream<Arguments> getTestDataForWeekFormats() {
    return Stream.of(
        Arguments.of(DSL.literal(new ExprDateValue("2019-01-05")), "DATE '2019-01-05'", 0),
        Arguments.of(
            DSL.literal(new ExprTimestampValue("2019-01-05 01:02:03")),
            "TIMESTAMP '2019-01-05 01:02:03'",
            0),
        Arguments.of(DSL.literal("2019-01-05"), "\"2019-01-05\"", 0),
        Arguments.of(DSL.literal("2019-01-05 00:01:00"), "\"2019-01-05 00:01:00\"", 0));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getTestDataForWeekFormats")
  public void testWeekFormats(
      LiteralExpression arg, String expectedString, Integer expectedInteger) {
    validateStringFormat(
        DSL.week(functionProperties, arg),
        String.format("week(%s)", expectedString),
        expectedInteger);
    validateStringFormat(
        DSL.week_of_year(functionProperties, arg),
        String.format("week_of_year(%s)", expectedString),
        expectedInteger);
    validateStringFormat(
        DSL.weekofyear(functionProperties, arg),
        String.format("weekofyear(%s)", expectedString),
        expectedInteger);
  }

  @Test
  @Disabled("Test is disabled because of issue https://github.com/opensearch-project/sql/issues/2477")
  public void testWeekOfYearWithTimeType() {
    LocalDate today = LocalDate.now(functionProperties.getQueryStartClock());

    // week is based on the first sunday of the year
    LocalDate firstSundayOfYear = today.withDayOfYear(1).with(nextOrSame(SUNDAY));
    int week =
        today.isBefore(firstSundayOfYear)
            ? 0
            : (int) ChronoUnit.WEEKS.between(firstSundayOfYear, today) + 1;

    assertAll(
        () ->
            validateStringFormat(
                DSL.week(
                    functionProperties, DSL.literal(new ExprTimeValue("12:23:34")), DSL.literal(0)),
                "week(TIME '12:23:34', 0)",
                week),
        () ->
            validateStringFormat(
                DSL.week_of_year(functionProperties, DSL.literal(new ExprTimeValue("12:23:34"))),
                "week_of_year(TIME '12:23:34')",
                week),
        () ->
            validateStringFormat(
                DSL.weekofyear(functionProperties, DSL.literal(new ExprTimeValue("12:23:34"))),
                "weekofyear(TIME '12:23:34')",
                week));
  }

  @Test
  public void modeInUnsupportedFormat() {
    FunctionExpression expression1 =
        DSL.week(functionProperties, DSL.literal(new ExprDateValue("2019-01-05")), DSL.literal(8));
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> eval(expression1));
    assertEquals("mode:8 is invalid, please use mode value between 0-7", exception.getMessage());

    FunctionExpression expression2 =
        DSL.week(functionProperties, DSL.literal(new ExprDateValue("2019-01-05")), DSL.literal(-1));
    exception = assertThrows(SemanticCheckException.class, () -> eval(expression2));
    assertEquals("mode:-1 is invalid, please use mode value between 0-7", exception.getMessage());
  }

  @Test
  public void testInvalidWeekOfYear() {
    assertAll(
        // Test for WeekOfYear
        // test invalid month
        () ->
            assertThrows(
                SemanticCheckException.class, () -> weekOfYearQuery("2019-13-05 01:02:03", 0, 0)),
        // test invalid day
        () ->
            assertThrows(
                SemanticCheckException.class, () -> weekOfYearQuery("2019-01-50 01:02:03", 0, 0)),
        // test invalid leap year
        () ->
            assertThrows(
                SemanticCheckException.class, () -> weekOfYearQuery("2019-02-29 01:02:03", 0, 0)),

        // Test for Week_Of_Year
        // test invalid month
        () ->
            assertThrows(
                SemanticCheckException.class,
                () -> weekOfYearUnderscoresQuery("2019-13-05 01:02:03", 0, 0)),
        // test invalid day
        () ->
            assertThrows(
                SemanticCheckException.class,
                () -> weekOfYearUnderscoresQuery("2019-01-50 01:02:03", 0, 0)),
        // test invalid leap year
        () ->
            assertThrows(
                SemanticCheckException.class,
                () -> weekOfYearUnderscoresQuery("2019-02-29 01:02:03", 0, 0)));
  }

  @Test
  public void weekOfYearModeInUnsupportedFormat() {
    FunctionExpression expression1 =
        DSL.week_of_year(
            functionProperties, DSL.literal(new ExprDateValue("2019-01-05")), DSL.literal(8));
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> eval(expression1));
    assertEquals("mode:8 is invalid, please use mode value between 0-7", exception.getMessage());

    FunctionExpression expression2 =
        DSL.week_of_year(
            functionProperties, DSL.literal(new ExprDateValue("2019-01-05")), DSL.literal(-1));
    exception = assertThrows(SemanticCheckException.class, () -> eval(expression2));
    assertEquals("mode:-1 is invalid, please use mode value between 0-7", exception.getMessage());
  }

  @Test
  public void to_days() {
    FunctionExpression expression = DSL.to_days(DSL.literal(new ExprDateValue("2008-10-07")));
    assertEquals(LONG, expression.type());
    assertEquals("to_days(DATE '2008-10-07')", expression.toString());
    assertEquals(longValue(733687L), eval(expression));

    expression = DSL.to_days(DSL.literal("1969-12-31"));
    assertEquals(LONG, expression.type());
    assertEquals("to_days(\"1969-12-31\")", expression.toString());
    assertEquals(longValue(719527L), eval(expression));

    expression = DSL.to_days(DSL.literal("1969-12-31 01:01:01"));
    assertEquals(LONG, expression.type());
    assertEquals("to_days(\"1969-12-31 01:01:01\")", expression.toString());
    assertEquals(longValue(719527L), eval(expression));
  }

  @Test
  public void year() {
    FunctionExpression expression = DSL.year(DSL.literal(new ExprDateValue("2020-08-07")));
    assertEquals(INTEGER, expression.type());
    assertEquals("year(DATE '2020-08-07')", expression.toString());
    assertEquals(integerValue(2020), eval(expression));

    expression = DSL.year(DSL.literal("2020-08-07"));
    assertEquals(INTEGER, expression.type());
    assertEquals("year(\"2020-08-07\")", expression.toString());
    assertEquals(integerValue(2020), eval(expression));

    expression = DSL.year(DSL.literal("2020-08-07 01:01:01"));
    assertEquals(INTEGER, expression.type());
    assertEquals("year(\"2020-08-07 01:01:01\")", expression.toString());
    assertEquals(integerValue(2020), eval(expression));
  }

  @Test
  public void date_format() {
    dateFormatTesters.forEach(this::testDateFormat);
    String timestamp = "1998-01-31 13:14:15.012345";
    String timestampFormat =
        "%a %b %c %D %d %e %f %H %h %I %i %j %k %l %M " + "%m %p %r %S %s %T %% %P";
    String timestampFormatted =
        "Sat Jan 01 31st 31 31 012345 13 01 01 14 031 13 1 "
            + "January 01 PM 01:14:15 PM 15 15 13:14:15 % P";

    FunctionExpression expr =
        DSL.date_format(functionProperties, DSL.literal(timestamp), DSL.literal(timestampFormat));
    assertEquals(STRING, expr.type());
    assertEquals(timestampFormatted, eval(expr).stringValue());
  }

  void testDateFormat(DateFormatTester dft) {
    FunctionExpression expr = dft.getDateFormatExpression();
    assertEquals(STRING, expr.type());
    assertEquals(dft.getFormatted(), eval(expr).stringValue());
  }

  @Test
  public void testDateFormatWithTimeType() {
    FunctionExpression expr =
        DSL.date_format(
            functionProperties,
            DSL.literal(new ExprTimeValue("12:23:34")),
            DSL.literal(new ExprStringValue("%m %d")));

    assertEquals(expr.toString(), "date_format(TIME '12:23:34', \"%m %d\")");
    assertEquals(
        LocalDateTime.now(functionProperties.getQueryStartClock())
            .format(DateTimeFormatter.ofPattern("\"MM dd\"")),
        eval(expr).toString());
  }

  @Test
  public void testTimeFormatWithDateType() {
    FunctionExpression expr =
        DSL.time_format(
            functionProperties,
            DSL.literal(new ExprDateValue("2023-01-16")),
            DSL.literal(new ExprStringValue("%h %s")));

    assertEquals(expr.toString(), "time_format(DATE '2023-01-16', \"%h %s\")");
    assertEquals("\"12 00\"", eval(expr).toString());
  }

  private static Stream<Arguments> getTestDataForTimeFormat() {
    return Stream.of(
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%f"), "012345"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.002345"), DSL.literal("%f"), "002345"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012300"), DSL.literal("%f"), "012300"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%H"), "13"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%h"), "01"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%I"), "01"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%i"), "14"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%k"), "13"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%l"), "1"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%p"), "PM"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%r"), "01:14:15 PM"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%S"), "15"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%s"), "15"),
        Arguments.of(DSL.literal("1998-01-31 13:14:15.012345"), DSL.literal("%T"), "13:14:15"),
        Arguments.of(
            DSL.literal("1998-01-31 13:14:15.012345"),
            DSL.literal("%f %H %h %I %i %k %l %p %r %S %s %T"),
            "012345 13 01 01 14 13 1 PM 01:14:15 PM 15 15 13:14:15"));
  }

  private void timeFormatQuery(
      LiteralExpression arg, LiteralExpression format, String expectedResult) {
    FunctionExpression expr = DSL.time_format(functionProperties, arg, format);
    assertEquals(STRING, expr.type());
    assertEquals(expectedResult, eval(expr).stringValue());
  }

  @ParameterizedTest(name = "{0}{1}")
  @MethodSource("getTestDataForTimeFormat")
  public void testTimeFormat(
      LiteralExpression arg, LiteralExpression format, String expectedResult) {
    timeFormatQuery(arg, format, expectedResult);
  }

  private static Stream<Arguments> getInvalidTestDataForTimeFormat() {
    return Stream.of(
        Arguments.of(DSL.literal("asdfasdf"), DSL.literal("%f")),
        Arguments.of(DSL.literal("12345"), DSL.literal("%h")),
        Arguments.of(DSL.literal("10:11:61"), DSL.literal("%h")),
        Arguments.of(DSL.literal("10:61:12"), DSL.literal("%h")),
        Arguments.of(DSL.literal("61:11:12"), DSL.literal("%h")));
  }

  @ParameterizedTest(name = "{0}{1}")
  @MethodSource("getInvalidTestDataForTimeFormat")
  public void testInvalidTimeFormat(LiteralExpression arg, LiteralExpression format) {
    FunctionExpression expr = DSL.time_format(functionProperties, arg, format);
    assertThrows(SemanticCheckException.class, () -> eval(expr));
  }

  private static Stream<Arguments> getInvalidTimeFormatHandlers() {
    return Stream.of(
        Arguments.of("%a"),
        Arguments.of("%b"),
        Arguments.of("%j"),
        Arguments.of("%M"),
        Arguments.of("%W"),
        Arguments.of("%D"),
        Arguments.of("%w"),
        Arguments.of("%U"),
        Arguments.of("%u"),
        Arguments.of("%V"),
        Arguments.of("%v"),
        Arguments.of("%X"),
        Arguments.of("%x"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getInvalidTimeFormatHandlers")
  public void testTimeFormatWithInvalidHandlers(String handler) {
    FunctionExpression expr =
        DSL.time_format(functionProperties, DSL.literal("12:23:34"), DSL.literal(handler));
    assertEquals(ExprNullValue.of(), eval(expr));
  }

  @Test
  public void testTimeFormatWithDateHandlers() {
    FunctionExpression expr =
        DSL.time_format(
            functionProperties,
            DSL.literal(new ExprDateValue("2023-01-17")),
            DSL.literal("%c %d %e %m %Y %y"));
    assertEquals("0 00 0 00 0000 00", eval(expr).stringValue());
  }

  @Test
  public void testTimeFormatAndDateFormatReturnSameResult() {
    FunctionExpression timeFormatExpr =
        DSL.time_format(
            functionProperties,
            DSL.literal(new ExprDateValue("1998-01-31 13:14:15.012345")),
            DSL.literal("%f %H %h %I %i %k %l %p %r %S %s %T"));
    FunctionExpression dateFormatExpr =
        DSL.date_format(
            functionProperties,
            DSL.literal(new ExprDateValue("1998-01-31 13:14:15.012345")),
            DSL.literal("%f %H %h %I %i %k %l %p %r %S %s %T"));

    assertEquals(eval(dateFormatExpr), eval(timeFormatExpr));
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
