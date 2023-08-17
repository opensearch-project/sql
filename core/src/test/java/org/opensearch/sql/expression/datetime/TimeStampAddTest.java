/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
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

class TimeStampAddTest extends ExpressionTestBase {

  private static Stream<Arguments> getTestDataForTimestampAdd() {
    return Stream.of(
        Arguments.of(
            "MINUTE", 1, new ExprStringValue("2003-01-02 00:00:00"), "2003-01-02 00:01:00"),
        Arguments.of("WEEK", 1, new ExprStringValue("2003-01-02 00:00:00"), "2003-01-09 00:00:00"),

        // Date
        Arguments.of("MINUTE", 1, new ExprDateValue("2003-01-02"), "2003-01-02 00:01:00"),
        Arguments.of("WEEK", 1, new ExprDateValue("2003-01-02"), "2003-01-09 00:00:00"),

        // Timestamp
        Arguments.of(
            "MINUTE", 1, new ExprTimestampValue("2003-01-02 00:00:00"), "2003-01-02 00:01:00"),
        Arguments.of(
            "WEEK", 1, new ExprTimestampValue("2003-01-02 00:00:00"), "2003-01-09 00:00:00"),

        // Cases surrounding leap year
        Arguments.of(
            "SECOND", 1, new ExprTimestampValue("2020-02-28 23:59:59"), "2020-02-29 00:00:00"),
        Arguments.of(
            "MINUTE", 1, new ExprTimestampValue("2020-02-28 23:59:59"), "2020-02-29 00:00:59"),
        Arguments.of(
            "HOUR", 1, new ExprTimestampValue("2020-02-28 23:59:59"), "2020-02-29 00:59:59"),
        Arguments.of(
            "DAY", 1, new ExprTimestampValue("2020-02-28 23:59:59"), "2020-02-29 23:59:59"),
        Arguments.of(
            "WEEK", 1, new ExprTimestampValue("2020-02-28 23:59:59"), "2020-03-06 23:59:59"),

        // Cases surrounding end-of-year
        Arguments.of(
            "SECOND", 1, new ExprTimestampValue("2020-12-31 23:59:59"), "2021-01-01 00:00:00"),
        Arguments.of(
            "MINUTE", 1, new ExprTimestampValue("2020-12-31 23:59:59"), "2021-01-01 00:00:59"),
        Arguments.of(
            "HOUR", 1, new ExprTimestampValue("2020-12-31 23:59:59"), "2021-01-01 00:59:59"),
        Arguments.of(
            "DAY", 1, new ExprTimestampValue("2020-12-31 23:59:59"), "2021-01-01 23:59:59"),
        Arguments.of(
            "WEEK", 1, new ExprTimestampValue("2020-12-31 23:59:59"), "2021-01-07 23:59:59"),

        // Test adding a month (including special cases)
        Arguments.of("MONTH", 1, new ExprStringValue("2003-01-02 00:00:00"), "2003-02-02 00:00:00"),
        Arguments.of("MONTH", 1, new ExprDateValue("2024-03-30"), "2024-04-30 00:00:00"),
        Arguments.of("MONTH", 1, new ExprDateValue("2024-03-31"), "2024-04-30 00:00:00"),

        // Test remaining interval types
        Arguments.of(
            "MICROSECOND",
            123,
            new ExprStringValue("2003-01-02 00:00:00"),
            "2003-01-02 00:00:00.000123"),
        Arguments.of(
            "QUARTER", 1, new ExprStringValue("2003-01-02 00:00:00"), "2003-04-02 00:00:00"),
        Arguments.of("YEAR", 1, new ExprStringValue("2003-01-02 00:00:00"), "2004-01-02 00:00:00"),

        // Test negative value for amount (Test for all intervals)
        Arguments.of(
            "MICROSECOND",
            -1,
            new ExprStringValue("2000-01-01 00:00:00"),
            "1999-12-31 23:59:59.999999"),
        Arguments.of(
            "SECOND", -1, new ExprStringValue("2000-01-01 00:00:00"), "1999-12-31 23:59:59"),
        Arguments.of(
            "MINUTE", -1, new ExprStringValue("2000-01-01 00:00:00"), "1999-12-31 23:59:00"),
        Arguments.of("HOUR", -1, new ExprStringValue("2000-01-01 00:00:00"), "1999-12-31 23:00:00"),
        Arguments.of("DAY", -1, new ExprStringValue("2000-01-01 00:00:00"), "1999-12-31 00:00:00"),
        Arguments.of("WEEK", -1, new ExprStringValue("2000-01-01 00:00:00"), "1999-12-25 00:00:00"),
        Arguments.of(
            "MONTH", -1, new ExprStringValue("2000-01-01 00:00:00"), "1999-12-01 00:00:00"),
        Arguments.of(
            "QUARTER", -1, new ExprStringValue("2000-01-01 00:00:00"), "1999-10-01 00:00:00"),
        Arguments.of(
            "YEAR", -1, new ExprStringValue("2000-01-01 00:00:00"), "1999-01-01 00:00:00"));
  }

  private static FunctionExpression timestampaddQuery(
      String unit, int amount, ExprValue datetimeExpr) {
    return DSL.timestampadd(
        DSL.literal(unit), DSL.literal(new ExprIntegerValue(amount)), DSL.literal(datetimeExpr));
  }

  @ParameterizedTest
  @MethodSource("getTestDataForTimestampAdd")
  public void testTimestampadd(String unit, int amount, ExprValue datetimeExpr, String expected) {
    FunctionExpression expr = timestampaddQuery(unit, amount, datetimeExpr);
    assertEquals(new ExprTimestampValue(expected), eval(expr));
  }

  private static Stream<Arguments> getTestDataForTestAddingDatePartToTime() {
    return Stream.of(
        Arguments.of("DAY", 1, "10:11:12", LocalDate.now().plusDays(1)),
        Arguments.of("DAY", 5, "10:11:12", LocalDate.now().plusDays(5)),
        Arguments.of("DAY", 10, "10:11:12", LocalDate.now().plusDays(10)),
        Arguments.of("DAY", -10, "10:11:12", LocalDate.now().plusDays(-10)),
        Arguments.of("WEEK", 1, "10:11:12", LocalDate.now().plusWeeks(1)),
        Arguments.of("WEEK", 5, "10:11:12", LocalDate.now().plusWeeks(5)),
        Arguments.of("WEEK", 10, "10:11:12", LocalDate.now().plusWeeks(10)),
        Arguments.of("WEEK", -10, "10:11:12", LocalDate.now().plusWeeks(-10)),
        Arguments.of("MONTH", 1, "10:11:12", LocalDate.now().plusMonths(1)),
        Arguments.of("MONTH", 5, "10:11:12", LocalDate.now().plusMonths(5)),
        Arguments.of("MONTH", 10, "10:11:12", LocalDate.now().plusMonths(10)),
        Arguments.of("MONTH", -10, "10:11:12", LocalDate.now().plusMonths(-10)),
        Arguments.of("QUARTER", 1, "10:11:12", LocalDate.now().plusMonths(3 * 1)),
        Arguments.of("QUARTER", 3, "10:11:12", LocalDate.now().plusMonths(3 * 3)),
        Arguments.of("QUARTER", 5, "10:11:12", LocalDate.now().plusMonths(3 * 5)),
        Arguments.of("QUARTER", -5, "10:11:12", LocalDate.now().plusMonths(3 * -5)),
        Arguments.of("YEAR", 1, "10:11:12", LocalDate.now().plusYears(1)),
        Arguments.of("YEAR", 5, "10:11:12", LocalDate.now().plusYears(5)),
        Arguments.of("YEAR", 10, "10:11:12", LocalDate.now().plusYears(10)),
        Arguments.of("YEAR", -10, "10:11:12", LocalDate.now().plusYears(-10)));
  }

  @ParameterizedTest
  @MethodSource("getTestDataForTestAddingDatePartToTime")
  public void testAddingDatePartToTime(
      String interval, int addedInterval, String timeArg, LocalDate expectedDate) {
    FunctionExpression expr =
        DSL.timestampadd(
            functionProperties,
            DSL.literal(interval),
            DSL.literal(new ExprIntegerValue(addedInterval)),
            DSL.literal(new ExprTimeValue(timeArg)));

    LocalDateTime expected1 = LocalDateTime.of(expectedDate, LocalTime.parse(timeArg));

    assertEquals(new ExprTimestampValue(expected1), eval(expr));
  }

  @Test
  public void testAddingTimePartToTime() {
    String interval = "MINUTE";
    int addedInterval = 1;
    String timeArg = "10:11:12";

    FunctionExpression expr =
        DSL.timestampadd(
            functionProperties,
            DSL.literal(interval),
            DSL.literal(new ExprIntegerValue(addedInterval)),
            DSL.literal(new ExprTimeValue(timeArg)));

    LocalDateTime expected =
        LocalDateTime.of(LocalDate.now(), LocalTime.parse(timeArg).plusMinutes(addedInterval));

    assertEquals(new ExprTimestampValue(expected), eval(expr));
  }

  @Test
  public void testDifferentInputTypesHaveSameResult() {
    String part = "SECOND";
    int amount = 1;
    FunctionExpression dateExpr = timestampaddQuery(part, amount, new ExprDateValue("2000-01-01"));

    FunctionExpression stringExpr =
        timestampaddQuery(part, amount, new ExprStringValue("2000-01-01 00:00:00"));

    FunctionExpression timestampExpr =
        timestampaddQuery(part, amount, new ExprTimestampValue("2000-01-01 00:00:00"));

    assertAll(
        () -> assertEquals(eval(dateExpr), eval(stringExpr)),
        () -> assertEquals(eval(dateExpr), eval(timestampExpr)));
  }

  private static Stream<Arguments> getInvalidTestDataForTimestampAdd() {
    return Stream.of(
        Arguments.of("WEEK", 1, new ExprStringValue("2000-13-01")),
        Arguments.of("WEEK", 1, new ExprStringValue("2000-01-40")));
  }

  @ParameterizedTest
  @MethodSource("getInvalidTestDataForTimestampAdd")
  public void testInvalidArguments(String interval, int amount, ExprValue datetimeExpr) {
    FunctionExpression expr = timestampaddQuery(interval, amount, datetimeExpr);
    assertThrows(SemanticCheckException.class, () -> eval(expr));
  }

  @Test
  public void testNullReturnValue() {
    FunctionExpression expr = timestampaddQuery("INVALID", 1, new ExprDateValue("2000-01-01"));
    assertEquals(ExprNullValue.of(), eval(expr));
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
