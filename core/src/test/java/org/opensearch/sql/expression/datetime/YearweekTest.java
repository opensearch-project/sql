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
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;

class YearweekTest extends ExpressionTestBase {

  private void yearweekQuery(String date, int mode, int expectedResult) {
    FunctionExpression expression =
        DSL.yearweek(functionProperties, DSL.literal(new ExprDateValue(date)), DSL.literal(mode));
    assertAll(
        () -> assertEquals(INTEGER, expression.type()),
        () ->
            assertEquals(
                String.format("yearweek(DATE '%s', %d)", date, mode), expression.toString()),
        () -> assertEquals(integerValue(expectedResult), eval(expression)));
  }

  private static Stream<Arguments> getTestDataForYearweek() {
    // Test the behavior of different modes passed into the 'yearweek' function
    return Stream.of(
        Arguments.of("2019-01-05", 0, 201852),
        Arguments.of("2019-01-05", 1, 201901),
        Arguments.of("2019-01-05", 2, 201852),
        Arguments.of("2019-01-05", 3, 201901),
        Arguments.of("2019-01-05", 4, 201901),
        Arguments.of("2019-01-05", 5, 201853),
        Arguments.of("2019-01-05", 6, 201901),
        Arguments.of("2019-01-05", 7, 201853),
        Arguments.of("2019-01-06", 0, 201901),
        Arguments.of("2019-01-06", 1, 201901),
        Arguments.of("2019-01-06", 2, 201901),
        Arguments.of("2019-01-06", 3, 201901),
        Arguments.of("2019-01-06", 4, 201902),
        Arguments.of("2019-01-06", 5, 201853),
        Arguments.of("2019-01-06", 6, 201902),
        Arguments.of("2019-01-06", 7, 201853),
        Arguments.of("2019-01-07", 0, 201901),
        Arguments.of("2019-01-07", 1, 201902),
        Arguments.of("2019-01-07", 2, 201901),
        Arguments.of("2019-01-07", 3, 201902),
        Arguments.of("2019-01-07", 4, 201902),
        Arguments.of("2019-01-07", 5, 201901),
        Arguments.of("2019-01-07", 6, 201902),
        Arguments.of("2019-01-07", 7, 201901),
        Arguments.of("2000-01-01", 0, 199952),
        Arguments.of("2000-01-01", 2, 199952),
        Arguments.of("1999-12-31", 0, 199952),
        Arguments.of("1999-01-01", 0, 199852),
        Arguments.of("1999-01-01", 1, 199852),
        Arguments.of("1999-01-01", 4, 199852),
        Arguments.of("1999-01-01", 5, 199852),
        Arguments.of("1999-01-01", 6, 199852));
  }

  @ParameterizedTest(name = "{0} | {1}")
  @MethodSource("getTestDataForYearweek")
  public void testYearweak(String date, int mode, int expected) {
    yearweekQuery(date, mode, expected);
  }

  @Test
  public void testYearweekWithoutMode() {
    LocalDate date = LocalDate.of(2019, 1, 05);

    FunctionExpression expression =
        DSL.yearweek(functionProperties, DSL.literal(new ExprDateValue(date)), DSL.literal(0));

    FunctionExpression expressionWithoutMode =
        DSL.yearweek(functionProperties, DSL.literal(new ExprDateValue(date)));

    assertEquals(eval(expression), eval(expressionWithoutMode));
  }

  // subtracting 1 as a temporary fix for year 2024.
  // Issue: https://github.com/opensearch-project/sql/issues/2477
  @Test
  public void testYearweekWithTimeType() {
    int expected = getYearWeekBeforeSunday(LocalDate.now(functionProperties.getQueryStartClock()));

    FunctionExpression expression =
        DSL.yearweek(
            functionProperties, DSL.literal(new ExprTimeValue("10:11:12")), DSL.literal(0));

    FunctionExpression expressionWithoutMode =
        DSL.yearweek(functionProperties, DSL.literal(new ExprTimeValue("10:11:12")));

    assertEquals(
        expected,
        eval(expression).integerValue(),
        String.format(
            "Expected year week: %d, got %s (test with mode)", expected, eval(expression)));
    assertEquals(
        expected,
        eval(expressionWithoutMode).integerValue(),
        String.format(
            "Expected year week: %d, got %s (test without mode)", expected, eval(expression)));
  }

  private int getYearWeekBeforeSunday(LocalDate date) {
    LocalDate firstSundayOfYear = date.withDayOfYear(1).with(nextOrSame(SUNDAY));
    if (date.isBefore(firstSundayOfYear)) {
      return getYearWeekBeforeSunday(date.minusDays(1));
    }

    int week = (int) ChronoUnit.WEEKS.between(firstSundayOfYear, date) + 1;
    int year = date.getYear();
    return Integer.parseInt(String.format("%d%02d", year, week));
  }

  @Test
  public void testInvalidYearWeek() {
    assertAll(
        // test invalid month
        () ->
            assertThrows(
                SemanticCheckException.class, () -> yearweekQuery("2019-13-05 01:02:03", 0, 0)),
        // test invalid day
        () ->
            assertThrows(
                SemanticCheckException.class, () -> yearweekQuery("2019-01-50 01:02:03", 0, 0)),
        // test invalid leap year
        () ->
            assertThrows(
                SemanticCheckException.class, () -> yearweekQuery("2019-02-29 01:02:03", 0, 0)));
  }

  @Test
  public void yearweekModeInUnsupportedFormat() {
    FunctionExpression expression1 =
        DSL.yearweek(
            functionProperties,
            DSL.literal(new ExprTimestampValue("2019-01-05 10:11:12")),
            DSL.literal(8));
    SemanticCheckException exception =
        assertThrows(SemanticCheckException.class, () -> eval(expression1));
    assertEquals("mode:8 is invalid, please use mode value between 0-7", exception.getMessage());

    FunctionExpression expression2 =
        DSL.yearweek(
            functionProperties,
            DSL.literal(new ExprTimestampValue("2019-01-05 10:11:12")),
            DSL.literal(-1));
    exception = assertThrows(SemanticCheckException.class, () -> eval(expression2));
    assertEquals("mode:-1 is invalid, please use mode value between 0-7", exception.getMessage());
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
