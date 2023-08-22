/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import java.time.LocalDate;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.LiteralExpression;

class WeekdayTest extends ExpressionTestBase {

  private void weekdayQuery(FunctionExpression dateExpression, int dayOfWeek, String testExpr) {

    assertAll(
        () -> assertEquals(INTEGER, dateExpression.type()),
        () -> assertEquals(integerValue(dayOfWeek), eval(dateExpression)),
        () -> assertEquals(testExpr, dateExpression.toString()));
  }

  private static Stream<Arguments> getTestDataForWeekday() {
    return Stream.of(
        Arguments.of(DSL.literal(new ExprDateValue("2020-08-07")), 4, "weekday(DATE '2020-08-07')"),
        Arguments.of(DSL.literal(new ExprDateValue("2020-08-09")), 6, "weekday(DATE '2020-08-09')"),
        Arguments.of(DSL.literal("2020-08-09"), 6, "weekday(\"2020-08-09\")"),
        Arguments.of(DSL.literal("2020-08-09 01:02:03"), 6, "weekday(\"2020-08-09 01:02:03\")"));
  }

  @MethodSource("getTestDataForWeekday")
  @ParameterizedTest
  public void weekday(LiteralExpression arg, int expectedInt, String expectedString) {
    FunctionExpression expression = DSL.weekday(functionProperties, arg);

    weekdayQuery(expression, expectedInt, expectedString);
  }

  @Test
  public void testWeekdayWithTimeType() {
    FunctionExpression expression =
        DSL.weekday(functionProperties, DSL.literal(new ExprTimeValue("12:23:34")));

    assertAll(
        () -> assertEquals(INTEGER, eval(expression).type()),
        () ->
            assertEquals(
                (LocalDate.now(functionProperties.getQueryStartClock()).getDayOfWeek().getValue()
                    - 1),
                eval(expression).integerValue()),
        () -> assertEquals("weekday(TIME '12:23:34')", expression.toString()));
  }

  private void testInvalidWeekday(String date) {
    FunctionExpression expression =
        DSL.weekday(functionProperties, DSL.literal(new ExprDateValue(date)));
    eval(expression);
  }

  @Test
  public void weekdayLeapYear() {
    assertAll(
        // Feb. 29 of a leap year
        () ->
            weekdayQuery(
                DSL.weekday(functionProperties, DSL.literal("2020-02-29")),
                5,
                "weekday(\"2020-02-29\")"),
        // day after Feb. 29 of a leap year
        () ->
            weekdayQuery(
                DSL.weekday(functionProperties, DSL.literal("2020-03-01")),
                6,
                "weekday(\"2020-03-01\")"),
        // Feb. 28 of a non-leap year
        () ->
            weekdayQuery(
                DSL.weekday(functionProperties, DSL.literal("2021-02-28")),
                6,
                "weekday(\"2021-02-28\")"),
        // Feb. 29 of a non-leap year
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidWeekday("2021-02-29")));
  }

  @Test
  public void weekdayInvalidArgument() {
    assertAll(
        // 40th day of the month
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidWeekday("2021-02-40")),

        // 13th month of the year
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidWeekday("2021-13-29")),

        // incorrect format
        () -> assertThrows(SemanticCheckException.class, () -> testInvalidWeekday("asdfasdf")));
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
