/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.expression.datetime.DateTimeFunction.SECONDS_PER_DAY;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprIntervalValue;
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

class ToSecondsTest extends ExpressionTestBase {

  private static final long SECONDS_FROM_0001_01_01_TO_EPOCH_START = 62167219200L;

  private static Stream<Arguments> getTestDataForToSeconds() {
    return Stream.of(
        Arguments.of(new ExprLongValue(101), new ExprLongValue(63113904000L)),
        Arguments.of(new ExprLongValue(1030), new ExprLongValue(63140083200L)),
        Arguments.of(new ExprLongValue(50101), new ExprLongValue(63271756800L)),
        Arguments.of(new ExprLongValue(950501), new ExprLongValue(62966505600L)),
        Arguments.of(new ExprLongValue(19950501), new ExprLongValue(62966505600L)),
        Arguments.of(new ExprLongValue(9950501), ExprNullValue.of()),
        Arguments.of(new ExprLongValue(-123L), ExprNullValue.of()),
        Arguments.of(new ExprLongValue(1), ExprNullValue.of()),
        Arguments.of(new ExprLongValue(919950501), ExprNullValue.of()),
        Arguments.of(new ExprStringValue("2009-11-29 00:00:00"), new ExprLongValue(63426672000L)),
        Arguments.of(new ExprStringValue("2009-11-29 13:43:32"), new ExprLongValue(63426721412L)),
        Arguments.of(new ExprDateValue("2009-11-29"), new ExprLongValue(63426672000L)),
        Arguments.of(
            new ExprTimestampValue("2009-11-29 13:43:32"), new ExprLongValue(63426721412L)));
  }

  @ParameterizedTest
  @MethodSource("getTestDataForToSeconds")
  public void testToSeconds(ExprValue arg, ExprValue expected) {
    FunctionExpression expr = DSL.to_seconds(DSL.literal(arg));
    assertEquals(LONG, expr.type());
    assertEquals(expected, eval(expr));
  }

  @Test
  public void testToSecondsWithTimeType() {
    FunctionExpression expr =
        DSL.to_seconds(functionProperties, DSL.literal(new ExprTimeValue("10:11:12")));

    long expected =
        SECONDS_FROM_0001_01_01_TO_EPOCH_START
            + LocalDate.now(functionProperties.getQueryStartClock())
                .toEpochSecond(LocalTime.parse("10:11:12"), ZoneOffset.UTC);

    assertEquals(expected, eval(expr).longValue());
  }

  private static Stream<Arguments> getInvalidTestDataForToSeconds() {
    return Stream.of(
        Arguments.of(new ExprStringValue("asdfasdf")),
        Arguments.of(new ExprStringValue("2000-14-10")),
        Arguments.of(new ExprStringValue("2000-10-45")),
        Arguments.of(new ExprStringValue("2000-10-10 70:00:00")),
        Arguments.of(new ExprStringValue("2000-10-10 00:70:00")),
        Arguments.of(new ExprStringValue("2000-10-10 00:00:70")));
  }

  @ParameterizedTest
  @MethodSource("getInvalidTestDataForToSeconds")
  public void testToSecondsInvalidArg(ExprValue arg) {
    FunctionExpression expr = DSL.to_seconds(DSL.literal(arg));
    assertThrows(SemanticCheckException.class, () -> eval(expr));
  }

  @Test
  public void testToSecondsWithDateAdd() {
    LocalDate date = LocalDate.of(2000, 1, 1);
    FunctionExpression dateExpr = DSL.to_seconds(DSL.literal(new ExprDateValue(date)));
    long addedSeconds = SECONDS_PER_DAY;
    long expected = eval(dateExpr).longValue() + addedSeconds;

    FunctionExpression dateAddExpr =
        DSL.date_add(
            DSL.literal(new ExprDateValue(date)),
            DSL.literal(new ExprIntervalValue(Duration.ofSeconds(addedSeconds))));

    long result = eval(DSL.to_seconds(DSL.literal(eval(dateAddExpr)))).longValue();

    assertEquals(expected, result);
  }

  private ExprValue eval(Expression expression) {
    return expression.valueOf();
  }
}
