/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.temporal.Temporal;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;


public class NowLikeFunctionTest extends ExpressionTestBase {
  private static Stream<Arguments> functionNames() {
    return Stream.of(
        Arguments.of((Function<Expression[], FunctionExpression>) DSL::now,
            "now", DATETIME, false, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>) DSL::current_timestamp,
            "current_timestamp", DATETIME, false, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>) DSL::localtimestamp,
            "localtimestamp", DATETIME, false, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>) DSL::localtime,
            "localtime", DATETIME, false, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>) DSL::sysdate,
            "sysdate", DATETIME, true, (Supplier<Temporal>)LocalDateTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>) DSL::curtime,
            "curtime", TIME, false, (Supplier<Temporal>)LocalTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>) DSL::current_time,
            "current_time", TIME, false, (Supplier<Temporal>)LocalTime::now),
        Arguments.of((Function<Expression[], FunctionExpression>) DSL::curdate,
            "curdate", DATE, false, (Supplier<Temporal>)LocalDate::now),
        Arguments.of((Function<Expression[], FunctionExpression>) DSL::current_date,
            "current_date", DATE, false, (Supplier<Temporal>)LocalDate::now));
  }

  private Temporal extractValue(FunctionExpression func) {
    switch ((ExprCoreType)func.type()) {
      case DATE: return func.valueOf().dateValue();
      case DATETIME: return func.valueOf().datetimeValue();
      case TIME: return func.valueOf().timeValue();
      // unreachable code
      default: throw new IllegalArgumentException(String.format("%s", func.type()));
    }
  }

  private long getDiff(Temporal sample, Temporal reference) {
    if (sample instanceof LocalDate) {
      return Period.between((LocalDate) sample, (LocalDate) reference).getDays();
    }
    return Duration.between(sample, reference).toSeconds();
  }

  /**
   * Check how NOW-like functions are processed.
   * @param function Function
   * @param name Function name
   * @param resType Return type
   * @param hasFsp Whether function has fsp argument
   * @param referenceGetter A callback to get reference value
   */
  @ParameterizedTest(name = "{1}")
  @MethodSource("functionNames")
  public void test_now_like_functions(Function<Expression[], FunctionExpression> function,
                       @SuppressWarnings("unused")  // Used in the test name above
                       String name,
                       ExprCoreType resType,
                       Boolean hasFsp,
                       Supplier<Temporal> referenceGetter) {
    // Check return types:
    // `func()`
    FunctionExpression expr = function.apply(new Expression[]{});
    assertEquals(resType, expr.type());
    if (hasFsp) {
      // `func(fsp = 0)`
      expr = function.apply(new Expression[]{DSL.literal(0)});
      assertEquals(resType, expr.type());
      // `func(fsp = 6)`
      expr = function.apply(new Expression[]{DSL.literal(6)});
      assertEquals(resType, expr.type());

      for (var wrongFspValue: List.of(-1, 10)) {
        var exception = assertThrows(IllegalArgumentException.class,
            () -> function.apply(new Expression[]{DSL.literal(wrongFspValue)}).valueOf());
        assertEquals(String.format("Invalid `fsp` value: %d, allowed 0 to 6", wrongFspValue),
            exception.getMessage());
      }
    }

    // Check how calculations are precise:
    // `func()`
    assertTrue(Math.abs(getDiff(
            extractValue(function.apply(new Expression[]{})),
            referenceGetter.get()
        )) <= 1);
    if (hasFsp) {
      // `func(fsp)`
      assertTrue(Math.abs(getDiff(
              extractValue(function.apply(new Expression[]{DSL.literal(0)})),
              referenceGetter.get()
      )) <= 1);
    }
  }
}
