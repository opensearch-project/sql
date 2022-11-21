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
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;


class NowLikeFunctionTest extends ExpressionTestBase {
  @Test
  void now() {
    test_now_like_functions(DSL::now,
        DATETIME,
        false,
        () -> LocalDateTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void current_timestamp() {
    test_now_like_functions(DSL::current_timestamp, DATETIME, false,
        () -> LocalDateTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void localtimestamp() {
    test_now_like_functions(DSL::localtimestamp, DATETIME, false,
        () -> LocalDateTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void localtime() {
    test_now_like_functions(DSL::localtime, DATETIME, false,
        () -> LocalDateTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void sysdate() {
    test_now_like_functions(DSL::sysdate, DATETIME, true, LocalDateTime::now);
  }

  @Test
  void curtime() {
    test_now_like_functions(DSL::curtime, TIME, false,
        () -> LocalTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void currdate() {

    test_now_like_functions(DSL::curdate,
        DATE, false,
        () -> LocalDate.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void current_time() {
    test_now_like_functions(DSL::current_time,
        TIME,
        false,
        () -> LocalTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void current_date() {
    test_now_like_functions(DSL::current_date, DATE, false,
        () -> LocalDate.now(functionProperties.getQueryStartClock()));
  }

  /**
   * Check how NOW-like functions are processed.
   *
   * @param function        Function
   * @param resType         Return type
   * @param hasFsp          Whether function has fsp argument
   * @param referenceGetter A callback to get reference value
   */
  void test_now_like_functions(Function<Expression[], FunctionExpression> function,
                               ExprCoreType resType,
                               Boolean hasFsp,
                               Supplier<Temporal> referenceGetter) {
    // Check return types:
    // `func()`
    FunctionExpression expr = function.apply(new Expression[] {});
    assertEquals(resType, expr.type());
    if (hasFsp) {
      // `func(fsp = 0)`
      expr = function.apply(new Expression[] {DSL.literal(0)});
      assertEquals(resType, expr.type());
      // `func(fsp = 6)`
      expr = function.apply(new Expression[] {DSL.literal(6)});
      assertEquals(resType, expr.type());

      for (var wrongFspValue : List.of(-1, 10)) {
        var exception = assertThrows(IllegalArgumentException.class,
            () -> function.apply(
                new Expression[] {DSL.literal(wrongFspValue)}).valueOf());
        assertEquals(String.format("Invalid `fsp` value: %d, allowed 0 to 6", wrongFspValue),
            exception.getMessage());
      }
    }

    // Check how calculations are precise:
    // `func()`
    Temporal sample = extractValue(function.apply(new Expression[] {}));
    Temporal reference = referenceGetter.get();
    assertTrue(Math.abs(getDiff(reference, sample)) <= 1);
    if (hasFsp) {
      // `func(fsp)`
      assertTrue(Math.abs(getDiff(
          extractValue(function.apply(new Expression[] {DSL.literal(0)})),
          referenceGetter.get()
      )) <= 1);
    }
  }

  @TestFactory
  Stream<DynamicTest> constantValueTestFactory() {
    BiFunction<String, Callable<FunctionExpression>, DynamicTest> buildTest = (name, action) ->
        DynamicTest.dynamicTest(
          String.format("multiple_invocations_same_value_test[%s]", name),
          () -> {
            var v1 = extractValue(action.call());
            Thread.sleep(1000);
            var v2 = extractValue(action.call());
            assertEquals(v1, v2);
          }
      );
    return Stream.of(
        buildTest.apply("now", DSL::now),
        buildTest.apply("current_timestamp", DSL::current_timestamp),
        buildTest.apply("current_time", DSL::current_time),
        buildTest.apply("curdate", DSL::curdate),
        buildTest.apply("curtime", DSL::curtime),
        buildTest.apply("localtimestamp", DSL::localtimestamp),
        buildTest.apply("localtime", DSL::localtime)
    );
  }

  @Test
  void sysdate_multiple_invocations_differ() throws InterruptedException {
    var v1 = extractValue(DSL.sysdate());
    Thread.sleep(1000);
    var v2 = extractValue(DSL.sysdate());
    assertEquals(1, getDiff(v1, v2));
  }

  private Temporal extractValue(FunctionExpression func) {
    switch ((ExprCoreType) func.type()) {
      case DATE:
        return func.valueOf().dateValue();
      case DATETIME:
        return func.valueOf().datetimeValue();
      case TIME:
        return func.valueOf().timeValue();
      // unreachable code
      default:
        throw new IllegalArgumentException(String.format("%s", func.type()));
    }
  }

  private long getDiff(Temporal sample, Temporal reference) {
    if (sample instanceof LocalDate) {
      return Period.between((LocalDate) sample, (LocalDate) reference).getDays();
    }
    return Duration.between(sample, reference).toSeconds();
  }
}
