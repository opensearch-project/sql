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
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;


class NowLikeFunctionTest extends ExpressionTestBase {
  @Test
  void now() {
    test_now_like_functions(dsl::now, DATETIME, false, LocalDateTime::now);
  }

  @Test
  void current_timestamp() {
    test_now_like_functions(dsl::current_timestamp, DATETIME, false, LocalDateTime::now);
  }

  @Test
  void localtimestamp() {
    test_now_like_functions(dsl::localtimestamp, DATETIME, false, LocalDateTime::now);
  }

  @Test
  void localtime() {
    test_now_like_functions(dsl::localtime, DATETIME, false, LocalDateTime::now);
  }

  @Test
  void sysdate() {
    test_now_like_functions(dsl::sysdate, DATETIME, true, LocalDateTime::now);
  }

  @Test
  void curtime() {
    test_now_like_functions(dsl::curtime, TIME, false, LocalTime::now);
  }

  @Test
  void currdate() {
    test_now_like_functions(dsl::curdate, DATE, false, LocalDate::now);
  }

  @Test
  void current_time() {
    test_now_like_functions(dsl::current_time, TIME, false, LocalTime::now);
  }

  @Test
  void current_date() {
    test_now_like_functions(dsl::current_date, DATE, false, LocalDate::now);
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
            () -> function.apply(new Expression[] {DSL.literal(wrongFspValue)}).valueOf(null));
        assertEquals(String.format("Invalid `fsp` value: %d, allowed 0 to 6", wrongFspValue),
            exception.getMessage());
      }
    }

    // Check how calculations are precise:
    // `func()`
    assertTrue(Math.abs(getDiff(
        extractValue(function.apply(new Expression[] {})),
        referenceGetter.get()
    )) <= 1);
    if (hasFsp) {
      // `func(fsp)`
      assertTrue(Math.abs(getDiff(
          extractValue(function.apply(new Expression[] {DSL.literal(0)})),
          referenceGetter.get()
      )) <= 1);
    }
  }


  private Temporal extractValue(FunctionExpression func) {
    switch ((ExprCoreType) func.type()) {
      case DATE:
        return func.valueOf(null).dateValue();
      case DATETIME:
        return func.valueOf(null).datetimeValue();
      case TIME:
        return func.valueOf(null).timeValue();
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
