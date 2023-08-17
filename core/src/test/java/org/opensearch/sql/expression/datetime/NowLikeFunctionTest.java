/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_ID;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalUnit;
import java.util.List;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsNot;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionTestBase;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.function.FunctionProperties;

class NowLikeFunctionTest extends ExpressionTestBase {
  @Test
  void now() {
    test_now_like_functions(
        DSL::now,
        TIMESTAMP,
        false,
        () -> LocalDateTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void current_timestamp() {
    test_now_like_functions(
        DSL::current_timestamp,
        TIMESTAMP,
        false,
        () -> LocalDateTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void localtimestamp() {
    test_now_like_functions(
        DSL::localtimestamp,
        TIMESTAMP,
        false,
        () -> LocalDateTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void localtime() {
    test_now_like_functions(
        DSL::localtime,
        TIMESTAMP,
        false,
        () -> LocalDateTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void sysdate() {
    test_now_like_functions(DSL::sysdate, TIMESTAMP, true, LocalDateTime::now);
  }

  @Test
  void curtime() {
    test_now_like_functions(
        DSL::curtime, TIME, false, () -> LocalTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void currdate() {

    test_now_like_functions(
        DSL::curdate, DATE, false, () -> LocalDate.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void current_time() {
    test_now_like_functions(
        DSL::current_time,
        TIME,
        false,
        () -> LocalTime.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void current_date() {
    test_now_like_functions(
        DSL::current_date,
        DATE,
        false,
        () -> LocalDate.now(functionProperties.getQueryStartClock()));
  }

  @Test
  void utc_date() {
    test_now_like_functions(
        DSL::utc_date, DATE, false, () -> utcDateTimeNow(functionProperties).toLocalDate());
  }

  @Test
  void utc_time() {
    test_now_like_functions(
        DSL::utc_time, TIME, false, () -> utcDateTimeNow(functionProperties).toLocalTime());
  }

  @Test
  void utc_timestamp() {
    test_now_like_functions(
        DSL::utc_timestamp, TIMESTAMP, false, () -> utcDateTimeNow(functionProperties));
  }

  private static LocalDateTime utcDateTimeNow(FunctionProperties functionProperties) {
    ZonedDateTime zonedDateTime =
        LocalDateTime.now(functionProperties.getQueryStartClock())
            .atZone(TimeZone.getDefault().toZoneId());
    return zonedDateTime.withZoneSameInstant(UTC_ZONE_ID).toLocalDateTime();
  }

  /**
   * Check how NOW-like functions are processed.
   *
   * @param function Function
   * @param resType Return type
   * @param hasFsp Whether function has fsp argument
   * @param referenceGetter A callback to get reference value
   */
  void test_now_like_functions(
      BiFunction<FunctionProperties, Expression[], FunctionExpression> function,
      ExprCoreType resType,
      Boolean hasFsp,
      Supplier<Temporal> referenceGetter) {
    // Check return types:
    // `func()`
    FunctionExpression expr = function.apply(functionProperties, new Expression[] {});
    assertEquals(resType, expr.type());
    if (hasFsp) {
      // `func(fsp = 0)`
      expr = function.apply(functionProperties, new Expression[] {DSL.literal(0)});
      assertEquals(resType, expr.type());
      // `func(fsp = 6)`
      expr = function.apply(functionProperties, new Expression[] {DSL.literal(6)});
      assertEquals(resType, expr.type());

      for (var wrongFspValue : List.of(-1, 10)) {
        var exception =
            assertThrows(
                IllegalArgumentException.class,
                () ->
                    function
                        .apply(functionProperties, new Expression[] {DSL.literal(wrongFspValue)})
                        .valueOf());
        assertEquals(
            String.format("Invalid `fsp` value: %d, allowed 0 to 6", wrongFspValue),
            exception.getMessage());
      }
    }

    // Check how calculations are precise:
    // `func()`
    Temporal sample = extractValue(function.apply(functionProperties, new Expression[] {}));
    Temporal reference = referenceGetter.get();
    long maxDiff = 1;
    TemporalUnit unit = resType.isCompatible(DATE) ? ChronoUnit.DAYS : ChronoUnit.SECONDS;
    assertThat(sample, isCloseTo(reference, maxDiff, unit));
    if (hasFsp) {
      // `func(fsp)`
      Temporal value =
          extractValue(function.apply(functionProperties, new Expression[] {DSL.literal(0)}));
      assertThat(referenceGetter.get(), isCloseTo(value, maxDiff, unit));
    }
  }

  static Matcher<Temporal> isCloseTo(Temporal reference, long maxDiff, TemporalUnit units) {
    return new BaseMatcher<>() {
      @Override
      public void describeTo(Description description) {
        description
            .appendText("value between ")
            .appendValue(reference.minus(maxDiff, units))
            .appendText(" and ")
            .appendValue(reference.plus(maxDiff, units));
      }

      @Override
      public boolean matches(Object value) {
        if (value instanceof Temporal) {
          Temporal temporalValue = (Temporal) value;
          long diff = reference.until(temporalValue, units);
          return Math.abs(diff) <= maxDiff;
        }
        return false;
      }
    };
  }

  @TestFactory
  Stream<DynamicTest> constantValueTestFactory() {
    BiFunction<String, Function<FunctionProperties, FunctionExpression>, DynamicTest> buildTest =
        (name, action) ->
            DynamicTest.dynamicTest(
                String.format("multiple_invocations_same_value_test[%s]", name),
                () -> {
                  var v1 = extractValue(action.apply(functionProperties));
                  Thread.sleep(1000);
                  var v2 = extractValue(action.apply(functionProperties));
                  assertEquals(v1, v2);
                });
    return Stream.of(
        buildTest.apply("now", DSL::now),
        buildTest.apply("current_timestamp", DSL::current_timestamp),
        buildTest.apply("current_time", DSL::current_time),
        buildTest.apply("curdate", DSL::curdate),
        buildTest.apply("curtime", DSL::curtime),
        buildTest.apply("localtimestamp", DSL::localtimestamp),
        buildTest.apply("localtime", DSL::localtime));
  }

  @Test
  void sysdate_multiple_invocations_differ() throws InterruptedException {
    var v1 = extractValue(DSL.sysdate(functionProperties));
    Thread.sleep(1000);
    var v2 = extractValue(DSL.sysdate(functionProperties));
    assertThat(v1, IsNot.not(isCloseTo(v2, 1, ChronoUnit.NANOS)));
  }

  private Temporal extractValue(FunctionExpression func) {
    switch ((ExprCoreType) func.type()) {
      case DATE:
        return func.valueOf().dateValue();
      case TIMESTAMP:
        return LocalDateTime.ofInstant(func.valueOf().timestampValue(), ZoneOffset.UTC);
      case TIME:
        return func.valueOf().timeValue();
        // unreachable code
      default:
        throw new IllegalArgumentException(String.format("%s", func.type()));
    }
  }
}
