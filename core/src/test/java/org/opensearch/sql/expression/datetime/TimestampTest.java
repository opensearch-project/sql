/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValueUtils;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.ExpressionTestBase;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class TimestampTest extends ExpressionTestBase {

  @Test
  public void timestamp_one_arg_string() {
    var expr = DSL.timestamp(functionProperties, DSL.literal("1961-04-12 09:07:00"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(new ExprTimestampValue("1961-04-12 09:07:00"), expr.valueOf());

    expr = DSL.timestamp(functionProperties, DSL.literal("1961-04-12 09:07:00.123456"));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(
        LocalDateTime.of(1961, 4, 12, 9, 7, 0, 123456000),
        expr.valueOf().timestampValue().atZone(ZoneOffset.UTC).toLocalDateTime());
  }

  /**
   * Check that `TIMESTAMP` function throws an exception on incorrect string input.
   *
   * @param value A value.
   * @param testName A test name.
   */
  @ParameterizedTest(name = "{1}")
  @CsvSource({
    "1984-02-30 12:20:42, Feb 30th",
    "1984-02-10 24:00:00, 24:00:00",
    "84-02-10 12:20:42, 2 digit year"
  })
  public void timestamp_one_arg_string_invalid_format(String value, String testName) {
    // exception thrown from ExprTimestampValue(String) CTOR
    var exception =
        assertThrows(
            SemanticCheckException.class,
            () -> DSL.timestamp(functionProperties, DSL.literal(value)).valueOf());
    assertEquals(
        String.format(
            "timestamp:%s in unsupported format, please " + "use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'",
            value),
        exception.getMessage());
  }

  @Test
  public void timestamp_one_arg_time() {
    var expr = DSL.timestamp(functionProperties, DSL.time(DSL.literal("22:33:44")));
    assertEquals(TIMESTAMP, expr.type());
    var refValue =
        LocalDate.now().atTime(LocalTime.of(22, 33, 44)).atZone(ZoneOffset.UTC).toInstant();
    assertEquals(new ExprTimestampValue(refValue), expr.valueOf());
  }

  @Test
  public void timestamp_one_arg_date() {
    var expr = DSL.timestamp(functionProperties, DSL.date(DSL.literal("2077-12-15")));
    assertEquals(TIMESTAMP, expr.type());
    var refValue = LocalDate.of(2077, 12, 15).atStartOfDay().atZone(ZoneOffset.UTC).toInstant();
    assertEquals(new ExprTimestampValue(refValue), expr.valueOf());
  }

  @Test
  public void timestamp_one_arg_timestamp() {
    var refValue = new ExprTimestampValue(Instant.ofEpochSecond(10050042));
    var expr =
        DSL.timestamp(functionProperties, DSL.timestamp(functionProperties, DSL.literal(refValue)));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(refValue, expr.valueOf());
  }

  private static Instant dateTime2Instant(LocalDateTime dt) {
    return dt.atZone(ZoneOffset.UTC).toInstant();
  }

  private static ExprTimestampValue dateTime2ExprTs(LocalDateTime dt) {
    return new ExprTimestampValue(dateTime2Instant(dt));
  }

  private static Stream<Arguments> getTestData() {
    var today = LocalDate.now();
    // First argument of `TIMESTAMP` function, second argument and expected result value
    return Stream.of(
        // STRING and STRING/DATE/TIME/DATETIME/TIMESTAMP
        Arguments.of(
            "1961-04-12 09:07:00",
            "2077-12-15 01:48:00",
            dateTime2ExprTs(LocalDateTime.of(1961, 4, 12, 10, 55, 0))),
        Arguments.of(
            "1984-02-10 12:20:42",
            LocalDate.of(2077, 12, 21),
            dateTime2ExprTs(LocalDateTime.of(1984, 2, 10, 12, 20, 42))),
        Arguments.of(
            "1961-04-12 09:07:00",
            LocalTime.of(1, 48),
            dateTime2ExprTs(LocalDateTime.of(1961, 4, 12, 10, 55, 0))),
        Arguments.of(
            "2020-12-31 17:30:00",
            LocalDateTime.of(2077, 12, 21, 12, 20, 42),
            dateTime2ExprTs(LocalDateTime.of(2021, 1, 1, 5, 50, 42))),
        Arguments.of(
            "2020-12-31 17:30:00",
            Instant.ofEpochSecond(42),
            dateTime2ExprTs(LocalDateTime.of(2020, 12, 31, 17, 30, 42))),
        // DATE and STRING/DATE/TIME/DATETIME/TIMESTAMP
        Arguments.of(
            LocalDate.of(2077, 12, 21),
            "2077-12-15 01:48:00",
            dateTime2ExprTs(LocalDateTime.of(2077, 12, 21, 1, 48, 0))),
        Arguments.of(
            LocalDate.of(2077, 12, 21),
            LocalDate.of(1984, 2, 3),
            dateTime2ExprTs(LocalDateTime.of(2077, 12, 21, 0, 0, 0))),
        Arguments.of(
            LocalDate.of(2077, 12, 21),
            LocalTime.of(22, 33, 44),
            dateTime2ExprTs(LocalDateTime.of(2077, 12, 21, 22, 33, 44))),
        Arguments.of(
            LocalDate.of(2077, 12, 21),
            LocalDateTime.of(1999, 9, 9, 22, 33, 44),
            dateTime2ExprTs(LocalDateTime.of(2077, 12, 21, 22, 33, 44))),
        Arguments.of(
            LocalDate.of(2077, 12, 21),
            Instant.ofEpochSecond(42),
            dateTime2ExprTs(LocalDateTime.of(2077, 12, 21, 0, 0, 42))),
        // TIME and STRING/DATE/TIME/DATETIME/TIMESTAMP
        Arguments.of(
            LocalTime.of(9, 7, 0),
            "2077-12-15 01:48:00",
            dateTime2ExprTs(today.atTime(LocalTime.of(10, 55, 0)))),
        Arguments.of(
            LocalTime.of(12, 20, 42),
            LocalDate.of(2077, 12, 21),
            dateTime2ExprTs(today.atTime(LocalTime.of(12, 20, 42)))),
        Arguments.of(
            LocalTime.of(9, 7, 0),
            LocalTime.of(1, 48),
            dateTime2ExprTs(today.atTime(LocalTime.of(10, 55, 0)))),
        Arguments.of(
            LocalTime.of(17, 30, 0),
            LocalDateTime.of(2077, 12, 21, 12, 20, 42),
            dateTime2ExprTs(today.plusDays(1).atTime(LocalTime.of(5, 50, 42)))),
        Arguments.of(
            LocalTime.of(17, 30, 0),
            Instant.ofEpochSecond(42),
            dateTime2ExprTs(today.atTime(LocalTime.of(17, 30, 42)))),
        // DATETIME and STRING/DATE/TIME/DATETIME/TIMESTAMP
        Arguments.of(
            LocalDateTime.of(1961, 4, 12, 9, 7, 0),
            "2077-12-15 01:48:00",
            dateTime2ExprTs(LocalDateTime.of(1961, 4, 12, 10, 55, 0))),
        Arguments.of(
            LocalDateTime.of(1984, 2, 10, 12, 20, 42),
            LocalDate.of(2077, 12, 21),
            dateTime2ExprTs(LocalDateTime.of(1984, 2, 10, 12, 20, 42))),
        Arguments.of(
            LocalDateTime.of(1961, 4, 12, 9, 7, 0),
            LocalTime.of(1, 48),
            dateTime2ExprTs(LocalDateTime.of(1961, 4, 12, 10, 55, 0))),
        Arguments.of(
            LocalDateTime.of(2020, 12, 31, 17, 30, 0),
            LocalDateTime.of(2077, 12, 21, 12, 20, 42),
            dateTime2ExprTs(LocalDateTime.of(2021, 1, 1, 5, 50, 42))),
        Arguments.of(
            LocalDateTime.of(2020, 12, 31, 17, 30, 0),
            Instant.ofEpochSecond(42),
            dateTime2ExprTs(LocalDateTime.of(2020, 12, 31, 17, 30, 42))),
        // TIMESTAMP and STRING/DATE/TIME/DATETIME/TIMESTAMP
        Arguments.of(
            dateTime2Instant(LocalDateTime.of(1961, 4, 12, 9, 7, 0)),
            "2077-12-15 01:48:00",
            dateTime2ExprTs(LocalDateTime.of(1961, 4, 12, 10, 55, 0))),
        Arguments.of(
            dateTime2Instant(LocalDateTime.of(1984, 2, 10, 12, 20, 42)),
            LocalDate.of(2077, 12, 21),
            dateTime2ExprTs(LocalDateTime.of(1984, 2, 10, 12, 20, 42))),
        Arguments.of(
            dateTime2Instant(LocalDateTime.of(1961, 4, 12, 9, 7, 0)),
            LocalTime.of(1, 48),
            dateTime2ExprTs(LocalDateTime.of(1961, 4, 12, 10, 55, 0))),
        Arguments.of(
            dateTime2Instant(LocalDateTime.of(2020, 12, 31, 17, 30, 0)),
            LocalDateTime.of(2077, 12, 21, 12, 20, 42),
            dateTime2ExprTs(LocalDateTime.of(2021, 1, 1, 5, 50, 42))),
        Arguments.of(
            dateTime2Instant(LocalDateTime.of(2020, 12, 31, 17, 30, 0)),
            Instant.ofEpochSecond(42),
            dateTime2ExprTs(LocalDateTime.of(2020, 12, 31, 17, 30, 42))));
  }

  /**
   * Test `TIMESTAMP` function which takes 2 arguments with input of different types.
   *
   * @param arg1 First argument to be passed to `TIMESTAMP` function.
   * @param arg2 Second argument to be passed to `TIMESTAMP` function.
   * @param expected The expected result.
   */
  @ParameterizedTest
  @MethodSource("getTestData")
  public void timestamp_with_two_args(Object arg1, Object arg2, ExprTimestampValue expected) {
    var expr =
        DSL.timestamp(
            functionProperties,
            DSL.literal(ExprValueUtils.fromObjectValue(arg1)),
            DSL.literal(ExprValueUtils.fromObjectValue(arg2)));
    assertEquals(TIMESTAMP, expr.type());
    assertEquals(expected, expr.valueOf());
  }
}
