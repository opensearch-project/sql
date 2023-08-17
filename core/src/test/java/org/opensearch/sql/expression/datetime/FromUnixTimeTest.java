/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.expression.DSL;

public class FromUnixTimeTest extends DateTimeTestBase {

  private static Stream<Arguments> getLongSamples() {
    return Stream.of(
        Arguments.of(0L),
        Arguments.of(1L),
        Arguments.of(1447430881L),
        Arguments.of(2147483647L),
        Arguments.of(1662577241L));
  }

  /**
   * Test processing different Long values.
   *
   * @param value a value
   */
  @ParameterizedTest
  @MethodSource("getLongSamples")
  public void checkOfLong(Long value) {
    assertEquals(
        LocalDateTime.of(1970, 1, 1, 0, 0, 0).plus(value, ChronoUnit.SECONDS), fromUnixTime(value));
    assertEquals(
        LocalDateTime.of(1970, 1, 1, 0, 0, 0).plus(value, ChronoUnit.SECONDS),
        LocalDateTime.ofInstant(
            eval(fromUnixTime(DSL.literal(new ExprLongValue(value)))).timestampValue(),
                ZoneOffset.UTC));
  }

  private static Stream<Arguments> getDoubleSamples() {
    return Stream.of(
        Arguments.of(0.123d),
        Arguments.of(100500.100500d),
        Arguments.of(1447430881.564d),
        Arguments.of(2147483647.451232d),
        Arguments.of(1662577241.d));
  }

  /**
   * Test processing different Double values.
   *
   * @param value a value
   */
  @ParameterizedTest
  @MethodSource("getDoubleSamples")
  public void checkOfDouble(Double value) {
    var intPart = Math.round(Math.floor(value));
    var fracPart = value - intPart;
    var valueAsString = new DecimalFormat("0.#").format(value);

    assertEquals(
        LocalDateTime.ofEpochSecond(intPart, (int) Math.round(fracPart * 1E9), ZoneOffset.UTC),
        fromUnixTime(value),
        valueAsString);
    assertEquals(
        LocalDateTime.ofEpochSecond(intPart, (int) Math.round(fracPart * 1E9), ZoneOffset.UTC),
        LocalDateTime.ofInstant(
            eval(fromUnixTime(DSL.literal(new ExprDoubleValue(value)))).timestampValue(),
                ZoneOffset.UTC),
        valueAsString);
  }

  // We are testing all different formats and combinations, because they are already tested

  private static Stream<Arguments> getLongSamplesWithFormat() {
    return Stream.of(
        Arguments.of(0L, "%c", "01"), // 1970-01-01 00:00:00, %c - 2 digit month
        Arguments.of(1L, "%Y", "1970"), // 1970-01-01 00:00:01, %Y - 4 digit year
        Arguments.of(1447430881L, "%s", "01"), // 2015-11-13 16:08:01, %s - second
        Arguments.of(2147483647L, "%T", "03:14:07"), // 2038-01-19 03:14:07, %T - time
        Arguments.of(1662577241L, "%d", "07") // 1662577241, %d - day of the month
        );
  }

  /**
   * Test processing different Long values with format.
   *
   * @param value a value
   * @param format a format
   * @param expected expected result
   */
  @ParameterizedTest
  @MethodSource("getLongSamplesWithFormat")
  public void checkOfLongWithFormat(Long value, String format, String expected) {
    assertEquals(expected, fromUnixTime(value, format));
    assertEquals(
        expected,
        eval(fromUnixTime(
                DSL.literal(new ExprLongValue(value)), DSL.literal(new ExprStringValue(format))))
            .stringValue());
  }

  private static Stream<Arguments> getDoubleSamplesWithFormat() {
    return Stream.of(
        Arguments.of(0.123d, "%f", "123000"), // 1970-01-01 00:00:00.123, %f - microseconds
        Arguments.of(100500.1005d, "%W", "Friday"), // 1970-01-02 03:55:00.1005, %W - Weekday name
        Arguments.of(1447430881.56d, "%M", "November"), // 2015-11-13 16:08:01.56, %M - Month name
        Arguments.of(2147483647.42d, "%j", "019"), // 2038-01-19 03:14:07.42, %j - day of the year
        Arguments.of(1662577241.d, "%l", "7") // 2022-09-07 19:00:41, %l - 12 hour clock, no 0 pad
        );
  }

  /**
   * Test processing different Double values with format.
   *
   * @param value a value
   * @param format a format
   * @param expected expected result
   */
  @ParameterizedTest
  @MethodSource("getDoubleSamplesWithFormat")
  public void checkOfDoubleWithFormat(Double value, String format, String expected) {
    assertEquals(expected, fromUnixTime(value, format));
    assertEquals(
        expected,
        eval(fromUnixTime(
                DSL.literal(new ExprDoubleValue(value)), DSL.literal(new ExprStringValue(format))))
            .stringValue());
  }

  @Test
  public void checkInvalidFormat() {
    assertEquals(
        new ExprStringValue("q"), fromUnixTime(DSL.literal(0L), DSL.literal("%q")).valueOf());
    assertEquals(new ExprStringValue(""), fromUnixTime(DSL.literal(0L), DSL.literal("")).valueOf());
  }

  @Test
  public void checkValueOutsideOfTheRangeWithoutFormat() {
    assertEquals(ExprNullValue.of(), fromUnixTime(DSL.literal(-1L)).valueOf());
    assertEquals(ExprNullValue.of(), fromUnixTime(DSL.literal(-1.5d)).valueOf());
    assertEquals(ExprNullValue.of(), fromUnixTime(DSL.literal(32536771200L)).valueOf());
    assertEquals(ExprNullValue.of(), fromUnixTime(DSL.literal(32536771200d)).valueOf());
  }

  @Test
  public void checkInsideTheRangeWithoutFormat() {
    assertNotEquals(ExprNullValue.of(), fromUnixTime(DSL.literal(32536771199L)).valueOf());
    assertNotEquals(ExprNullValue.of(), fromUnixTime(DSL.literal(32536771199d)).valueOf());
  }

  @Test
  public void checkValueOutsideOfTheRangeWithFormat() {
    assertEquals(
        ExprNullValue.of(), fromUnixTime(DSL.literal(32536771200L), DSL.literal("%d")).valueOf());
    assertEquals(
        ExprNullValue.of(), fromUnixTime(DSL.literal(32536771200d), DSL.literal("%d")).valueOf());
  }

  @Test
  public void checkInsideTheRangeWithFormat() {
    assertNotEquals(
        ExprNullValue.of(), fromUnixTime(DSL.literal(32536771199L), DSL.literal("%d")).valueOf());
    assertNotEquals(
        ExprNullValue.of(), fromUnixTime(DSL.literal(32536771199d), DSL.literal("%d")).valueOf());
  }

  @Test
  public void checkNullOrNegativeValues() {
    assertEquals(ExprNullValue.of(), fromUnixTime(DSL.literal(ExprNullValue.of())).valueOf());
    assertEquals(ExprNullValue.of(), fromUnixTime(DSL.literal(-1L), DSL.literal("%d")).valueOf());
    assertEquals(ExprNullValue.of(), fromUnixTime(DSL.literal(-1.5d), DSL.literal("%d")).valueOf());
    assertEquals(
        ExprNullValue.of(),
        fromUnixTime(DSL.literal(42L), DSL.literal(ExprNullValue.of())).valueOf());
    assertEquals(
        ExprNullValue.of(),
        fromUnixTime(DSL.literal(ExprNullValue.of()), DSL.literal("%d")).valueOf());
    assertEquals(
        ExprNullValue.of(),
        fromUnixTime(DSL.literal(ExprNullValue.of()), DSL.literal(ExprNullValue.of())).valueOf());
  }
}
