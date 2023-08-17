/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_OFFSET;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoField;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.DSL;

public class UnixTimeStampTest extends DateTimeTestBase {

  @Test
  public void checkNoArgs() {

    final long expected = functionProperties.getQueryStartClock().millis() / 1000L;
    assertEquals(expected, unixTimeStamp());
    assertEquals(expected, eval(unixTimeStampExpr()).longValue());
  }

  private static Stream<Arguments> getDateSamples() {
    return Stream.of(
        Arguments.of(LocalDate.of(1984, 1, 1)),
        Arguments.of(LocalDate.of(2000, 2, 29)),
        Arguments.of(LocalDate.of(1999, 12, 31)),
        Arguments.of(LocalDate.of(2004, 2, 29)),
        Arguments.of(LocalDate.of(2100, 2, 28)),
        Arguments.of(LocalDate.of(2012, 2, 21)));
  }

  /**
   * Check processing valid values of type LocalDate.
   *
   * @param value a value
   */
  @ParameterizedTest
  @MethodSource("getDateSamples")
  public void checkOfDate(LocalDate value) {
    assertEquals(value.getLong(ChronoField.EPOCH_DAY) * 24 * 3600, unixTimeStampOf(value));
    assertEquals(
        value.getLong(ChronoField.EPOCH_DAY) * 24 * 3600,
        eval(unixTimeStampOf(DSL.literal(new ExprDateValue(value)))).longValue());
  }

  private static Stream<Arguments> getDateTimeSamples() {
    return Stream.of(
        Arguments.of(LocalDateTime.of(1984, 1, 1, 1, 1)),
        Arguments.of(LocalDateTime.of(2000, 2, 29, 22, 54)),
        Arguments.of(LocalDateTime.of(1999, 12, 31, 23, 59)),
        Arguments.of(LocalDateTime.of(2004, 2, 29, 7, 40)),
        Arguments.of(LocalDateTime.of(2100, 2, 28, 13, 14)),
        Arguments.of(LocalDateTime.of(2012, 2, 21, 0, 0)));
  }

  /**
   * Check processing valid values of type LocalDateTime.
   *
   * @param value a value
   */
  @ParameterizedTest
  @MethodSource("getDateTimeSamples")
  public void checkOfDateTime(LocalDateTime value) {
    assertEquals(value.toEpochSecond(UTC_ZONE_OFFSET), unixTimeStampOf(value));
    assertEquals(
        value.toEpochSecond(UTC_ZONE_OFFSET),
        eval(unixTimeStampOf(DSL.literal(new ExprTimestampValue(value)))).longValue());
  }

  private static Stream<Arguments> getInstantSamples() {
    return getDateTimeSamples()
        .map(v -> Arguments.of(((LocalDateTime) v.get()[0]).toInstant(UTC_ZONE_OFFSET)));
  }

  /**
   * Check processing valid values of type Instant.
   *
   * @param value a value
   */
  @ParameterizedTest
  @MethodSource("getInstantSamples")
  public void checkOfInstant(Instant value) {
    assertEquals(value.getEpochSecond(), unixTimeStampOf(value));
    assertEquals(
        value.getEpochSecond(),
        eval(unixTimeStampOf(DSL.literal(new ExprTimestampValue(value)))).longValue());
  }

  // formats: YYMMDD, YYMMDDhhmmss[.uuuuuu], YYYYMMDD, or YYYYMMDDhhmmss[.uuuuuu]
  // use BigDecimal, because double can't fit such big values
  private static Stream<Arguments> getDoubleSamples() {
    return Stream.of(
        Arguments.of(840101d, LocalDateTime.of(1984, 1, 1, 0, 0, 0)),
        Arguments.of(840101112233d, LocalDateTime.of(1984, 1, 1, 11, 22, 33)),
        Arguments.of(840101112233.123456, LocalDateTime.of(1984, 1, 1, 11, 22, 33, 123456000)),
        Arguments.of(19840101d, LocalDateTime.of(1984, 1, 1, 0, 0, 0)),
        Arguments.of(19840101000000d, LocalDateTime.of(1984, 1, 1, 0, 0, 0)),
        Arguments.of(19840101112233d, LocalDateTime.of(1984, 1, 1, 11, 22, 33)),
        Arguments.of(19840101112233.123456, LocalDateTime.of(1984, 1, 1, 11, 22, 33, 123456000)));
  }

  /**
   * Check processing valid Double values.
   *
   * @param valueAsDouble a value
   * @param valueAsLDT the value as LocalDateTime
   */
  @ParameterizedTest
  @MethodSource("getDoubleSamples")
  public void checkOfDoubleFormats(Double valueAsDouble, LocalDateTime valueAsLDT) {
    var valueAsStr = new DecimalFormat("0.#").format(valueAsDouble);
    assertEquals(
        valueAsLDT.toEpochSecond(UTC_ZONE_OFFSET), unixTimeStampOf(valueAsDouble), 1d, valueAsStr);
    assertEquals(
        valueAsLDT.toEpochSecond(UTC_ZONE_OFFSET),
        eval(unixTimeStampOf(DSL.literal(new ExprDoubleValue(valueAsDouble)))).longValue(),
        1d,
        valueAsStr);
  }

  @Test
  public void checkOfDouble() {
    // 19991231235959.99 passed ok, but 19991231235959.999999 rounded to ...60.0 which is incorrect
    // It is a double type limitation
    var valueDt = LocalDateTime.of(1999, 12, 31, 23, 59, 59, 999_999_999);
    assertEquals(valueDt.toEpochSecond(UTC_ZONE_OFFSET), unixTimeStampOf(19991231235959.99d), 1d);
  }

  @Test
  public void checkYearLessThan1970() {
    assertNotEquals(0, unixTimeStamp());
    assertEquals(0, unixTimeStampOf(LocalDate.of(1961, 4, 12)));
    assertEquals(0, unixTimeStampOf(LocalDateTime.of(1961, 4, 12, 9, 7, 0)));
    assertEquals(0, unixTimeStampOf(Instant.ofEpochMilli(-1)));
    assertEquals(0, unixTimeStampOf(LocalDateTime.of(1970, 1, 1, 0, 0, 0)));
    assertEquals(1, unixTimeStampOf(LocalDateTime.of(1970, 1, 1, 0, 0, 1)));
    assertEquals(0, unixTimeStampOf(19610412d));
    assertEquals(0, unixTimeStampOf(19610412090700d));
  }

  @Test
  public void checkMaxValue() {
    // MySQL returns 0 for values above
    //     '3001-01-19 03:14:07.999999' UTC (corresponding to 32536771199.999999 seconds).
    assertEquals(0, unixTimeStampOf(LocalDate.of(3001, 1, 20)));
    assertNotEquals(0d, unixTimeStampOf(LocalDate.of(3001, 1, 18)));
    assertEquals(0, unixTimeStampOf(LocalDateTime.of(3001, 1, 20, 3, 14, 8)));
    assertNotEquals(0d, unixTimeStampOf(LocalDateTime.of(3001, 1, 18, 3, 14, 7)));
    assertEquals(0, unixTimeStampOf(Instant.ofEpochSecond(32536771199L + 1)));
    assertNotEquals(0d, unixTimeStampOf(Instant.ofEpochSecond(32536771199L)));
    assertEquals(0, unixTimeStampOf(30010120d));
    assertNotEquals(0d, unixTimeStampOf(30010118d));
  }

  private static Stream<Arguments> getInvalidDoubleSamples() {
    return Stream.of(
        // invalid dates
        Arguments.of(19990231.d),
        Arguments.of(19991320.d),
        Arguments.of(19991232.d),
        Arguments.of(19990013.d),
        Arguments.of(19990931.d),
        Arguments.of(990231.d),
        Arguments.of(991320.d),
        Arguments.of(991232.d),
        Arguments.of(990013.d),
        Arguments.of(990931.d),
        Arguments.of(9990102.d),
        Arguments.of(99102.d),
        Arguments.of(9912.d),
        Arguments.of(199912.d),
        Arguments.of(1999102.d),
        // same as above, but with valid time
        Arguments.of(19990231112233.d),
        Arguments.of(19991320112233.d),
        Arguments.of(19991232112233.d),
        Arguments.of(19990013112233.d),
        Arguments.of(19990931112233.d),
        Arguments.of(990231112233.d),
        Arguments.of(991320112233.d),
        Arguments.of(991232112233.d),
        Arguments.of(990013112233.d),
        Arguments.of(990931112233.d),
        Arguments.of(9990102112233.d),
        Arguments.of(99102112233.d),
        Arguments.of(9912112233.d),
        Arguments.of(199912112233.d),
        Arguments.of(1999102112233.d),
        // invalid time
        Arguments.of(19840101242233.d),
        Arguments.of(19840101116033.d),
        Arguments.of(19840101112260.d),
        Arguments.of(1984010111223.d),
        Arguments.of(198401011123.d),
        Arguments.of(19840101123.d),
        Arguments.of(1984010113.d),
        Arguments.of(198401011.d),
        // same, but with short date
        Arguments.of(840101242233.d),
        Arguments.of(840101116033.d),
        Arguments.of(840101112260.d),
        Arguments.of(84010111223.d),
        Arguments.of(8401011123.d),
        Arguments.of(840101123.d),
        Arguments.of(8401011.d),
        // misc
        Arguments.of(0d),
        Arguments.of(-1d),
        Arguments.of(42d),
        // too many digits
        Arguments.of(199902201122330d));
  }

  /**
   * Check processing invalid Double values.
   *
   * @param value a value
   */
  @ParameterizedTest
  @MethodSource("getInvalidDoubleSamples")
  public void checkInvalidDoubleCausesNull(Double value) {
    assertEquals(
        ExprNullValue.of(),
        unixTimeStampOf(DSL.literal(new ExprDoubleValue(value))).valueOf(),
        new DecimalFormat("0.#").format(value));
  }
}
