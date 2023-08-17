/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.expression.DSL;

public class UnixTwoWayConversionTest extends DateTimeTestBase {

  @Test
  public void checkConvertNow() {
    assertEquals(getExpectedNow(), fromUnixTime(unixTimeStamp()));
  }

  @Test
  public void checkConvertNow_with_eval() {
    assertEquals(
        getExpectedNow(),
        LocalDateTime.ofInstant(
            eval(fromUnixTime(unixTimeStampExpr())).timestampValue(), ZoneOffset.UTC));
  }

  private LocalDateTime getExpectedNow() {
    return LocalDateTime.now(functionProperties.getQueryStartClock().withZone(ZoneOffset.UTC))
        .withNano(0);
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
   * Test converting valid Double values EpochTime -> DateTime -> EpochTime.
   *
   * @param value a value
   */
  @ParameterizedTest
  @MethodSource("getDoubleSamples")
  public void convertEpoch2DateTime2Epoch(Double value) {
    assertEquals(value, unixTimeStampOf(fromUnixTime(value)));
    assertEquals(
        value,
        eval(unixTimeStampOf(fromUnixTime(DSL.literal(new ExprDoubleValue(value))))).doubleValue());

    assertEquals(Math.round(value) + 0d, unixTimeStampOf(fromUnixTime(Math.round(value))));
    assertEquals(
        Math.round(value) + 0d,
        eval(unixTimeStampOf(fromUnixTime(DSL.literal(new ExprLongValue(Math.round(value))))))
            .doubleValue());
  }

  private static Stream<Arguments> getDateTimeSamples() {
    return Stream.of(
        Arguments.of(LocalDateTime.of(1984, 1, 1, 1, 1)),
        Arguments.of(LocalDateTime.of(2000, 2, 29, 22, 54)),
        Arguments.of(LocalDateTime.of(1999, 12, 31, 23, 59, 59)),
        Arguments.of(LocalDateTime.of(2004, 2, 29, 7, 40)),
        Arguments.of(LocalDateTime.of(2100, 2, 28, 13, 14, 15)),
        Arguments.of(LocalDateTime.of(2012, 2, 21, 0, 0, 17)));
  }

  /**
   * Test converting valid values DateTime -> EpochTime -> DateTime.
   *
   * @param value a value
   */
  @ParameterizedTest
  @MethodSource("getDateTimeSamples")
  public void convertDateTime2Epoch2DateTime(LocalDateTime value) {
    assertEquals(value, fromUnixTime(unixTimeStampOf(value)));
    assertEquals(
        value,
        LocalDateTime.ofInstant(
            eval(fromUnixTime(unixTimeStampOf(DSL.literal(new ExprTimestampValue(value)))))
                .timestampValue(),
            ZoneOffset.UTC));
  }
}
