/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.model.ExprValueUtils.nullValue;

import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeParseException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.expression.DSL;

public class MakeTimeTest extends DateTimeTestBase {

  @Test
  public void checkEdgeCases() {
    assertEquals(
        nullValue(),
        eval(maketime(DSL.literal(-1.), DSL.literal(42.), DSL.literal(42.))),
        "Negative hour doesn't produce NULL");
    assertEquals(
        nullValue(),
        eval(maketime(DSL.literal(42.), DSL.literal(-1.), DSL.literal(42.))),
        "Negative minute doesn't produce NULL");
    assertEquals(
        nullValue(),
        eval(maketime(DSL.literal(12.), DSL.literal(42.), DSL.literal(-1.))),
        "Negative second doesn't produce NULL");

    assertThrows(
        DateTimeParseException.class,
        () -> eval(maketime(DSL.literal(24.), DSL.literal(42.), DSL.literal(42.))));
    assertThrows(
        DateTimeParseException.class,
        () -> eval(maketime(DSL.literal(12.), DSL.literal(60.), DSL.literal(42.))));
    assertThrows(
        DateTimeParseException.class,
        () -> eval(maketime(DSL.literal(12.), DSL.literal(42.), DSL.literal(60.))));

    assertEquals(LocalTime.of(23, 59, 59), maketime(23., 59., 59.));
    assertEquals(LocalTime.of(0, 0, 0), maketime(0., 0., 0.));
  }

  @Test
  public void checkRounding() {
    assertEquals(LocalTime.of(0, 0, 0), maketime(0.49, 0.49, 0.));
    assertEquals(LocalTime.of(1, 1, 0), maketime(0.50, 0.50, 0.));
  }

  @Test
  public void checkSecondFraction() {
    assertEquals(LocalTime.of(0, 0, 0).withNano(999999999), maketime(0., 0., 0.999999999));
    assertEquals(LocalTime.of(0, 0, 0).withNano(100502000), maketime(0., 0., 0.100502));
  }

  private static Stream<Arguments> getTestData() {
    return Stream.of(
        Arguments.of(20., 30., 40.),
        Arguments.of(18.392650, 32.625996, 52.877904),
        Arguments.of(20.115442, 7.393619, 27.006809),
        Arguments.of(1.231453, 36.462770, 28.736317),
        Arguments.of(3.586288, 13.180347, 22.665265),
        Arguments.of(4.284613, 40.426888, 19.631883),
        Arguments.of(14.843040, 44.682624, 53.484064),
        Arguments.of(19.797981, 41.826666, 2.635713),
        Arguments.of(4.194618, 10.934165, 32.019225),
        Arguments.of(13.240491, 53.625706, 34.506773),
        Arguments.of(7.606246, 27.344016, 30.117284),
        Arguments.of(13.922934, 26.936002, 42.599373),
        Arguments.of(23.114911, 37.764516, 7.677971),
        Arguments.of(7.388466, 31.973471, 35.131596),
        Arguments.of(19.777173, 44.926077, 24.613693),
        Arguments.of(5.773249, 52.693275, 10.190731),
        Arguments.of(17.812324, 36.549285, 4.620326),
        Arguments.of(9.774054, 41.955251, 23.995705),
        Arguments.of(19.619894, 54.933941, 48.788633),
        Arguments.of(18.731704, 48.510363, 50.444896),
        Arguments.of(10.345095, 27.593594, 23.083821),
        Arguments.of(22.925545, 25.113236, 10.645589),
        Arguments.of(7.494112, 9.761983, 17.444988),
        Arguments.of(17.867756, 10.313120, 36.391815),
        Arguments.of(19.712155, 3.197562, 6.607233),
        Arguments.of(2.385090, 41.761568, 33.342590));
  }

  /**
   * Test function with given pseudo-random values.
   *
   * @param hour hour
   * @param minute minute
   * @param second second
   */
  @ParameterizedTest(name = "hour = {0}, minute = {1}, second = {2}")
  @MethodSource("getTestData")
  public void checkRandomValues(double hour, double minute, double second) {
    // results could have 1 nanosec diff because of rounding FP
    var expected =
        LocalTime.of(
                (int) Math.round(hour),
                (int) Math.round(minute),
                // pick fraction second part as nanos
                (int) Math.floor(second))
            .withNano((int) ((second % 1) * 1E9));
    var delta = Duration.between(expected, maketime(hour, minute, second)).getNano();
    assertEquals(
        0, delta, 1, String.format("hour = %f, minute = %f, second = %f", hour, minute, second));
  }
}
