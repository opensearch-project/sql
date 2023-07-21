/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.expression.DSL;

public class PeriodFunctionsTest extends DateTimeTestBase {

  /**
   * Generate sample data for `PERIOD_ADD` function.
   * @return A data set.
   */
  public static Stream<Arguments> getTestDataForPeriodAdd() {
    // arguments are: first arg for `PERIOD_ADD`, second arg and expected result value.
    return Stream.of(
        Arguments.of(1, 3, 200004), // Jan 2000 + 3
        Arguments.of(3, -1, 200002), // Mar 2000 - 1
        Arguments.of(12, 0, 200012), // Dec 2000 + 0
        Arguments.of(6104, 100, 206908), // Apr 2061 + 100m (8y4m)
        Arguments.of(201510, 14, 201612)
    );
  }

  @ParameterizedTest
  @MethodSource("getTestDataForPeriodAdd")
  public void period_add_with_different_data(int period, int months, int expected) {
    assertEquals(expected, period_add(period, months));
  }

  /**
   * Generate sample data for `PERIOD_DIFF` function.
   * @return A data set.
   */
  public static Stream<Arguments> getTestDataForPeriodDiff() {
    // arguments are: first arg for `PERIOD_DIFF`, second arg and expected result value.
    return Stream.of(
        Arguments.of(1, 3, -2), // Jan - Mar 2000
        Arguments.of(3, 1, 2), // Mar - Jan 2000
        Arguments.of(12, 111, -11), // Dec 2000 - Nov 2001
        Arguments.of(2212, 201105, 139), // Dec 2022 - May 2011
        Arguments.of(200505, 7505, 360), // May 2005 - May 1975
        Arguments.of(6104, 8509, 907), // Apr 2061 - Sep 1985
        Arguments.of(207707, 7707, 1200) // Jul 2077 - Jul 1977
    );
  }

  @ParameterizedTest
  @MethodSource("getTestDataForPeriodDiff")
  public void period_diff_with_different_data(int period1, int period2, int expected) {
    assertEquals(expected, period_diff(period1, period2));
  }

  @ParameterizedTest
  @MethodSource("getTestDataForPeriodDiff")
  public void two_way_conversion(int period1, int period2, int expected) {
    assertEquals(0, period_diff(period_add(period1, -expected), period2));
  }

  /**
   * Generate invalid sample data for test.
   * @return A data set.
   */
  public static Stream<Arguments> getInvalidTestData() {
    return Stream.of(
      Arguments.of(0),
      Arguments.of(123),
      Arguments.of(100),
      Arguments.of(1234),
      Arguments.of(1000),
      Arguments.of(2020),
      Arguments.of(12345),
      Arguments.of(123456),
      Arguments.of(1234567),
      Arguments.of(200213),
      Arguments.of(200300),
      Arguments.of(-1),
      Arguments.of(-1234),
      Arguments.of(-123401)
    );
  }

  /**
   * Check that `PERIOD_ADD` and `PERIOD_DIFF` return NULL on invalid input.
   * @param period An invalid data.
   */
  @ParameterizedTest
  @MethodSource("getInvalidTestData")
  public void period_add_returns_null_on_invalid_input(int period) {
    assertNull(period_add(DSL.literal(period), DSL.literal(1)).valueOf().value());
    assertNull(period_diff(DSL.literal(period), DSL.literal(1)).valueOf().value());
    assertNull(period_diff(DSL.literal(1), DSL.literal(period)).valueOf().value());
  }
}
