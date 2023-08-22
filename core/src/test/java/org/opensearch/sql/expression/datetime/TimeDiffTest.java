/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalTime;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.expression.DSL;

public class TimeDiffTest extends DateTimeTestBase {

  private static Stream<Arguments> getTestData() {
    return Stream.of(
        Arguments.of(LocalTime.of(12, 42), LocalTime.of(7, 40), LocalTime.of(5, 2)),
        Arguments.of(LocalTime.of(7, 40), LocalTime.of(12, 42), LocalTime.of(18, 58)),
        Arguments.of(LocalTime.of(7, 40), LocalTime.of(7, 40), LocalTime.of(0, 0)),
        Arguments.of(LocalTime.MAX, LocalTime.MIN, LocalTime.MAX));
  }

  /**
   * Test `TIME_DIFF` function with different data.
   *
   * @param arg1 First argument.
   * @param arg2 Second argument.
   * @param expectedResult Expected result.
   */
  @ParameterizedTest
  @MethodSource("getTestData")
  public void try_different_data(LocalTime arg1, LocalTime arg2, LocalTime expectedResult) {
    assertEquals(expectedResult, timediff(arg1, arg2));
    assertEquals(
        expectedResult,
        eval(timediff(DSL.literal(new ExprTimeValue(arg1)), DSL.literal(new ExprTimeValue(arg2))))
            .timeValue());
  }
}
