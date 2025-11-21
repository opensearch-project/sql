/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static java.time.temporal.ChronoUnit.DAYS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class DateDiffTest extends DateTimeTestBase {

  private static final LocalTime timeSample1 = LocalTime.of(12, 42);
  private static final LocalTime timeSample2 = LocalTime.of(7, 40);
  private static final LocalDate dateSample1 = LocalDate.of(2022, 6, 6);
  private static final LocalDate dateSample2 = LocalDate.of(1961, 4, 12);
  private static final LocalDate dateSample3 = LocalDate.of(1993, 3, 4);
  private static final LocalDate epochStart = LocalDate.of(1970, 1, 1);
  // Use fixed date instead of LocalDate.now() to avoid timezone issues
  private static final LocalDate dateNow = LocalDate.of(2023, 5, 15);
  private static final LocalDateTime dateTimeSample1 = LocalDateTime.of(1961, 4, 12, 9, 7);
  private static final LocalDateTime dateTimeSample2 = LocalDateTime.of(1993, 3, 4, 5, 6);
  // Use fixed datetime instead of LocalDateTime.now()
  private static final LocalDateTime fixedNow = LocalDateTime.of(2023, 5, 15, 12, 0, 0);
  // Use fixed instant instead of Instant.now()
  private static final Instant fixedInstantNow = Instant.parse("2023-05-15T12:00:00Z");

  // Function signature is:
  // (DATE/DATETIME/TIMESTAMP/TIME, DATE/DATETIME/TIMESTAMP/TIME) -> LONG
  private static Stream<Arguments> getTestData() {
    // Arguments are: first argument for `DATE_DIFF` function, second argument and expected result.
    return Stream.of(
        Arguments.of(timeSample1, timeSample2, 0L),
        Arguments.of(timeSample1, dateNow, 0L),
        Arguments.of(timeSample1, fixedNow, 0L),
        Arguments.of(timeSample1, fixedInstantNow, 0L),
        Arguments.of(dateSample1, timeSample1, -DAYS.between(dateSample1, dateNow)),
        Arguments.of(dateSample1, dateSample3, -DAYS.between(dateSample1, dateSample3)),
        Arguments.of(dateSample1, dateTimeSample1, -DAYS.between(dateSample1, dateSample2)),
        Arguments.of(
            dateSample1, Instant.ofEpochSecond(42), -DAYS.between(dateSample1, epochStart)),
        Arguments.of(dateTimeSample1, LocalTime.of(12, 0), -DAYS.between(dateSample2, dateNow)),
        Arguments.of(dateTimeSample1, dateSample3, -DAYS.between(dateSample2, dateSample3)),
        Arguments.of(dateTimeSample1, dateTimeSample2, -DAYS.between(dateSample2, dateSample3)),
        Arguments.of(
            dateTimeSample1, Instant.ofEpochSecond(0), -DAYS.between(dateSample2, epochStart)),
        Arguments.of(Instant.ofEpochSecond(0), LocalTime.MAX, -DAYS.between(epochStart, dateNow)),
        Arguments.of(Instant.ofEpochSecond(0), dateSample3, -DAYS.between(epochStart, dateSample3)),
        Arguments.of(
            Instant.ofEpochSecond(0), dateTimeSample2, -DAYS.between(epochStart, dateSample3)),
        Arguments.of(
            Instant.ofEpochSecond(0),
            fixedInstantNow,
            -DAYS.between(epochStart, fixedNow.toLocalDate())));
  }

  @ParameterizedTest
  @MethodSource("getTestData")
  public void try_different_data(Temporal arg1, Temporal arg2, Long expectedResult) {
    assertEquals(expectedResult, datediff(arg1, arg2));
  }
}
