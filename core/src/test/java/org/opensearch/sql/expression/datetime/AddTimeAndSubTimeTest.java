/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class AddTimeAndSubTimeTest extends DateTimeTestBase {

  @Test
  // (TIME, TIME/DATE/DATETIME/TIMESTAMP) -> TIME
  public void return_time_when_first_arg_is_time() {
    var res = addtime(LocalTime.of(21, 0), LocalTime.of(0, 5));
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(21, 5), res.timeValue());

    res = subtime(LocalTime.of(21, 0), LocalTime.of(0, 5));
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(20, 55), res.timeValue());

    res = addtime(LocalTime.of(12, 20), Instant.ofEpochSecond(42));
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(12, 20, 42), res.timeValue());

    res = subtime(LocalTime.of(10, 0), Instant.ofEpochSecond(42));
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(9, 59, 18), res.timeValue());

    res = addtime(LocalTime.of(2, 3, 4), LocalDateTime.of(1961, 4, 12, 9, 7));
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(11, 10, 4), res.timeValue());

    res = subtime(LocalTime.of(12, 3, 4), LocalDateTime.of(1961, 4, 12, 9, 7));
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(2, 56, 4), res.timeValue());

    res = addtime(LocalTime.of(9, 7), LocalDate.now());
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(9, 7), res.timeValue());

    res = subtime(LocalTime.of(9, 7), LocalDate.of(1961, 4, 12));
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(9, 7), res.timeValue());
  }

  @Test
  public void time_limited_by_24_hours() {
    var res = addtime(LocalTime.of(21, 0), LocalTime.of(14, 5));
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(11, 5), res.timeValue());

    res = subtime(LocalTime.of(14, 0), LocalTime.of(21, 5));
    assertEquals(TIME, res.type());
    assertEquals(LocalTime.of(16, 55), res.timeValue());
  }

  // Function signature is:
  // (DATE/DATETIME/TIMESTAMP, TIME/DATE/DATETIME/TIMESTAMP) -> DATETIME
  private static Stream<Arguments> getTestData() {
    return Stream.of(
        // DATETIME and TIME/DATE/DATETIME/TIMESTAMP
        Arguments.of(LocalDateTime.of(1961, 4, 12, 9, 7), LocalTime.of(1, 48),
            LocalDateTime.of(1961, 4, 12, 10, 55), LocalDateTime.of(1961, 4, 12, 7, 19)),
        Arguments.of(LocalDateTime.of(1961, 4, 12, 9, 7), LocalDate.of(2000, 1, 1),
            LocalDateTime.of(1961, 4, 12, 9, 7), LocalDateTime.of(1961, 4, 12, 9, 7)),
        Arguments.of(LocalDateTime.of(1961, 4, 12, 9, 7), LocalDateTime.of(1235, 5, 6, 1, 48),
            LocalDateTime.of(1961, 4, 12, 10, 55), LocalDateTime.of(1961, 4, 12, 7, 19)),
        Arguments.of(LocalDateTime.of(1961, 4, 12, 9, 7), Instant.ofEpochSecond(42),
            LocalDateTime.of(1961, 4, 12, 9, 7, 42), LocalDateTime.of(1961, 4, 12, 9, 6, 18)),
        // DATE and TIME/DATE/DATETIME/TIMESTAMP
        Arguments.of(LocalDate.of(1961, 4, 12), LocalTime.of(9, 7),
            LocalDateTime.of(1961, 4, 12, 9, 7), LocalDateTime.of(1961, 4, 11, 14, 53)),
        Arguments.of(LocalDate.of(1961, 4, 12), LocalDate.of(2000, 1, 1),
            LocalDateTime.of(1961, 4, 12, 0, 0), LocalDateTime.of(1961, 4, 12, 0, 0)),
        Arguments.of(LocalDate.of(1961, 4, 12), LocalDateTime.of(1235, 5, 6, 1, 48),
            LocalDateTime.of(1961, 4, 12, 1, 48), LocalDateTime.of(1961, 4, 11, 22, 12)),
        Arguments.of(LocalDate.of(1961, 4, 12), Instant.ofEpochSecond(42),
            LocalDateTime.of(1961, 4, 12, 0, 0, 42), LocalDateTime.of(1961, 4, 11, 23, 59, 18)),
        // TIMESTAMP and TIME/DATE/DATETIME/TIMESTAMP
        Arguments.of(Instant.ofEpochSecond(42), LocalTime.of(9, 7),
            LocalDateTime.of(1970, 1, 1, 9, 7, 42), LocalDateTime.of(1969, 12, 31, 14, 53, 42)),
        Arguments.of(Instant.ofEpochSecond(42), LocalDate.of(1961, 4, 12),
            LocalDateTime.of(1970, 1, 1, 0, 0, 42), LocalDateTime.of(1970, 1, 1, 0, 0, 42)),
        Arguments.of(Instant.ofEpochSecond(42), LocalDateTime.of(1961, 4, 12, 9, 7),
            LocalDateTime.of(1970, 1, 1, 9, 7, 42), LocalDateTime.of(1969, 12, 31, 14, 53, 42)),
        Arguments.of(Instant.ofEpochSecond(42), Instant.ofEpochMilli(42),
            LocalDateTime.of(1970, 1, 1, 0, 0, 42, 42000000),
            LocalDateTime.of(1970, 1, 1, 0, 0, 41, 958000000))
    );
  }

  /**
   * Check that `ADDTIME` and `SUBTIME` functions result value and type.
   * @param arg1 First argument.
   * @param arg2 Second argument.
   * @param addTimeExpectedResult Expected result for `ADDTIME`.
   * @param subTimeExpectedResult Expected result for `SUBTIME`.
   */
  @ParameterizedTest
  @MethodSource("getTestData")
  public void return_datetime_when_first_arg_is_not_time(Temporal arg1, Temporal arg2,
                                                         LocalDateTime addTimeExpectedResult,
                                                         LocalDateTime subTimeExpectedResult) {
    var res = addtime(arg1, arg2);
    assertEquals(DATETIME, res.type());
    assertEquals(addTimeExpectedResult, res.datetimeValue());

    res = subtime(arg1, arg2);
    assertEquals(DATETIME, res.type());
    assertEquals(subTimeExpectedResult, res.datetimeValue());
  }
}
