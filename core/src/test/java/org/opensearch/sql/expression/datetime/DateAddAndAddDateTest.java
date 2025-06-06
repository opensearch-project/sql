/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.exception.ExpressionEvaluationException;

public class DateAddAndAddDateTest extends DateTimeTestBase {

  private LocalDate today() {
    return LocalDate.now(functionProperties.getQueryStartClock());
  }

  @Test
  public void adddate_returns_datetime_when_args_are_time_and_time_interval() {
    var res = adddate(LocalTime.MIN, Duration.ofHours(1).plusMinutes(2));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalTime.of(1, 2).atDate(today()), res.datetimeValue());
  }

  @Test
  public void date_add_returns_datetime_when_args_are_time_and_time_interval() {
    var res =
        date_add(LocalTime.of(10, 20, 30), Duration.ofHours(1).plusMinutes(2).plusSeconds(42));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalTime.of(11, 23, 12).atDate(today()), res.datetimeValue());
  }

  @Test
  public void adddate_time_limited_by_24_hours() {
    var res = adddate(LocalTime.MAX, Duration.ofNanos(1));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalTime.MIN, res.datetimeValue().toLocalTime());
  }

  @Test
  public void date_add_time_limited_by_24_hours() {
    var res =
        date_add(LocalTime.of(10, 20, 30), Duration.ofHours(20).plusMinutes(50).plusSeconds(7));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalTime.of(7, 10, 37), res.datetimeValue().toLocalTime());
  }

  @Test
  public void adddate_returns_datetime_when_args_are_date_and_date_interval() {
    var res = adddate(LocalDate.of(2020, 2, 20), Period.of(3, 11, 21));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDate.of(2024, 2, 10).atStartOfDay(), res.datetimeValue());
  }

  @Test
  public void date_add_returns_datetime_when_args_are_date_and_date_interval() {
    var res = date_add(LocalDate.of(1961, 4, 12), Period.of(50, 50, 50));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDate.of(2015, 8, 1).atStartOfDay(), res.datetimeValue());
  }

  @Test
  public void adddate_returns_datetime_when_args_are_date_and_time_interval() {
    var res = adddate(LocalDate.of(2020, 2, 20), Duration.ofHours(1).plusMinutes(2));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDateTime.of(2020, 2, 20, 1, 2), res.datetimeValue());
  }

  @Test
  public void date_add_returns_datetime_when_args_are_date_and_time_interval() {
    var res = date_add(LocalDate.of(1961, 4, 12), Duration.ofHours(9).plusMinutes(7));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDateTime.of(1961, 4, 12, 9, 7), res.datetimeValue());
  }

  @Test
  public void adddate_returns_datetime_when_args_are_time_and_date_interval() {
    // Date based on today
    var res = adddate(LocalTime.of(1, 2, 0), Period.ofDays(1));
    assertEquals(DATETIME, res.type());
    assertEquals(today().plusDays(1).atTime(LocalTime.of(1, 2, 0)), res.datetimeValue());
  }

  @Test
  public void date_add_returns_datetime_when_args_are_time_and_date_interval() {
    var res = date_add(LocalTime.MIDNIGHT, Period.ofDays(0));
    assertEquals(DATETIME, res.type());
    assertEquals(today().atStartOfDay(), res.datetimeValue());
  }

  @Test
  public void adddate_returns_datetime_when_first_arg_is_datetime() {
    var res = adddate(LocalDateTime.of(1961, 4, 12, 9, 7), Duration.ofMinutes(108));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDateTime.of(1961, 4, 12, 10, 55), res.datetimeValue());
  }

  @Test
  public void date_add_returns_datetime_when_first_arg_is_timestamp() {
    var res =
        date_add(
            LocalDateTime.of(1961, 4, 12, 9, 7).toInstant(ZoneOffset.UTC), Duration.ofMinutes(108));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDateTime.of(1961, 4, 12, 10, 55), res.datetimeValue());
  }

  @Test
  public void adddate_accepts_negative_interval() {
    var res = adddate(LocalDateTime.of(2020, 10, 20, 14, 42), Duration.ofDays(-10));
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDateTime.of(2020, 10, 10, 14, 42), res.datetimeValue());
    assertEquals(subdate(LocalDateTime.of(2020, 10, 20, 14, 42), Duration.ofDays(10)), res);
  }

  @Test
  public void adddate_has_second_signature_but_not_date_add() {
    var res = adddate(LocalDateTime.of(1961, 4, 12, 9, 7), 100500);
    assertEquals(DATETIME, res.type());

    var exception =
        assertThrows(
            ExpressionEvaluationException.class,
            () -> date_add(LocalDateTime.of(1961, 4, 12, 9, 7), 100500));
    assertEquals(
        "date_add function expected {[DATE,INTERVAL],[DATETIME,INTERVAL],"
            + "[TIMESTAMP,INTERVAL],[TIME,INTERVAL]}, but got [DATETIME,INTEGER]",
        exception.getMessage());
  }

  @Test
  public void adddate_returns_date_when_args_are_date_and_days() {
    var res = adddate(LocalDate.of(1961, 4, 12), 100500);
    assertEquals(DATE, res.type());
    assertEquals(LocalDate.of(1961, 4, 12).plusDays(100500), res.dateValue());
  }

  @Test
  public void adddate_returns_datetime_when_args_are_date_but_days() {
    var res = adddate(LocalDate.of(2000, 1, 1).atStartOfDay(), 2);
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDateTime.of(2000, 1, 3, 0, 0), res.datetimeValue());

    res = adddate(LocalTime.now(), 2);
    assertEquals(DATETIME, res.type());
    assertEquals(today().plusDays(2), res.dateValue());

    res = adddate(Instant.ofEpochSecond(42), 2);
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDateTime.of(1970, 1, 3, 0, 0, 42), res.datetimeValue());
  }

  @Test
  public void adddate_accepts_negative_days() {
    var res = adddate(LocalDateTime.of(2020, 10, 20, 8, 16, 32), -40);
    assertEquals(DATETIME, res.type());
    assertEquals(LocalDateTime.of(2020, 10, 20, 8, 16, 32).minusDays(40), res.datetimeValue());
    assertEquals(subdate(LocalDateTime.of(2020, 10, 20, 8, 16, 32), 40), res);
  }
}
