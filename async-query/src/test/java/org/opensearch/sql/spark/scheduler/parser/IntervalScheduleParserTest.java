/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.scheduler.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.jobscheduler.spi.schedule.IntervalSchedule;
import org.opensearch.jobscheduler.spi.schedule.Schedule;

public class IntervalScheduleParserTest {

  private Instant startTime;

  @BeforeEach
  public void setup() {
    startTime = Instant.now();
  }

  @Test
  public void testParseValidScheduleString() {
    String scheduleStr = "5 minutes";
    Schedule schedule = IntervalScheduleParser.parse(scheduleStr, startTime);

    assertEquals(new IntervalSchedule(startTime, 5, ChronoUnit.MINUTES), schedule);
  }

  @Test
  public void testParseValidScheduleStringWithDifferentUnits() {
    String scheduleStr = "2 hours";
    Schedule schedule = IntervalScheduleParser.parse(scheduleStr, startTime);

    assertEquals(new IntervalSchedule(startTime, 120, ChronoUnit.MINUTES), schedule);

    scheduleStr = "1 day";
    schedule = IntervalScheduleParser.parse(scheduleStr, startTime);

    assertEquals(new IntervalSchedule(startTime, 1440, ChronoUnit.MINUTES), schedule);

    scheduleStr = "3 weeks";
    schedule = IntervalScheduleParser.parse(scheduleStr, startTime);

    assertEquals(new IntervalSchedule(startTime, 30240, ChronoUnit.MINUTES), schedule);
  }

  @Test
  public void testParseNullSchedule() {
    Schedule schedule = IntervalScheduleParser.parse(null, startTime);
    assertNull(schedule);
  }

  @Test
  public void testParseScheduleObject() {
    IntervalSchedule expectedSchedule = new IntervalSchedule(startTime, 10, ChronoUnit.MINUTES);
    Schedule schedule = IntervalScheduleParser.parse(expectedSchedule, startTime);

    assertEquals(expectedSchedule, schedule);
  }

  @Test
  public void testParseInvalidScheduleString() {
    String scheduleStr = "invalid schedule";
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> IntervalScheduleParser.parse(scheduleStr, startTime),
            "Expected IllegalArgumentException but no exception was thrown");

    assertEquals("Invalid interval format: " + scheduleStr.toLowerCase(), exception.getMessage());
  }

  @Test
  public void testParseUnsupportedUnits() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> IntervalScheduleParser.parse("1 year", startTime),
            "Expected IllegalArgumentException but no exception was thrown");

    assertEquals("Years cannot be converted to minutes accurately.", exception.getMessage());

    exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> IntervalScheduleParser.parse("1 month", startTime),
            "Expected IllegalArgumentException but no exception was thrown");

    assertEquals("Months cannot be converted to minutes accurately.", exception.getMessage());
  }

  @Test
  public void testParseNonStringSchedule() {
    Object nonStringSchedule = 12345;
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> IntervalScheduleParser.parse(nonStringSchedule, startTime),
            "Expected IllegalArgumentException but no exception was thrown");

    assertEquals("Schedule must be a String object for parsing.", exception.getMessage());
  }

  @Test
  public void testParseScheduleWithNanoseconds() {
    String scheduleStr = "60000000000 nanoseconds"; // Equivalent to 1 minute
    Schedule schedule = IntervalScheduleParser.parse(scheduleStr, startTime);

    assertEquals(new IntervalSchedule(startTime, 1, ChronoUnit.MINUTES), schedule);
  }

  @Test
  public void testParseScheduleWithMilliseconds() {
    String scheduleStr = "60000 milliseconds"; // Equivalent to 1 minute
    Schedule schedule = IntervalScheduleParser.parse(scheduleStr, startTime);

    assertEquals(new IntervalSchedule(startTime, 1, ChronoUnit.MINUTES), schedule);
  }

  @Test
  public void testParseScheduleWithMicroseconds() {
    String scheduleStr = "60000000 microseconds"; // Equivalent to 1 minute
    Schedule schedule = IntervalScheduleParser.parse(scheduleStr, startTime);

    assertEquals(new IntervalSchedule(startTime, 1, ChronoUnit.MINUTES), schedule);
  }

  @Test
  public void testUnsupportedTimeUnit() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> IntervalScheduleParser.convertToSupportedUnit(10, "unsupportedunit"),
            "Expected IllegalArgumentException but no exception was thrown");

    assertEquals("Unsupported time unit: unsupportedunit", exception.getMessage());
  }

  @Test
  public void testParseScheduleWithSeconds() {
    String scheduleStr = "120 seconds"; // Equivalent to 2 minutes
    Schedule schedule = IntervalScheduleParser.parse(scheduleStr, startTime);

    assertEquals(new IntervalSchedule(startTime, 2, ChronoUnit.MINUTES), schedule);
  }
}
