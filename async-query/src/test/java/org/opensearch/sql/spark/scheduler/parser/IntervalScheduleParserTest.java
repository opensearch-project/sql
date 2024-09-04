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
    verifyParseSchedule(5, "5 minutes");
  }

  @Test
  public void testParseValidScheduleStringWithDifferentUnits() {
    verifyParseSchedule(120, "2 hours");
    verifyParseSchedule(1440, "1 day");
    verifyParseSchedule(30240, "3 weeks");
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
    assertThrows(
        IllegalArgumentException.class,
        () -> IntervalScheduleParser.parse("1 year", startTime),
        "Expected IllegalArgumentException but no exception was thrown");

    assertThrows(
        IllegalArgumentException.class,
        () -> IntervalScheduleParser.parse("1 month", startTime),
        "Expected IllegalArgumentException but no exception was thrown");
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
    verifyParseSchedule(1, "60000000000 nanoseconds");
  }

  @Test
  public void testParseScheduleWithMilliseconds() {
    verifyParseSchedule(1, "60000 milliseconds");
  }

  @Test
  public void testParseScheduleWithMicroseconds() {
    verifyParseSchedule(1, "60000000 microseconds");
  }

  @Test
  public void testUnsupportedTimeUnit() {
    assertThrows(
        IllegalArgumentException.class,
        () -> IntervalScheduleParser.convertToSupportedUnit(10, "unsupportedunit"),
        "Expected IllegalArgumentException but no exception was thrown");
  }

  @Test
  public void testParseScheduleWithSeconds() {
    verifyParseSchedule(2, "120 seconds");
  }

  private void verifyParseSchedule(int expectedMinutes, String scheduleStr) {
    Schedule schedule = IntervalScheduleParser.parse(scheduleStr, startTime);
    assertEquals(new IntervalSchedule(startTime, expectedMinutes, ChronoUnit.MINUTES), schedule);
  }
}
