/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TimeBoundsTest {

  /**
   * Bounds are kept as sent, not parsed: the index's own date parser reads them when the probe
   * runs, so nothing here can produce a second reading that disagrees with the caller's.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("boundSpellings")
  void shouldKeepEveryAcceptedSpellingAsSent(String spelling, String start, String end) {
    TimeBounds bounds = new TimeBounds("@timestamp", start, end);

    assertEquals(start + ".." + end, bounds.getStart() + ".." + bounds.getEnd());
  }

  private static Stream<Arguments> boundSpellings() {
    return Stream.of(
        arguments("date math", "now-7d", "now"),
        arguments("date math with rounding", "now-1d/d", "now/d"),
        // What Dashboards sends: the same literals it puts in the where clause it appended.
        arguments("dashboards format", "2026-09-09 22:00:00.000", "2026-09-09 22:30:00.000"),
        arguments("ISO-8601 instant", "2026-09-09T22:00:00.000Z", "2026-09-09T22:30:00.000Z"),
        arguments("epoch millis", "1788991200000", "1788993000000"));
  }

  @Test
  void shouldKeepTheDeclaredTimeField() {
    assertEquals("event_time", new TimeBounds("event_time", "now-1h", "now").getTimeField());
  }

  @Test
  void shouldTrimSurroundingSpace() {
    TimeBounds bounds = new TimeBounds(" ts ", " now-1h ", " now ");

    assertEquals(
        "ts|now-1h|now", bounds.getTimeField() + "|" + bounds.getStart() + "|" + bounds.getEnd());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("unusableBounds")
  void shouldRejectBoundsNothingCanBePrunedOn(
      String why, String timeField, String start, String end) {
    assertThrows(IllegalArgumentException.class, () -> new TimeBounds(timeField, start, end));
  }

  private static Stream<Arguments> unusableBounds() {
    return Stream.of(
        arguments("no field", null, "now-1h", "now"),
        arguments("blank field", "  ", "now-1h", "now"),
        arguments("no lower bound", "@timestamp", null, "now"),
        arguments("blank lower bound", "@timestamp", " ", "now"),
        arguments("no upper bound", "@timestamp", "now-1h", null),
        arguments("blank upper bound", "@timestamp", "now-1h", ""));
  }
}
