/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class TimeBoundsTest {

  /** Kept as sent, so nothing here can produce a second reading. */
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
        arguments("dashboards format", "2026-09-09 22:00:00.000", "2026-09-09 22:30:00.000"),
        arguments("space, no millis", "2026-09-09 22:00:00", "2026-09-09 22:30:00"),
        arguments("date only", "2026-09-09", "2026-09-10"),
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

  /** Round trip through the table name, for every spelling a bound may arrive in. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("boundSpellings")
  void shouldSurviveTheTableName(String spelling, String start, String end) {
    TimeBounds bounds = new TimeBounds("event_time", start, end);

    TimeBounds.Decoded decoded = TimeBounds.decode(bounds.encodeInto("logs-*"));

    assertEquals("logs-*", decoded.tableName());
    assertEquals(bounds, decoded.bounds());
  }

  @Test
  void shouldEncodeReadably() {
    assertEquals(
        "logs-*<@timestamp,now-7d,now>",
        new TimeBounds("@timestamp", "now-7d", "now").encodeInto("logs-*"));
  }

  /** A name no request encoded decodes to itself, so an ordinary query is untouched. */
  @ParameterizedTest(name = "{0}")
  @ValueSource(
      strings = {"logs-*", "logs-2026.09", "remote:logs-*", "a,b", "logs-*<broken", "logs-*<a,b>"})
  void shouldLeaveAPlainNameAlone(String tableName) {
    TimeBounds.Decoded decoded = TimeBounds.decode(tableName);

    assertEquals(tableName, decoded.tableName());
    assertNull(decoded.bounds());
  }

  /**
   * A delimiter inside a bound cannot round trip, so the name is left unencoded and nothing prunes.
   */
  @Test
  void shouldRefuseToEncodeABoundHoldingADelimiter() {
    assertEquals("logs-*", new TimeBounds("@timestamp", "a,b", "now").encodeInto("logs-*"));
    assertEquals("logs-*", new TimeBounds("@timestamp", "now-7d", "b>c").encodeInto("logs-*"));
    assertEquals("logs-*", new TimeBounds("f<x", "now-7d", "now").encodeInto("logs-*"));
  }
}
