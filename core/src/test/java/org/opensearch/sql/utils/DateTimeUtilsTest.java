/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.utils.DateTimeUtils.getRelativeZonedDateTime;
import static org.opensearch.sql.utils.DateTimeUtils.resolveTimeModifier;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.physical.collector.Rounding.DateTimeUnit;

public class DateTimeUtilsTest {
  private final ZonedDateTime monday = ZonedDateTime.of(2025, 9, 15, 2, 0, 0, 0, ZoneOffset.UTC);
  private final ZonedDateTime tuesday =
      ZonedDateTime.of(2014, 11, 18, 14, 27, 32, 0, ZoneOffset.UTC);
  private final ZonedDateTime wednesday =
      ZonedDateTime.of(2025, 10, 22, 10, 32, 12, 0, ZoneOffset.UTC);
  private final ZonedDateTime thursday = ZonedDateTime.of(2025, 9, 11, 10, 0, 0, 0, ZoneOffset.UTC);
  private final ZonedDateTime sunday = ZonedDateTime.of(2025, 9, 14, 2, 0, 0, 0, ZoneOffset.UTC);

  @Test
  void round() {
    long actual =
        LocalDateTime.parse("2021-09-28T23:40:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli();
    long rounded = DateTimeUtils.roundFloor(actual, TimeUnit.HOURS.toMillis(1));
    assertEquals(
        LocalDateTime.parse("2021-09-28T23:00:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());
  }

  @Test
  void testRelativeZonedDateTimeWithNow() {
    ZonedDateTime now = ZonedDateTime.ofInstant(Instant.now(), ZoneOffset.UTC);
    assertEquals(getRelativeZonedDateTime("now", now), now);
    assertEquals(getRelativeZonedDateTime("now()", now), now);
  }

  @Test
  void testRelativeZonedDateTimeWithSnap() {
    ZonedDateTime snap1 = getRelativeZonedDateTime("-1d@d", wednesday);
    ZonedDateTime snap2 = getRelativeZonedDateTime("-3d-2h@h", wednesday);
    assertEquals("2025-10-21T00:00", snap1.toLocalDateTime().toString());
    assertEquals("2025-10-19T08:00", snap2.toLocalDateTime().toString());
  }

  @Test
  void testRelativeZonedDateTimeWithOffset() {
    ZonedDateTime snap1 = getRelativeZonedDateTime("-1d+1y@mon", wednesday);
    ZonedDateTime snap2 = getRelativeZonedDateTime("-3d@d-2h+10m@h", wednesday);
    assertEquals("2026-10-01T00:00", snap1.toLocalDateTime().toString());
    assertEquals("2025-10-18T22:00", snap2.toLocalDateTime().toString());
  }

  @Test
  void testRelativeZonedDateTimeWithAlias() {
    ZonedDateTime snap1 = getRelativeZonedDateTime("-1d+1y@Month", wednesday);
    ZonedDateTime snap2 = getRelativeZonedDateTime("-3d@d-2h+10m@hours", wednesday);
    assertEquals("2026-10-01T00:00", snap1.toLocalDateTime().toString());
    assertEquals("2025-10-18T22:00", snap2.toLocalDateTime().toString());
  }

  @Test
  void testRelativeZonedDateTimeWithWeekDayAndQuarter() {
    ZonedDateTime snap1 = getRelativeZonedDateTime("-1d+1y@W5", wednesday);
    ZonedDateTime snap2 = getRelativeZonedDateTime("-3d@d-2q+10m@quarter", wednesday);
    assertEquals("2026-10-16T00:00", snap1.toLocalDateTime().toString());
    assertEquals("2025-04-01T00:00", snap2.toLocalDateTime().toString());
  }

  @Test
  void testRelativeZonedDateTimeWithWrongInput() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> getRelativeZonedDateTime("1d+1y", wednesday));
    assertEquals("Unexpected character '1' at position 0 in input: 1d+1y", e.getMessage());
  }

  @Test
  void testRoundOnTimestampBeforeEpoch() {
    long actual =
        LocalDateTime.parse("1961-05-12T23:40:05")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli();
    long rounded = DateTimeUnit.MINUTE.round(actual, 1);
    assertEquals(
        LocalDateTime.parse("1961-05-12T23:40:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());

    rounded = DateTimeUnit.HOUR.round(actual, 1);
    assertEquals(
        LocalDateTime.parse("1961-05-12T23:00:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());

    rounded = DateTimeUnit.DAY.round(actual, 1);
    assertEquals(
        LocalDateTime.parse("1961-05-12T00:00:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());

    rounded = DateTimeUnit.DAY.round(actual, 3);
    assertEquals(
        LocalDateTime.parse("1961-05-12T00:00:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());

    rounded = DateTimeUnit.WEEK.round(actual, 1);
    assertEquals(
        LocalDateTime.parse("1961-05-08T00:00:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());

    rounded = DateTimeUnit.MONTH.round(actual, 1);
    assertEquals(
        LocalDateTime.parse("1961-05-01T00:00:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());

    rounded = DateTimeUnit.QUARTER.round(actual, 1);
    assertEquals(
        LocalDateTime.parse("1961-04-01T00:00:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());

    rounded = DateTimeUnit.YEAR.round(actual, 2);
    assertEquals(
        LocalDateTime.parse("1960-01-01T00:00:00")
            .atZone(ZoneOffset.UTC)
            .toInstant()
            .toEpochMilli(),
        Instant.ofEpochMilli(rounded).toEpochMilli());
  }

  @Test
  void testResolveTimeModifierNull() {
    assertNull(DateTimeUtils.resolveTimeModifier(null));
    assertNull(DateTimeUtils.resolveTimeModifier(""));
  }

  @Test
  void testResolveTimeModifierWithDatetimeString() {
    assertEquals("2025-10-22T00:00:00Z", DateTimeUtils.resolveTimeModifier("2025-10-22"));
    assertEquals("2025-10-22T10:32:12Z", DateTimeUtils.resolveTimeModifier("2025-10-22 10:32:12"));
    // Test "direct" format
    assertEquals("2025-10-22T10:32:12Z", DateTimeUtils.resolveTimeModifier("10/22/2025:10:32:12"));
    // Test ISO 8601 format
    assertEquals("2025-10-22T10:32:12Z", DateTimeUtils.resolveTimeModifier("2025-10-22T10:32:12Z"));
  }

  @Test
  void testResolveTimeModifierWithOffsets() {
    assertEquals("now-30s", DateTimeUtils.resolveTimeModifier("-30s"));
    assertEquals("now-1h", DateTimeUtils.resolveTimeModifier("-1h"));
    assertEquals("now+1d", DateTimeUtils.resolveTimeModifier("+1d"));
    assertEquals("now-3M", DateTimeUtils.resolveTimeModifier("-3month"));
    assertEquals("now-1y", DateTimeUtils.resolveTimeModifier("-1year"));
  }

  @Test
  void testTimeUnitVariants() {
    // Test all variants of seconds
    assertEquals("now-5s", DateTimeUtils.resolveTimeModifier("-5s"));
    assertEquals("now-5s", DateTimeUtils.resolveTimeModifier("-5sec"));
    assertEquals("now-5s", DateTimeUtils.resolveTimeModifier("-5secs"));
    assertEquals("now-5s", DateTimeUtils.resolveTimeModifier("-5second"));
    assertEquals("now-5s", DateTimeUtils.resolveTimeModifier("-5seconds"));

    // Test all variants of minutes
    assertEquals("now-5m", DateTimeUtils.resolveTimeModifier("-5m"));
    assertEquals("now-5m", DateTimeUtils.resolveTimeModifier("-5min"));
    assertEquals("now-5m", DateTimeUtils.resolveTimeModifier("-5mins"));
    assertEquals("now-5m", DateTimeUtils.resolveTimeModifier("-5minute"));
    assertEquals("now-5m", DateTimeUtils.resolveTimeModifier("-5minutes"));

    // Test all variants of hours
    assertEquals("now-5h", DateTimeUtils.resolveTimeModifier("-5h"));
    assertEquals("now-5h", DateTimeUtils.resolveTimeModifier("-5hr"));
    assertEquals("now-5h", DateTimeUtils.resolveTimeModifier("-5hrs"));
    assertEquals("now-5h", DateTimeUtils.resolveTimeModifier("-5hour"));
    assertEquals("now-5h", DateTimeUtils.resolveTimeModifier("-5hours"));

    // Test all variants of days
    assertEquals("now-5d", DateTimeUtils.resolveTimeModifier("-5d"));
    assertEquals("now-5d", DateTimeUtils.resolveTimeModifier("-5day"));
    assertEquals("now-5d", DateTimeUtils.resolveTimeModifier("-5days"));

    // Test all variants of weeks
    assertEquals("now-5w", DateTimeUtils.resolveTimeModifier("-5w"));
    assertEquals("now-5w", DateTimeUtils.resolveTimeModifier("-5wk"));
    assertEquals("now-5w", DateTimeUtils.resolveTimeModifier("-5wks"));
    assertEquals("now-5w", DateTimeUtils.resolveTimeModifier("-5week"));
    assertEquals("now-5w", DateTimeUtils.resolveTimeModifier("-5weeks"));

    // Test all variants of months
    assertEquals("now-5M", DateTimeUtils.resolveTimeModifier("-5mon"));
    assertEquals("now-5M", DateTimeUtils.resolveTimeModifier("-5month"));
    assertEquals("now-5M", DateTimeUtils.resolveTimeModifier("-5months"));

    // Test all variants of years
    assertEquals("now-5y", DateTimeUtils.resolveTimeModifier("-5y"));
    assertEquals("now-5y", DateTimeUtils.resolveTimeModifier("-5yr"));
    assertEquals("now-5y", DateTimeUtils.resolveTimeModifier("-5yrs"));
    assertEquals("now-5y", DateTimeUtils.resolveTimeModifier("-5year"));
    assertEquals("now-5y", DateTimeUtils.resolveTimeModifier("-5years"));

    // Test case sensitivity handling
    assertEquals("now-5s", DateTimeUtils.resolveTimeModifier("-5S"));
    assertEquals("now-5m", DateTimeUtils.resolveTimeModifier("-5MIN"));
    assertEquals("now-5h", DateTimeUtils.resolveTimeModifier("-5HOUR"));
    assertEquals("now-5d", DateTimeUtils.resolveTimeModifier("-5DAY"));
    assertEquals("now-5w", DateTimeUtils.resolveTimeModifier("-5WEEK"));
    assertEquals("now-5M", DateTimeUtils.resolveTimeModifier("-5MONTH"));
    assertEquals("now-5y", DateTimeUtils.resolveTimeModifier("-5YEAR"));
  }

  @Test
  void testResolveTimeModifierWithSnap() {
    // Test basic snapping
    assertEquals("now/d", DateTimeUtils.resolveTimeModifier("@d"));
    assertEquals("now/h", DateTimeUtils.resolveTimeModifier("@h"));
    assertEquals("now/w-1d", resolveTimeModifier("@w", wednesday));
    assertEquals("now/M", DateTimeUtils.resolveTimeModifier("@month"));
    assertEquals("now/y", DateTimeUtils.resolveTimeModifier("@y"));

    // Test snapping with all time unit variants
    // Seconds variants
    assertEquals("now/s", DateTimeUtils.resolveTimeModifier("@s"));
    assertEquals("now/s", DateTimeUtils.resolveTimeModifier("@sec"));
    assertEquals("now/s", DateTimeUtils.resolveTimeModifier("@secs"));
    assertEquals("now/s", DateTimeUtils.resolveTimeModifier("@second"));
    assertEquals("now/s", DateTimeUtils.resolveTimeModifier("@seconds"));

    // Minutes variants
    assertEquals("now/m", DateTimeUtils.resolveTimeModifier("@m"));
    assertEquals("now/m", DateTimeUtils.resolveTimeModifier("@min"));
    assertEquals("now/m", DateTimeUtils.resolveTimeModifier("@mins"));
    assertEquals("now/m", DateTimeUtils.resolveTimeModifier("@minute"));
    assertEquals("now/m", DateTimeUtils.resolveTimeModifier("@minutes"));

    // Hours variants
    assertEquals("now/h", DateTimeUtils.resolveTimeModifier("@h"));
    assertEquals("now/h", DateTimeUtils.resolveTimeModifier("@hr"));
    assertEquals("now/h", DateTimeUtils.resolveTimeModifier("@hrs"));
    assertEquals("now/h", DateTimeUtils.resolveTimeModifier("@hour"));
    assertEquals("now/h", DateTimeUtils.resolveTimeModifier("@hours"));

    // Days variants
    assertEquals("now/d", DateTimeUtils.resolveTimeModifier("@d"));
    assertEquals("now/d", DateTimeUtils.resolveTimeModifier("@day"));
    assertEquals("now/d", DateTimeUtils.resolveTimeModifier("@days"));

    // Weeks variants
    assertEquals("now/w-1d", resolveTimeModifier("@w", wednesday));
    assertEquals("now/w-1d", resolveTimeModifier("@wk", thursday));
    assertEquals("now/w-1d", resolveTimeModifier("@wks", wednesday));
    assertEquals("now/w-1d", resolveTimeModifier("@week", wednesday));
    assertEquals("now/w-1d", resolveTimeModifier("@weeks", wednesday));

    // Month variants
    assertEquals("now/M", DateTimeUtils.resolveTimeModifier("@mon"));
    assertEquals("now/M", DateTimeUtils.resolveTimeModifier("@month"));
    assertEquals("now/M", DateTimeUtils.resolveTimeModifier("@months"));

    // Year variants
    assertEquals("now/y", DateTimeUtils.resolveTimeModifier("@y"));
    assertEquals("now/y", DateTimeUtils.resolveTimeModifier("@yr"));
    assertEquals("now/y", DateTimeUtils.resolveTimeModifier("@yrs"));
    assertEquals("now/y", DateTimeUtils.resolveTimeModifier("@year"));
    assertEquals("now/y", DateTimeUtils.resolveTimeModifier("@years"));
  }

  @Test
  void testResolveTimeModifierWithCombined() {
    ZonedDateTime thursday = ZonedDateTime.of(2025, 9, 11, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now-1d/d", DateTimeUtils.resolveTimeModifier("-1d@d"));
    assertEquals("now-3h/h", DateTimeUtils.resolveTimeModifier("-3h@h"));
    assertEquals("now-1M/M", DateTimeUtils.resolveTimeModifier("-1month@month"));
    assertEquals("now+1y/M", DateTimeUtils.resolveTimeModifier("+1y@mon"));
    assertEquals("now-2d-3h/h", DateTimeUtils.resolveTimeModifier("-2d-3h@h"));
    assertEquals("now-1d/w-3d", resolveTimeModifier("-1d@w5", thursday));

    // Test combined formats with different time unit variants
    assertEquals("now-10s/m", DateTimeUtils.resolveTimeModifier("-10seconds@minute"));
    assertEquals("now-1h/d", DateTimeUtils.resolveTimeModifier("-1hour@day"));
    assertEquals("now+2d/w-1d", resolveTimeModifier("+2days@week", thursday));
    assertEquals("now-3w/M", DateTimeUtils.resolveTimeModifier("-3weeks@month"));
    assertEquals("now+1y/y", DateTimeUtils.resolveTimeModifier("+1year@year"));

    // Test multiple offsets with different time unit variants
    assertEquals("now-1d-6h/h", DateTimeUtils.resolveTimeModifier("-1day-6hours@hour"));
    assertEquals("now-2w+3d/d", DateTimeUtils.resolveTimeModifier("-2weeks+3days@day"));
    assertEquals("now-1y+2M-5d/d", DateTimeUtils.resolveTimeModifier("-1year+2months-5days@d"));

    // Test with case variations
    assertEquals("now-1h/d", DateTimeUtils.resolveTimeModifier("-1HOUR@DAY"));
    assertEquals("now+5d/M", DateTimeUtils.resolveTimeModifier("+5Day@Month"));
  }

  @Test
  void testResolveTimeModifierWithInvalidInput() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> DateTimeUtils.resolveTimeModifier("1d+1y"));
    assertEquals("Unexpected character '1' at position 0 in input: 1d+1y", e.getMessage());
    assertThrows(IllegalArgumentException.class, () -> DateTimeUtils.resolveTimeModifier("-1x"));
    assertThrows(
        IllegalArgumentException.class, () -> DateTimeUtils.resolveTimeModifier("@fortnight"));
    assertThrows(
        IllegalArgumentException.class, () -> DateTimeUtils.resolveTimeModifier("-5decades"));
    assertThrows(IllegalArgumentException.class, () -> DateTimeUtils.resolveTimeModifier("@+d"));
  }

  @Test
  void testParseTimeWithReferenceTimeModifier() {
    // Test with different reference dates in different quarters

    ZonedDateTime jan = ZonedDateTime.of(2023, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M", resolveTimeModifier("@q", jan)); // Jan is already start of Q1

    ZonedDateTime feb = ZonedDateTime.of(2023, 2, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M-1M", resolveTimeModifier("@q", feb)); // Feb needs to go back 1 month to Jan

    ZonedDateTime mar = ZonedDateTime.of(2023, 3, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals(
        "now/M-2M", resolveTimeModifier("@q", mar)); // Mar needs to go back 2 months to Jan

    ZonedDateTime apr = ZonedDateTime.of(2023, 4, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M", resolveTimeModifier("@q", apr)); // Apr is already start of Q2

    ZonedDateTime may = ZonedDateTime.of(2023, 5, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M-1M", resolveTimeModifier("@q", may)); // May needs to go back 1 month to Apr

    ZonedDateTime sep = ZonedDateTime.of(2023, 9, 11, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals(
        "now/M-2M", resolveTimeModifier("@q", sep)); // Sep needs to go back 2 months to Jul
  }

  @Test
  void testParseTimeWithReferenceTimeModifierAndOffset() {
    // Test with quarter snap combined with other operations, using fixed reference times

    // March 15, 2023 (Q1)
    ZonedDateTime refTime = ZonedDateTime.of(2023, 3, 15, 10, 0, 0, 0, ZoneOffset.UTC);

    // Quarter snapping with offsets
    assertEquals(
        "now-1d/M-2M", resolveTimeModifier("-1d@q", refTime)); // -1 day, then snap to Q1 (Jan)
    assertEquals(
        "now+5h/M-2M", resolveTimeModifier("+5h@q", refTime)); // +5 hours, then snap to Q1 (Jan)
    assertEquals(
        "now-2M/M", resolveTimeModifier("-2month@q", refTime)); // -2 months, then snap to Q1 (Jan)
    assertEquals(
        "now+1M/M", resolveTimeModifier("+1month@q", refTime)); // +1 month, then snap to Q2 (April)

    // Quarter offsets
    assertEquals("now-3M", resolveTimeModifier("-1q", refTime)); // -1 quarter = -3 months
    assertEquals("now-6M", resolveTimeModifier("-2q", refTime)); // -2 quarters = -6 months
    assertEquals("now+3M", resolveTimeModifier("+1q", refTime)); // +1 quarter = +3 months

    // Multiple operations with quarters
    assertEquals("now-3M+1d", resolveTimeModifier("-1q+1d", refTime)); // -1 quarter then +1 day
    assertEquals(
        "now-3M/M-2M", resolveTimeModifier("-1q@q", refTime)); // -1 quarter then snap to quarter
  }

  @Test
  void testWeekDaySnapping() {
    // Test all week day snap variants
    assertEquals("now/w-1d", resolveTimeModifier("@w", thursday));
    assertEquals("now/w-1d", resolveTimeModifier("@week", thursday));
    assertEquals("now/w", resolveTimeModifier("@w1", thursday)); // Monday (OpenSearch default)
    assertEquals("now/w-1d", resolveTimeModifier("@w0", thursday)); // Sunday
    assertEquals("now/w-1d", resolveTimeModifier("@w7", thursday)); // Sunday (alternative)
    assertEquals("now/w+1d", resolveTimeModifier("@w2", thursday)); // Tuesday
    assertEquals("now/w+2d", resolveTimeModifier("@w3", thursday)); // Wednesday
    assertEquals("now/w+3d", resolveTimeModifier("@w4", thursday)); // Thursday
    assertEquals("now/w-3d", resolveTimeModifier("@w5", thursday)); // Friday
    assertEquals("now/w-2d", resolveTimeModifier("@w6", thursday)); // Saturday

    // Test with offsets
    assertEquals("now-1d/w", resolveTimeModifier("-1d@w1", thursday)); // Monday with offset
    assertEquals("now+2h/w-1d", resolveTimeModifier("+2h@w0", thursday)); // Sunday with offset
    assertEquals("now-3h/w+2d", resolveTimeModifier("-3h@w3", thursday)); // Wednesday with offset

    // Both reference and target are Sunday
    assertEquals("now/w+6d", resolveTimeModifier("@w", sunday));

    // Both reference and target are monday
    assertEquals("now/w", resolveTimeModifier("@w1", monday));
  }
}
