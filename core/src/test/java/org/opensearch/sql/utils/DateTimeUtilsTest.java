/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.opensearch.sql.utils.DateTimeUtils.getRelativeZonedDateTime;
import static org.opensearch.sql.utils.DateTimeUtils.parseRelativeTime;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.sql.planner.physical.collector.Rounding.DateTimeUnit;

public class DateTimeUtilsTest {
  private final DateFormatter formatter =
      DateFormatter.forPattern("strict_date_optional_time||epoch_millis");
  private final DateMathParser parser = formatter.toDateMathParser();
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
  void testParseRelativeTimeNull() {
    assertNull(parseRelativeTime(null));
    assertNull(parseRelativeTime(""));
  }

  @Test
  void testParseRelativeTimeWithNow() {
    assertEquals("now", parseRelativeTime("now"));
    assertEquals("now", parseRelativeTime("now()"));
  }

  @Test
  void testParseRelativeTimeWithDatetimeString() {
    assertEquals("2025-10-22", parseRelativeTime("2025-10-22"));
    assertEquals("2025-10-22T10:32:12Z", parseRelativeTime("2025-10-22 10:32:12"));
  }

  @Test
  void testParseRelativeTimeWithOffsets() {
    assertEquals("now-30s", parseRelativeTime("-30s"));
    assertEquals("now-1h", parseRelativeTime("-1h"));
    assertEquals("now+1d", parseRelativeTime("+1d"));
    assertEquals("now-3M", parseRelativeTime("-3month"));
    assertEquals("now-1y", parseRelativeTime("-1year"));
  }

  @Test
  void testTimeUnitVariants() {
    // Test all variants of seconds
    assertEquals("now-5s", parseRelativeTime("-5s"));
    assertEquals("now-5s", parseRelativeTime("-5sec"));
    assertEquals("now-5s", parseRelativeTime("-5secs"));
    assertEquals("now-5s", parseRelativeTime("-5second"));
    assertEquals("now-5s", parseRelativeTime("-5seconds"));

    // Test all variants of minutes
    assertEquals("now-5m", parseRelativeTime("-5m"));
    assertEquals("now-5m", parseRelativeTime("-5min"));
    assertEquals("now-5m", parseRelativeTime("-5mins"));
    assertEquals("now-5m", parseRelativeTime("-5minute"));
    assertEquals("now-5m", parseRelativeTime("-5minutes"));

    // Test all variants of hours
    assertEquals("now-5h", parseRelativeTime("-5h"));
    assertEquals("now-5h", parseRelativeTime("-5hr"));
    assertEquals("now-5h", parseRelativeTime("-5hrs"));
    assertEquals("now-5h", parseRelativeTime("-5hour"));
    assertEquals("now-5h", parseRelativeTime("-5hours"));

    // Test all variants of days
    assertEquals("now-5d", parseRelativeTime("-5d"));
    assertEquals("now-5d", parseRelativeTime("-5day"));
    assertEquals("now-5d", parseRelativeTime("-5days"));

    // Test all variants of weeks
    assertEquals("now-5w", parseRelativeTime("-5w"));
    assertEquals("now-5w", parseRelativeTime("-5wk"));
    assertEquals("now-5w", parseRelativeTime("-5wks"));
    assertEquals("now-5w", parseRelativeTime("-5week"));
    assertEquals("now-5w", parseRelativeTime("-5weeks"));

    // Test all variants of months
    assertEquals("now-5M", parseRelativeTime("-5mon"));
    assertEquals("now-5M", parseRelativeTime("-5month"));
    assertEquals("now-5M", parseRelativeTime("-5months"));

    // Test all variants of years
    assertEquals("now-5y", parseRelativeTime("-5y"));
    assertEquals("now-5y", parseRelativeTime("-5yr"));
    assertEquals("now-5y", parseRelativeTime("-5yrs"));
    assertEquals("now-5y", parseRelativeTime("-5year"));
    assertEquals("now-5y", parseRelativeTime("-5years"));

    // Test case sensitivity handling
    assertEquals("now-5s", parseRelativeTime("-5S"));
    assertEquals("now-5m", parseRelativeTime("-5MIN"));
    assertEquals("now-5h", parseRelativeTime("-5HOUR"));
    assertEquals("now-5d", parseRelativeTime("-5DAY"));
    assertEquals("now-5w", parseRelativeTime("-5WEEK"));
    assertEquals("now-5M", parseRelativeTime("-5MONTH"));
    assertEquals("now-5y", parseRelativeTime("-5YEAR"));
  }

  @Test
  void testParseRelativeTimeWithSnap() {
    // Test basic snapping
    assertEquals("now/d", parseRelativeTime("@d"));
    assertEquals("now/h", parseRelativeTime("@h"));
    assertEquals("now/w-1d", parseRelativeTime("@w", wednesday));
    assertEquals("now/M", parseRelativeTime("@month"));
    assertEquals("now/y", parseRelativeTime("@y"));

    // Test snapping with all time unit variants
    // Seconds variants
    assertEquals("now/s", parseRelativeTime("@s"));
    assertEquals("now/s", parseRelativeTime("@sec"));
    assertEquals("now/s", parseRelativeTime("@secs"));
    assertEquals("now/s", parseRelativeTime("@second"));
    assertEquals("now/s", parseRelativeTime("@seconds"));

    // Minutes variants
    assertEquals("now/m", parseRelativeTime("@m"));
    assertEquals("now/m", parseRelativeTime("@min"));
    assertEquals("now/m", parseRelativeTime("@mins"));
    assertEquals("now/m", parseRelativeTime("@minute"));
    assertEquals("now/m", parseRelativeTime("@minutes"));

    // Hours variants
    assertEquals("now/h", parseRelativeTime("@h"));
    assertEquals("now/h", parseRelativeTime("@hr"));
    assertEquals("now/h", parseRelativeTime("@hrs"));
    assertEquals("now/h", parseRelativeTime("@hour"));
    assertEquals("now/h", parseRelativeTime("@hours"));

    // Days variants
    assertEquals("now/d", parseRelativeTime("@d"));
    assertEquals("now/d", parseRelativeTime("@day"));
    assertEquals("now/d", parseRelativeTime("@days"));

    // Weeks variants
    assertEquals("now/w-1d", parseRelativeTime("@w", wednesday));
    assertEquals("now/w-1d", parseRelativeTime("@wk", thursday));
    assertEquals("now/w-1d", parseRelativeTime("@wks", wednesday));
    assertEquals("now/w-1d", parseRelativeTime("@week", wednesday));
    assertEquals("now/w-1d", parseRelativeTime("@weeks", wednesday));

    // Month variants
    assertEquals("now/M", parseRelativeTime("@mon"));
    assertEquals("now/M", parseRelativeTime("@month"));
    assertEquals("now/M", parseRelativeTime("@months"));

    // Year variants
    assertEquals("now/y", parseRelativeTime("@y"));
    assertEquals("now/y", parseRelativeTime("@yr"));
    assertEquals("now/y", parseRelativeTime("@yrs"));
    assertEquals("now/y", parseRelativeTime("@year"));
    assertEquals("now/y", parseRelativeTime("@years"));
  }

  @Test
  void testParseRelativeTimeWithCombined() {
    ZonedDateTime thursday = ZonedDateTime.of(2025, 9, 11, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now-1d/d", parseRelativeTime("-1d@d"));
    assertEquals("now-3h/h", parseRelativeTime("-3h@h"));
    assertEquals("now-1M/M", parseRelativeTime("-1month@month"));
    assertEquals("now+1y/M", parseRelativeTime("+1y@mon"));
    assertEquals("now-2d-3h/h", parseRelativeTime("-2d-3h@h"));
    assertEquals("now-1d/w-3d", parseRelativeTime("-1d@w5", thursday));

    // Test combined formats with different time unit variants
    assertEquals("now-10s/m", parseRelativeTime("-10seconds@minute"));
    assertEquals("now-1h/d", parseRelativeTime("-1hour@day"));
    assertEquals("now+2d/w-1d", parseRelativeTime("+2days@week", thursday));
    assertEquals("now-3w/M", parseRelativeTime("-3weeks@month"));
    assertEquals("now+1y/y", parseRelativeTime("+1year@year"));

    // Test multiple offsets with different time unit variants
    assertEquals("now-1d-6h/h", parseRelativeTime("-1day-6hours@hour"));
    assertEquals("now-2w+3d/d", parseRelativeTime("-2weeks+3days@day"));
    assertEquals("now-1y+2M-5d/d", parseRelativeTime("-1year+2months-5days@d"));

    // Test with case variations
    assertEquals("now-1h/d", parseRelativeTime("-1HOUR@DAY"));
    assertEquals("now+5d/M", parseRelativeTime("+5Day@Month"));
  }

  @Test
  void testParseRelativeTimeWithInvalidInput() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> parseRelativeTime("1d+1y"));
    assertEquals("Unexpected character '1' at position 0 in input: 1d+1y", e.getMessage());
    assertThrows(IllegalArgumentException.class, () -> parseRelativeTime("-1x"));
    assertThrows(IllegalArgumentException.class, () -> parseRelativeTime("@fortnight"));
    assertThrows(IllegalArgumentException.class, () -> parseRelativeTime("-5decades"));
    assertThrows(IllegalArgumentException.class, () -> parseRelativeTime("@+d"));
  }

  @Test
  void testOpenSearchTimeEquivalence() {
    // Fixed reference time for testing
    ZonedDateTime baseZdt = tuesday;
    long baseMilli = baseZdt.toInstant().toEpochMilli();

    // Test cases for various formats
    Object[][] testCases = {
      // PPL format, OpenSearch format
      {"-30s", "now-30s"},
      {"-1h", "now-1h"},
      {"+5m", "now+5m"},
      {"-7d", "now-7d"},
      {"@s", "now/s"},
      {"@m", "now/m"},
      {"@h", "now/h"},
      {"@d", "now/d"},
      {"@w1", "now/w"},
      {"@month", "now/M"},
      {"@year", "now/y"},
      {"-1d@d", "now-1d/d"},
      {"-30m@h", "now-30m/h"},
      {"-2w+1d", "now-2w+1d"},
      {"-1month@mon", "now-1M/M"}
    };

    for (Object[] testCase : testCases) {
      String pplFormat = (String) testCase[0];
      String osFormat = (String) testCase[1];

      // Parse with PPL format
      ZonedDateTime pplParsed = getRelativeZonedDateTime(pplFormat, baseZdt);
      String converted = parseRelativeTime(pplFormat, baseZdt);

      // Parse with OpenSearch format
      Instant osParsed = parser.parse(osFormat, () -> baseMilli, false, ZoneId.of("UTC"));

      assertEquals(osFormat, converted);

      assertEquals(
          pplParsed.toInstant(),
          osParsed,
          String.format(
              "PPL '%s' and OpenSearch '%s' should yield the same instant", pplFormat, osFormat));
    }
  }

  @Test
  void testConversionConsistency() throws Exception {
    // Fixed reference time for testing
    String baseTimeString = "2014-11-18T14:27:32";
    ZonedDateTime baseTime = ZonedDateTime.parse(baseTimeString + "Z");
    long now = baseTime.toInstant().toEpochMilli();

    // Test cases for PPL formats
    String[] pplFormats = {
      "-30s",
      "-1h",
      "+5m",
      "-7d",
      "@s",
      "@m",
      "@h",
      "@d",
      "@month",
      "@year",
      "-1d@d",
      "-30m@h",
      "-2w+1d",
      "-1month@mon"
      // "+1year@q" removed as quarter handling differs between implementations
    };

    for (String pplFormat : pplFormats) {
      // Parse directly with PPL format
      ZonedDateTime directPPLParsed = getRelativeZonedDateTime(pplFormat, baseTime);

      // Convert to OpenSearch format then parse
      String osFormat = parseRelativeTime(pplFormat);
      Instant osParsed = parser.parse(osFormat, () -> now, false, ZoneId.of("UTC"));

      // Verify time string conversion produces the same datetime value
      assertEquals(
          directPPLParsed.toInstant(),
          osParsed,
          String.format(
              "Direct PPL parsing of '%s' and OpenSearch parsing of converted '%s' should match",
              pplFormat, osFormat));
    }
  }

  @Test
  void testSpecialCases() throws Exception {
    // Fixed reference time for testing
    ZonedDateTime baseTime = tuesday;
    long now = baseTime.toInstant().toEpochMilli();

    // Special cases that need approximate matching due to format differences
    Object[][] testCases = {
      {"-1q", "now-3M"}, // Quarter (3 months) approximation
      {"@w2", "now/w+1d"},
      {"-1d@w5", "now-1d/w-3d"}
    };

    for (Object[] testCase : testCases) {
      String pplFormat = (String) testCase[0];
      String osFormat = (String) testCase[1];

      ZonedDateTime pplParsed = getRelativeZonedDateTime(pplFormat, baseTime);
      String converted = parseRelativeTime(pplFormat, baseTime);
      Instant osParsed = parser.parse(osFormat, () -> now, false, ZoneId.of("UTC"));

      assertEquals(osFormat, converted);
      assertEquals(osParsed, pplParsed.toInstant());
    }
  }

  @Test
  void testParseRelativeTimeWithReferenceTime() {
    // Test with different reference dates in different quarters

    ZonedDateTime jan = ZonedDateTime.of(2023, 1, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M", parseRelativeTime("@q", jan)); // Jan is already start of Q1

    ZonedDateTime feb = ZonedDateTime.of(2023, 2, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M-1M", parseRelativeTime("@q", feb)); // Feb needs to go back 1 month to Jan

    ZonedDateTime mar = ZonedDateTime.of(2023, 3, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M-2M", parseRelativeTime("@q", mar)); // Mar needs to go back 2 months to Jan

    ZonedDateTime apr = ZonedDateTime.of(2023, 4, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M", parseRelativeTime("@q", apr)); // Apr is already start of Q2

    ZonedDateTime may = ZonedDateTime.of(2023, 5, 15, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M-1M", parseRelativeTime("@q", may)); // May needs to go back 1 month to Apr

    ZonedDateTime sep = ZonedDateTime.of(2023, 9, 11, 10, 0, 0, 0, ZoneOffset.UTC);
    assertEquals("now/M-2M", parseRelativeTime("@q", sep)); // Sep needs to go back 2 months to Jul
  }

  @Test
  void testParseRelativeTimeWithReferenceTimeAndOffset() {
    // Test with quarter snap combined with other operations, using fixed reference times

    // March 15, 2023 (Q1)
    ZonedDateTime refTime = ZonedDateTime.of(2023, 3, 15, 10, 0, 0, 0, ZoneOffset.UTC);

    // Quarter snapping with offsets
    assertEquals(
        "now-1d/M-2M", parseRelativeTime("-1d@q", refTime)); // -1 day, then snap to Q1 (Jan)
    assertEquals(
        "now+5h/M-2M", parseRelativeTime("+5h@q", refTime)); // +5 hours, then snap to Q1 (Jan)
    assertEquals(
        "now-2M/M", parseRelativeTime("-2month@q", refTime)); // -2 months, then snap to Q1 (Jan)
    assertEquals(
        "now+1M/M", parseRelativeTime("+1month@q", refTime)); // +1 month, then snap to Q2 (April)

    // Quarter offsets
    assertEquals("now-3M", parseRelativeTime("-1q", refTime)); // -1 quarter = -3 months
    assertEquals("now-6M", parseRelativeTime("-2q", refTime)); // -2 quarters = -6 months
    assertEquals("now+3M", parseRelativeTime("+1q", refTime)); // +1 quarter = +3 months

    // Multiple operations with quarters
    assertEquals("now-3M+1d", parseRelativeTime("-1q+1d", refTime)); // -1 quarter then +1 day
    assertEquals(
        "now-3M/M-2M", parseRelativeTime("-1q@q", refTime)); // -1 quarter then snap to quarter
  }

  @Test
  void testQuarterHandlingWithDateMathParser() throws Exception {
    // Fixed reference times for testing
    ZonedDateTime refSep = ZonedDateTime.of(2023, 9, 11, 10, 0, 0, 0, ZoneOffset.UTC);
    long nowSep = refSep.toInstant().toEpochMilli();

    // Expected quarter start for September is July 1
    ZonedDateTime expectedJul = ZonedDateTime.of(2023, 7, 1, 0, 0, 0, 0, ZoneOffset.UTC);

    // Convert @q using our parseRelativeTime with the September reference date
    String osFormatForQ = parseRelativeTime("@q", refSep); // Should be "now/M-2M"

    // Parse the OpenSearch format using DateMathParser
    Instant osParsed = parser.parse(osFormatForQ, () -> nowSep, false, ZoneId.of("UTC"));

    assertEquals(
        expectedJul.toInstant(),
        osParsed,
        "Quarter snapping from September 11 should result in July 1");
  }

  @Test
  void testWeekDaySnapping() {
    // Test all week day snap variants
    assertEquals("now/w-1d", parseRelativeTime("@w", thursday));
    assertEquals("now/w-1d", parseRelativeTime("@week", thursday));
    assertEquals("now/w", parseRelativeTime("@w1", thursday)); // Monday (OpenSearch default)
    assertEquals("now/w-1d", parseRelativeTime("@w0", thursday)); // Sunday
    assertEquals("now/w-1d", parseRelativeTime("@w7", thursday)); // Sunday (alternative)
    assertEquals("now/w+1d", parseRelativeTime("@w2", thursday)); // Tuesday
    assertEquals("now/w+2d", parseRelativeTime("@w3", thursday)); // Wednesday
    assertEquals("now/w+3d", parseRelativeTime("@w4", thursday)); // Thursday
    assertEquals("now/w-3d", parseRelativeTime("@w5", thursday)); // Friday
    assertEquals("now/w-2d", parseRelativeTime("@w6", thursday)); // Saturday

    // Test with offsets
    assertEquals("now-1d/w", parseRelativeTime("-1d@w1", thursday)); // Monday with offset
    assertEquals("now+2h/w-1d", parseRelativeTime("+2h@w0", thursday)); // Sunday with offset
    assertEquals("now-3h/w+2d", parseRelativeTime("-3h@w3", thursday)); // Wednesday with offset

    // Both reference and target are Sunday
    assertEquals("now/w+6d", parseRelativeTime("@w", sunday));

    // Both reference and target are monday
    assertEquals("now/w", parseRelativeTime("@w1", monday));
  }
}
