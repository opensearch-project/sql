/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprDateValue;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.function.udf.datetime.StrftimeFunction;

/**
 * Unit tests for STRFTIME function implementation. Tests the core functionality without going
 * through the full expression system. All tests use Locale.ROOT for consistent results.
 */
public class StrftimeFunctionTest {
  @Test
  public void testStrftimeWithBasicFormat() {
    // Unix timestamp for 2018-03-19T13:55:03 UTC
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(unixTime), new ExprStringValue("%Y-%m-%dT%H:%M:%S"));
    assertEquals("2018-03-19T13:55:03", result);
  }

  @Test
  public void testStrftimeWithISOFormat() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%F %T"));
    assertEquals("2018-03-19 13:55:03", result);
  }

  @Test
  public void testStrftimeWithWeekdayAndMonth() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(unixTime), new ExprStringValue("%a %b %d, %Y"));
    assertEquals("Mon Mar 19, 2018", result);
  }

  @Test
  public void testStrftimeWithFullWeekdayAndMonth() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(unixTime), new ExprStringValue("%A %B %d, %Y"));
    assertEquals("Mon Mar 19, 2018", result);
  }

  @Test
  public void testStrftimeWith12HourFormat() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(unixTime), new ExprStringValue("%I:%M:%S %p"));
    assertEquals("01:55:03 PM", result);
  }

  @Test
  public void testStrftimeWithEpochSeconds() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%s"));
    assertEquals("1521467703", result);
  }

  @Test
  public void testStrftimeWithLongTimestamp() {
    // Test with millisecond timestamp (auto-detected and converted to seconds)
    long unixTime = 1521467703123L; // milliseconds
    String result =
        StrftimeFunction.strftime(
            new ExprLongValue(unixTime), new ExprStringValue("%Y-%m-%dT%H:%M:%S"));
    assertEquals("2018-03-19T13:55:03", result);
  }

  @Test
  public void testStrftimeWithSubsecondPrecision() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%3Q"));
    assertEquals("03.000", result);
    result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%6Q"));
    assertEquals("03.000000", result);
    result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%9Q"));
    assertEquals("03.000000000", result);
    result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%5Q"));
    assertEquals("03.000", result);
    result = StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%Q"));
    assertEquals("03.000", result);
  }

  @Test
  public void testStrftimeWithNanosecondPrecision() {
    // Test with fractional seconds (using value with limited precision to avoid floating point
    // issues)
    double unixTime = 1521467703.125; // Use simple fraction to avoid precision issues
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%N"));
    assertEquals("03.125000000", result);
    result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%3N"));
    assertEquals("03.125", result);
    result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%6N"));
    assertEquals("03.125000", result);
    result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%9N"));
    assertEquals("03.125000000", result);
    result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%S.%5N"));
    assertEquals("03.12500", result);
  }

  @Test
  public void testStrftimeWithMicroseconds() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%f"));
    assertEquals("000000", result);
  }

  @Test
  public void testStrftimeWithWeekNumbers() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%V %U %w"));
    assertEquals("12 11 1", result);
  }

  @Test
  public void testStrftimeWithDayOfYear() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%j"));
    assertEquals("078", result);
  }

  @Test
  public void testStrftimeWithCentury() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%C"));
    assertEquals("20", result);
  }

  @Test
  public void testStrftimeWithYearFormats() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(unixTime), new ExprStringValue("%Y %y %G %g"));
    assertEquals("2018 18 2018 18", result);
  }

  @Test
  public void testStrftimeWithSpacePaddedFormats() {
    long unixTime = 1517472303L; // 2018-02-01 08:05:03
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%e %k"));
    assertEquals(" 1  8", result);
  }

  @Test
  public void testStrftimeWithPercentLiteral() {
    long unixTime = 1521467703L;
    // Test literal percent: %% should become %
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%%"));
    assertEquals("%", result);
    // %%Y should become %Y (not expanding Y)
    result = StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%%Y"));
    assertEquals("%Y", result);
  }

  @Test
  public void testStrftimeWithStringTimestamp() {
    // String inputs are not supported - should return null
    String result =
        StrftimeFunction.strftime(
            new ExprStringValue("1521467703"), new ExprStringValue("%Y-%m-%d"));
    assertNull(result);
  }

  @Test
  public void testStrftimeWithLongValue() {
    String result =
        StrftimeFunction.strftime(new ExprLongValue(1521467703L), new ExprStringValue("%Y-%m-%d"));
    assertEquals("2018-03-19", result);
  }

  @Test
  public void testStrftimeWithTimestampValue() {
    // Create timestamp using Instant for proper epoch second conversion
    Instant instant = Instant.ofEpochSecond(1521467703L); // 2018-03-19T13:55:03 UTC
    ExprTimestampValue timestamp = new ExprTimestampValue(instant);
    String result = StrftimeFunction.strftime(timestamp, new ExprStringValue("%Y-%m-%d %H:%M:%S"));
    assertEquals("2018-03-19 13:55:03", result);
  }

  @Test
  public void testStrftimeWithDateValue() {
    // Create a DATE value - should convert to timestamp at midnight UTC
    LocalDate date = LocalDate.of(2020, 9, 16);
    ExprDateValue dateValue = new ExprDateValue(date);
    String result = StrftimeFunction.strftime(dateValue, new ExprStringValue("%Y-%m-%d"));
    assertEquals("2020-09-16", result);

    // Also verify time is at midnight
    result = StrftimeFunction.strftime(dateValue, new ExprStringValue("%Y-%m-%d %H:%M:%S"));
    assertEquals("2020-09-16 00:00:00", result);
  }

  @Test
  public void testStrftimeWithNullTimestamp() {
    String result = StrftimeFunction.strftime(ExprNullValue.of(), new ExprStringValue("%Y-%m-%d"));
    assertNull(result);
  }

  @Test
  public void testStrftimeWithNullFormat() {
    String result = StrftimeFunction.strftime(new ExprDoubleValue(1521467703L), ExprNullValue.of());
    assertNull(result);
  }

  @Test
  public void testStrftimeWithNegativeTimestamp() {
    // Negative timestamps represent dates before 1970
    // -1 represents 1969-12-31 23:59:59 UTC
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(-1), new ExprStringValue("%Y-%m-%d"));
    assertEquals("1969-12-31", result);

    // Test another negative timestamp: -86400 = 1969-12-31 00:00:00 UTC
    result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(-86400), new ExprStringValue("%Y-%m-%d %H:%M:%S"));
    assertEquals("1969-12-31 00:00:00", result);
  }

  @Test
  public void testStrftimeWithOutOfRangeTimestamp() {
    // 32536771200 is above the max valid range (32536771199)
    // This should return null because it's out of the valid FROM_UNIXTIME range
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(32536771200.0), // Above max range by 1 second
            new ExprStringValue("%Y-%m-%d"));
    assertNull(result);
  }

  @Test
  public void testStrftimeWithInvalidStringTimestamp() {
    String result =
        StrftimeFunction.strftime(new ExprStringValue("invalid"), new ExprStringValue("%Y-%m-%d"));
    assertNull(result);
  }

  @Test
  public void testStrftimeFormatterUtilExtractUnixSeconds() {
    // Test extracting first 10 digits
    assertEquals(1521467703L, StrftimeFormatterUtil.extractUnixSeconds(1521467703049000000.0));
    assertEquals(1521467703L, StrftimeFormatterUtil.extractUnixSeconds(1521467703.0));
    assertEquals(152146770L, StrftimeFormatterUtil.extractUnixSeconds(152146770.0));
  }

  @Test
  public void testStrftimeWithComplexFormat() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(unixTime), new ExprStringValue("Date: %F, Time: %T, Weekday: %A"));
    assertEquals("Date: 2018-03-19, Time: 13:55:03, Weekday: Mon", result);
  }

  @Test
  public void testStrftimeWithLocaleFormats() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%c"));
    assertEquals("Mon Mar 19 13:55:03 2018", result);
    result = StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%+"));
    assertEquals("Mon Mar 19 13:55:03 UTC 2018", result);
  }

  @Test
  public void testStrftimeWithDateLocaleFormat() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%x"));
    assertEquals("03/19/2018", result);
  }

  @Test
  public void testStrftimeWithTimeLocaleFormat() {
    long unixTime = 1521467703L;
    String result =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%X"));
    assertEquals("13:55:03", result);
  }

  @Test
  public void testStrftimeWithTimezoneFormats() {
    long unixTime = 1521467703L;
    // Test %Ez - timezone offset in minutes
    String ezResult =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%Ez"));
    assertEquals("+0", ezResult);
    // Test %z - timezone offset +hhmm
    String zResult =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%z"));
    assertEquals("+0000", zResult);
    // Test %Z - timezone abbreviation
    String bigZResult =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%Z"));
    assertEquals("UTC", bigZResult);
    // Test %:z - timezone offset with colon
    String colonZResult =
        StrftimeFunction.strftime(new ExprDoubleValue(unixTime), new ExprStringValue("%:z"));
    assertEquals("+00:00", colonZResult);
  }

  @Test
  public void testStrftimeFormatterDirectly() {
    // Test the formatter utility directly
    Instant instant = Instant.ofEpochSecond(1521467703L);
    ZonedDateTime dateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));
    ExprValue result = StrftimeFormatterUtil.formatZonedDateTime(dateTime, "%Y-%m-%d");
    assertEquals("2018-03-19", result.stringValue());
  }

  @Test
  public void testStrftimeFunctionConstructor() {
    // Test the constructor and basic setup
    StrftimeFunction function = new StrftimeFunction();
    assertNotNull(function);
    assertNotNull(function.getReturnTypeInference());
    assertNotNull(function.getOperandMetadata());
  }

  @Test
  public void testExtractNumericValueWithNonStringExprValue() {
    // Test with a custom ExprValue that doesn't match our supported types
    // Since we removed string support, custom ExprValue types that aren't
    // ExprDoubleValue, ExprLongValue, ExprIntegerValue, or ExprFloatValue
    // will return null
    ExprValue customValue =
        new ExprValue() {
          @Override
          public Object value() {
            return 1521467703;
          }

          @Override
          public ExprType type() {
            return ExprCoreType.INTEGER;
          }

          @Override
          public String toString() {
            return "1521467703";
          }

          @Override
          public int compareTo(ExprValue other) {
            return 0;
          }

          @Override
          public Object valueForCalcite() {
            return value();
          }
        };

    String result = StrftimeFunction.strftime(customValue, new ExprStringValue("%Y-%m-%d"));
    // Custom ExprValue implementations that aren't one of our supported types return null
    assertNull(result);
  }

  @Test
  public void testIsValidTimestampBoundaries() {
    // Test boundary conditions for isValidTimestamp

    // Test exactly at max boundary
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(32536771199L), new ExprStringValue("%Y-%m-%d"));
    assertEquals("3001-01-18", result);

    // Test exactly at 0 (epoch)
    result =
        StrftimeFunction.strftime(new ExprDoubleValue(0), new ExprStringValue("%Y-%m-%d %H:%M:%S"));
    assertEquals("1970-01-01 00:00:00", result);
  }

  @Test
  public void testExtractNumericValueWithInvalidDouble() {
    // Test parsing failure in extractNumericValue
    ExprValue customValue =
        new ExprValue() {
          @Override
          public Object value() {
            return "not_a_number";
          }

          @Override
          public ExprType type() {
            return ExprCoreType.STRING;
          }

          @Override
          public String toString() {
            return "not_a_number";
          }

          @Override
          public int compareTo(ExprValue other) {
            return 0;
          }

          @Override
          public Object valueForCalcite() {
            return value();
          }
        };

    String result = StrftimeFunction.strftime(customValue, new ExprStringValue("%Y-%m-%d"));
    assertNull(result);
  }

  @Test
  public void testExtractFirstNDigitsWithShortNumber() {
    // Test extractFirstNDigits with a number shorter than requested digits
    // This covers the branch where valueStr.length() <= digits
    assertEquals(123L, StrftimeFormatterUtil.extractUnixSeconds(123.0));

    // Test with exactly 10 digits
    assertEquals(1234567890L, StrftimeFormatterUtil.extractUnixSeconds(1234567890.0));
  }

  @Test
  public void testExtractFirstNDigitsWithLongNumber() {
    // Test extractFirstNDigits with a number longer than 10 digits
    // This covers the branch where substring is used
    assertEquals(1234567890L, StrftimeFormatterUtil.extractUnixSeconds(12345678901234.0));
    assertEquals(1521467703L, StrftimeFormatterUtil.extractUnixSeconds(1521467703123456789.0));
  }

  @Test
  public void testStrftimeWithBoundaryTimestamp() {
    // Test with timestamp at the upper boundary (32536771199)
    long maxTimestamp = 32536771199L;
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(maxTimestamp), new ExprStringValue("%Y-%m-%d %H:%M:%S"));
    assertEquals("3001-01-18 23:59:59", result);

    // Test with timestamp just above the boundary (should return null)
    result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(maxTimestamp + 1), new ExprStringValue("%Y-%m-%d"));
    assertNull(result);
  }

  @Test
  public void testStrftimeWithLargeValueAsMilliseconds() {
    // Test with value > MAX_UNIX_TIMESTAMP that should be interpreted as milliseconds
    // Note: values < 100 billion are treated as invalid seconds, not milliseconds
    // This avoids treating values like 32536771200 (just above max) as milliseconds

    // Test with current time in milliseconds (around 1.7 trillion)
    // 1600000000000 ms = 1600000000 seconds = 2020-09-13
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(1600000000000L), new ExprStringValue("%Y-%m-%d"));
    assertEquals("2020-09-13", result);

    // Test with negative value that's within seconds range
    // -86400000 is treated as seconds (not milliseconds) since it's within valid range
    // -86400000 seconds = 1967-04-07
    result =
        StrftimeFunction.strftime(new ExprDoubleValue(-86400000L), new ExprStringValue("%Y-%m-%d"));
    assertEquals("1967-04-07", result);
  }

  @Test
  public void testStrftimeWithValueTooLargeEvenAsMilliseconds() {
    // Test with value so large that even as milliseconds it exceeds the range
    // 32536771199999 ms would be 32536771199.999 seconds (beyond max)
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(32536771200000L), // Too large even as milliseconds
            new ExprStringValue("%Y-%m-%d"));
    assertNull(result);
  }

  @Test
  public void testStrftimeDeadZone() {
    // Values between MAX_UNIX_TIMESTAMP and 100 billion are treated as invalid
    // This prevents values like 32536771200 (just above max) from being misinterpreted

    // 50 billion - in the "dead zone", returns null
    String result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(50000000000L), new ExprStringValue("%Y-%m-%d"));
    assertNull(result);

    // Just above MAX_UNIX_TIMESTAMP - should return null, not interpret as milliseconds
    result =
        StrftimeFunction.strftime(
            new ExprDoubleValue(32536771200L), new ExprStringValue("%Y-%m-%d"));
    assertNull(result);
  }
}
