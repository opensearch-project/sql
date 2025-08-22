/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Locale;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.udf.datetime.StrftimeFunction;

/**
 * Unit tests for STRFTIME function implementation. Tests the core functionality without going
 * through the full expression system.
 */
public class StrftimeFunctionTest {

  private static Locale originalLocale;

  @BeforeAll
  public static void setUpLocale() {
    // Save the original locale
    originalLocale = Locale.getDefault();
    // Set locale to US for consistent test results
    Locale.setDefault(Locale.US);
  }

  @AfterAll
  public static void restoreLocale() {
    // Restore the original locale after tests
    Locale.setDefault(originalLocale);
  }

  @Test
  public void testStrftimeWithBasicFormat() {
    // Unix timestamp for 2018-03-19T13:55:03 UTC
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprDoubleValue(unixTime),
            new ExprStringValue("%Y-%m-%dT%H:%M:%S"));

    assertEquals("2018-03-19T13:55:03", result);
  }

  @Test
  public void testStrftimeWithISOFormat() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%F %T"));

    assertEquals("2018-03-19 13:55:03", result);
  }

  @Test
  public void testStrftimeWithWeekdayAndMonth() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprDoubleValue(unixTime),
            new ExprStringValue("%a %b %d, %Y"));

    assertEquals("Mon Mar 19, 2018", result);
  }

  @Test
  public void testStrftimeWithFullWeekdayAndMonth() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprDoubleValue(unixTime),
            new ExprStringValue("%A %B %d, %Y"));

    assertEquals("Monday March 19, 2018", result);
  }

  @Test
  public void testStrftimeWith12HourFormat() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprDoubleValue(unixTime),
            new ExprStringValue("%I:%M:%S %p"));

    assertEquals("01:55:03 PM", result);
  }

  @Test
  public void testStrftimeWithEpochSeconds() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%s"));

    assertEquals("1521467703", result);
  }

  @Test
  public void testStrftimeWithLongTimestamp() {
    // Timestamp with nanoseconds
    long unixTime = 1521467703049000000L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprDoubleValue(unixTime),
            new ExprStringValue("%Y-%m-%dT%H:%M:%S.%Q"));

    assertEquals("2018-03-19T13:55:03.000", result);
  }

  @Test
  public void testStrftimeWithSubsecondPrecision() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%S.%3Q"));

    assertEquals("03.000", result);
  }

  @Test
  public void testStrftimeWithMicroseconds() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%f"));

    assertEquals("000000", result);
  }

  @Test
  public void testStrftimeWithWeekNumbers() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprDoubleValue(unixTime),
            new ExprStringValue("%V %U %w"));

    assertEquals("12 11 1", result);
  }

  @Test
  public void testStrftimeWithDayOfYear() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%j"));

    assertEquals("078", result);
  }

  @Test
  public void testStrftimeWithCentury() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%C"));

    assertEquals("20", result);
  }

  @Test
  public void testStrftimeWithYearFormats() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprDoubleValue(unixTime),
            new ExprStringValue("%Y %y %G %g"));

    assertEquals("2018 18 2018 18", result);
  }

  @Test
  public void testStrftimeWithSpacePaddedFormats() {
    long unixTime = 1517472303L; // 2018-02-01 08:05:03

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%e %k"));

    assertEquals(" 1  8", result);
  }

  @Test
  public void testStrftimeWithPercentLiteral() {
    long unixTime = 1521467703L;

    // Test literal percent: %% should become %
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%%"));

    assertEquals("%", result);

    // %%Y should become %Y (not expanding Y)
    result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%%Y"));

    assertEquals("%Y", result);
  }

  @Test
  public void testStrftimeWithStringTimestamp() {
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprStringValue("1521467703"),
            new ExprStringValue("%Y-%m-%d"));

    assertEquals("2018-03-19", result);
  }

  @Test
  public void testStrftimeWithLongValue() {
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprLongValue(1521467703L),
            new ExprStringValue("%Y-%m-%d"));

    assertEquals("2018-03-19", result);
  }

  @Test
  public void testStrftimeWithTimestampValue() {
    // Create timestamp using Instant for proper epoch second conversion
    Instant instant = Instant.ofEpochSecond(1521467703L); // 2018-03-19T13:55:03 UTC
    ExprTimestampValue timestamp = new ExprTimestampValue(instant);

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, timestamp, new ExprStringValue("%Y-%m-%d %H:%M:%S"));

    assertEquals("2018-03-19 13:55:03", result);
  }

  @Test
  public void testStrftimeWithNullTimestamp() {
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, ExprNullValue.of(), new ExprStringValue("%Y-%m-%d"));

    assertNull(result);
  }

  @Test
  public void testStrftimeWithNullFormat() {
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(1521467703L), ExprNullValue.of());

    assertNull(result);
  }

  @Test
  public void testStrftimeWithNegativeTimestamp() {
    // Negative timestamps should return null (out of valid range)
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(-1), new ExprStringValue("%Y-%m-%d"));

    assertNull(result);
  }

  @Test
  public void testStrftimeWithOutOfRangeTimestamp() {
    // 32536771200 is above the max valid range (32536771199)
    // This should return null because it's out of the valid FROM_UNIXTIME range
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprDoubleValue(32536771200.0), // Above max range by 1 second
            new ExprStringValue("%Y-%m-%d"));

    assertNull(result);
  }

  @Test
  public void testStrftimeWithInvalidStringTimestamp() {
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None,
            new ExprStringValue("invalid"),
            new ExprStringValue("%Y-%m-%d"));

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
            FunctionProperties.None,
            new ExprDoubleValue(unixTime),
            new ExprStringValue("Date: %F, Time: %T, Weekday: %A"));

    assertEquals("Date: 2018-03-19, Time: 13:55:03, Weekday: Monday", result);
  }

  @Test
  public void testStrftimeWithLocaleFormats() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%c"));

    assertEquals("Mon Mar 19 13:55:03 2018", result);
  }

  @Test
  public void testStrftimeWithDateLocaleFormat() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%x"));

    assertEquals("03/19/2018", result);
  }

  @Test
  public void testStrftimeWithTimeLocaleFormat() {
    long unixTime = 1521467703L;

    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue("%X"));

    assertEquals("13:55:03", result);
  }

  @Test
  public void testStrftimeFormatterDirectly() {
    // Test the formatter utility directly
    Instant instant = Instant.ofEpochSecond(1521467703L);
    ZonedDateTime dateTime = ZonedDateTime.ofInstant(instant, ZoneId.of("UTC"));

    ExprValue result = StrftimeFormatterUtil.formatZonedDateTime(dateTime, "%Y-%m-%d");
    assertEquals("2018-03-19", result.stringValue());
  }
}
