/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Locale;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.udf.datetime.StrftimeFunction;

/**
 * Locale-specific tests for STRFTIME function. Tests that the function correctly uses the server's
 * default locale for formatting day and month names.
 */
public class StrftimeFunctionLocaleTest {

  private Locale originalLocale;

  @BeforeEach
  public void saveOriginalLocale() {
    originalLocale = Locale.getDefault();
  }

  @AfterEach
  public void restoreOriginalLocale() {
    Locale.setDefault(originalLocale);
  }

  private static Stream<Arguments> localeTestCases() {
    // Unix timestamp for 2024-03-15 (Friday, March)
    long unixTime = 1710504000L;

    return Stream.of(
        // English (US)
        Arguments.of(Locale.US, unixTime, "%A", "Friday"),
        Arguments.of(Locale.US, unixTime, "%a", "Fri"),
        Arguments.of(Locale.US, unixTime, "%B", "March"),
        Arguments.of(Locale.US, unixTime, "%b", "Mar"),
        Arguments.of(Locale.US, unixTime, "%p", "PM"),
        Arguments.of(Locale.US, unixTime, "%c", "Fri Mar 15 12:00:00 2024"),

        // Chinese (Simplified)
        Arguments.of(Locale.SIMPLIFIED_CHINESE, unixTime, "%A", "星期五"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, unixTime, "%a", "周五"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, unixTime, "%B", "三月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, unixTime, "%b", "3月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, unixTime, "%p", "下午"),

        // Chinese (Traditional)
        Arguments.of(Locale.TRADITIONAL_CHINESE, unixTime, "%A", "星期五"),
        Arguments.of(Locale.TRADITIONAL_CHINESE, unixTime, "%a", "週五"),
        Arguments.of(Locale.TRADITIONAL_CHINESE, unixTime, "%B", "3月"),
        Arguments.of(Locale.TRADITIONAL_CHINESE, unixTime, "%b", "3月"),

        // French
        Arguments.of(Locale.FRENCH, unixTime, "%A", "vendredi"),
        Arguments.of(Locale.FRENCH, unixTime, "%a", "ven."),
        Arguments.of(Locale.FRENCH, unixTime, "%B", "mars"),
        Arguments.of(Locale.FRENCH, unixTime, "%b", "mars"),
        Arguments.of(Locale.FRENCH, unixTime, "%c", "ven. mars 15 12:00:00 2024"),

        // German
        Arguments.of(Locale.GERMAN, unixTime, "%A", "Freitag"),
        Arguments.of(Locale.GERMAN, unixTime, "%a", "Fr."),
        Arguments.of(Locale.GERMAN, unixTime, "%B", "März"),
        Arguments.of(Locale.GERMAN, unixTime, "%b", "März"),

        // Japanese
        Arguments.of(Locale.JAPANESE, unixTime, "%A", "金曜日"),
        Arguments.of(Locale.JAPANESE, unixTime, "%a", "金"),
        Arguments.of(Locale.JAPANESE, unixTime, "%B", "3月"),
        Arguments.of(Locale.JAPANESE, unixTime, "%b", "3月"),
        Arguments.of(Locale.JAPANESE, unixTime, "%p", "午後"),

        // Spanish
        Arguments.of(Locale.forLanguageTag("es"), unixTime, "%A", "viernes"),
        Arguments.of(Locale.forLanguageTag("es"), unixTime, "%a", "vie"),
        Arguments.of(Locale.forLanguageTag("es"), unixTime, "%B", "marzo"),
        Arguments.of(Locale.forLanguageTag("es"), unixTime, "%b", "mar"),

        // Korean
        Arguments.of(Locale.KOREAN, unixTime, "%A", "금요일"),
        Arguments.of(Locale.KOREAN, unixTime, "%a", "금"),
        Arguments.of(Locale.KOREAN, unixTime, "%B", "3월"),
        Arguments.of(Locale.KOREAN, unixTime, "%b", "3월"),
        Arguments.of(Locale.KOREAN, unixTime, "%p", "오후"),

        // Italian
        Arguments.of(Locale.ITALIAN, unixTime, "%A", "venerdì"),
        Arguments.of(Locale.ITALIAN, unixTime, "%a", "ven"),
        Arguments.of(Locale.ITALIAN, unixTime, "%B", "marzo"),
        Arguments.of(Locale.ITALIAN, unixTime, "%b", "mar"),

        // Portuguese (Brazil)
        Arguments.of(Locale.forLanguageTag("pt-BR"), unixTime, "%A", "sexta-feira"),
        Arguments.of(Locale.forLanguageTag("pt-BR"), unixTime, "%a", "sex."),
        Arguments.of(Locale.forLanguageTag("pt-BR"), unixTime, "%B", "março"),
        Arguments.of(Locale.forLanguageTag("pt-BR"), unixTime, "%b", "mar."));
  }

  @ParameterizedTest
  @MethodSource("localeTestCases")
  public void testStrftimeWithDifferentLocales(
      Locale locale, long unixTime, String format, String expected) {
    // Set the locale for this test
    Locale.setDefault(locale);

    // Execute strftime with the given format
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue(format));

    assertEquals(
        expected, result, String.format("Failed for locale %s with format %s", locale, format));
  }

  private static Stream<Arguments> monthNumberTestCases() {
    return Stream.of(
        // Test all 12 months in different locales
        Arguments.of(Locale.US, 1704067200L, "%B", "January"), // 2024-01-01
        Arguments.of(Locale.US, 1706745600L, "%B", "February"), // 2024-02-01
        Arguments.of(Locale.US, 1709251200L, "%B", "March"), // 2024-03-01
        Arguments.of(Locale.US, 1711929600L, "%B", "April"), // 2024-04-01
        Arguments.of(Locale.US, 1714521600L, "%B", "May"), // 2024-05-01
        Arguments.of(Locale.US, 1717200000L, "%B", "June"), // 2024-06-01
        Arguments.of(Locale.US, 1719792000L, "%B", "July"), // 2024-07-01
        Arguments.of(Locale.US, 1722470400L, "%B", "August"), // 2024-08-01
        Arguments.of(Locale.US, 1725148800L, "%B", "September"), // 2024-09-01
        Arguments.of(Locale.US, 1727740800L, "%B", "October"), // 2024-10-01
        Arguments.of(Locale.US, 1730419200L, "%B", "November"), // 2024-11-01
        Arguments.of(Locale.US, 1733011200L, "%B", "December"), // 2024-12-01

        // Chinese months
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1704067200L, "%B", "一月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1706745600L, "%B", "二月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1709251200L, "%B", "三月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1711929600L, "%B", "四月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1714521600L, "%B", "五月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1717200000L, "%B", "六月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1719792000L, "%B", "七月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1722470400L, "%B", "八月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1725148800L, "%B", "九月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1727740800L, "%B", "十月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1730419200L, "%B", "十一月"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1733011200L, "%B", "十二月"));
  }

  @ParameterizedTest
  @MethodSource("monthNumberTestCases")
  public void testStrftimeMonthsInDifferentLocales(
      Locale locale, long unixTime, String format, String expected) {
    // Set the locale for this test
    Locale.setDefault(locale);

    // Execute strftime with the given format
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue(format));

    assertEquals(
        expected,
        result,
        String.format("Failed for locale %s with timestamp %d", locale, unixTime));
  }

  private static Stream<Arguments> weekdayTestCases() {
    return Stream.of(
        // Test all 7 days of the week in different locales
        // Using timestamps that fall on specific weekdays
        Arguments.of(Locale.US, 1710633600L, "%A", "Sunday"), // 2024-03-17
        Arguments.of(Locale.US, 1710720000L, "%A", "Monday"), // 2024-03-18
        Arguments.of(Locale.US, 1710806400L, "%A", "Tuesday"), // 2024-03-19
        Arguments.of(Locale.US, 1710892800L, "%A", "Wednesday"), // 2024-03-20
        Arguments.of(Locale.US, 1710979200L, "%A", "Thursday"), // 2024-03-21
        Arguments.of(Locale.US, 1710504000L, "%A", "Friday"), // 2024-03-15
        Arguments.of(Locale.US, 1710547200L, "%A", "Saturday"), // 2024-03-16

        // Chinese weekdays
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1710633600L, "%A", "星期日"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1710720000L, "%A", "星期一"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1710806400L, "%A", "星期二"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1710892800L, "%A", "星期三"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1710979200L, "%A", "星期四"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1710504000L, "%A", "星期五"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, 1710547200L, "%A", "星期六"),

        // Japanese weekdays
        Arguments.of(Locale.JAPANESE, 1710633600L, "%A", "日曜日"),
        Arguments.of(Locale.JAPANESE, 1710720000L, "%A", "月曜日"),
        Arguments.of(Locale.JAPANESE, 1710806400L, "%A", "火曜日"),
        Arguments.of(Locale.JAPANESE, 1710892800L, "%A", "水曜日"),
        Arguments.of(Locale.JAPANESE, 1710979200L, "%A", "木曜日"),
        Arguments.of(Locale.JAPANESE, 1710504000L, "%A", "金曜日"),
        Arguments.of(Locale.JAPANESE, 1710547200L, "%A", "土曜日"));
  }

  @ParameterizedTest
  @MethodSource("weekdayTestCases")
  public void testStrftimeWeekdaysInDifferentLocales(
      Locale locale, long unixTime, String format, String expected) {
    // Set the locale for this test
    Locale.setDefault(locale);

    // Execute strftime with the given format
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue(format));

    assertEquals(
        expected,
        result,
        String.format("Failed for locale %s with timestamp %d", locale, unixTime));
  }

  private static Stream<Arguments> numericFormatTestCases() {
    // Numeric formats should be consistent across locales
    long unixTime = 1710504000L; // 2024-03-15 12:00:00 UTC

    return Stream.of(
        // These should be the same in all locales
        Arguments.of(Locale.US, unixTime, "%Y-%m-%d", "2024-03-15"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, unixTime, "%Y-%m-%d", "2024-03-15"),
        Arguments.of(Locale.FRENCH, unixTime, "%Y-%m-%d", "2024-03-15"),
        Arguments.of(Locale.JAPANESE, unixTime, "%Y-%m-%d", "2024-03-15"),
        Arguments.of(Locale.US, unixTime, "%H:%M:%S", "12:00:00"),
        Arguments.of(Locale.SIMPLIFIED_CHINESE, unixTime, "%H:%M:%S", "12:00:00"),
        Arguments.of(Locale.FRENCH, unixTime, "%H:%M:%S", "12:00:00"),
        Arguments.of(Locale.JAPANESE, unixTime, "%H:%M:%S", "12:00:00"));
  }

  @ParameterizedTest
  @MethodSource("numericFormatTestCases")
  public void testStrftimeNumericFormatsAreLocaleIndependent(
      Locale locale, long unixTime, String format, String expected) {
    // Set the locale for this test
    Locale.setDefault(locale);

    // Execute strftime with the given format
    String result =
        StrftimeFunction.strftime(
            FunctionProperties.None, new ExprDoubleValue(unixTime), new ExprStringValue(format));

    assertEquals(
        expected,
        result,
        String.format("Numeric format should be consistent across locales, failed for %s", locale));
  }
}
