/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.*;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_DATETIME_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_DATE_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_INCOMPLETE_DATE_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_NUMERIC_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_TIME_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.isDateTypeCompatible;

import com.google.common.collect.Lists;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.common.time.FormatNames;
import org.opensearch.sql.data.type.ExprCoreType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchDateTypeTest {
  private static final String defaultFormatString = "";

  private static final String dateFormatString = "date";

  private static final String timeFormatString = "hourMinuteSecond";

  private static final String timestampFormatString = "basic_date_time";

  private static final OpenSearchDateType defaultDateType =
      OpenSearchDateType.of(defaultFormatString);
  private static final OpenSearchDateType dateDateType = OpenSearchDateType.of(dateFormatString);
  private static final OpenSearchDateType timeDateType = OpenSearchDateType.of(timeFormatString);

  @Test
  public void isCompatible() {
    assertAll(
        // timestamp types is compatible with all date-types
        () -> assertTrue(TIMESTAMP.isCompatible(defaultDateType)),
        () -> assertTrue(TIMESTAMP.isCompatible(dateDateType)),
        () -> assertTrue(TIMESTAMP.isCompatible(timeDateType)),

        // time type
        () -> assertFalse(TIME.isCompatible(defaultDateType)),
        () -> assertFalse(TIME.isCompatible(dateDateType)),
        () -> assertTrue(TIME.isCompatible(timeDateType)),

        // date type
        () -> assertFalse(DATE.isCompatible(defaultDateType)),
        () -> assertTrue(DATE.isCompatible(dateDateType)),
        () -> assertFalse(DATE.isCompatible(timeDateType)));
  }

  // `typeName` and `legacyTypeName` return the same thing for date objects:
  // https://github.com/opensearch-project/sql/issues/1296
  @Test
  public void check_typeName() {
    assertAll(
        // always use the MappingType of "DATE"
        () -> assertEquals("TIMESTAMP", defaultDateType.typeName()),
        () -> assertEquals("TIME", timeDateType.typeName()),
        () -> assertEquals("DATE", dateDateType.typeName()));
  }

  @Test
  public void check_legacyTypeName() {
    assertAll(
        // always use the legacy "DATE" type
        () -> assertEquals("TIMESTAMP", defaultDateType.legacyTypeName()),
        () -> assertEquals("TIME", timeDateType.legacyTypeName()),
        () -> assertEquals("DATE", dateDateType.legacyTypeName()));
  }

  @Test
  public void check_exprTypeName() {
    assertAll(
        // exprType changes based on type (no datetime):
        () -> assertEquals(TIMESTAMP, defaultDateType.getExprCoreType()),
        () -> assertEquals(TIME, timeDateType.getExprCoreType()),
        () -> assertEquals(DATE, dateDateType.getExprCoreType()));
  }

  private static Stream<Arguments> getAllSupportedFormats() {
    return EnumSet.allOf(FormatNames.class).stream().map(Arguments::of);
  }

  // Added RFC3339_LENIENT as a temporary fix.
  // Issue: https://github.com/opensearch-project/sql/issues/2478
  @ParameterizedTest
  @MethodSource("getAllSupportedFormats")
  public void check_supported_format_names_coverage(FormatNames formatName) {
    assertTrue(
        SUPPORTED_NAMED_NUMERIC_FORMATS.contains(formatName)
            || SUPPORTED_NAMED_DATETIME_FORMATS.contains(formatName)
            || SUPPORTED_NAMED_DATE_FORMATS.contains(formatName)
            || SUPPORTED_NAMED_TIME_FORMATS.contains(formatName)
            || SUPPORTED_NAMED_INCOMPLETE_DATE_FORMATS.contains(formatName)
            || formatName.equals(FormatNames.RFC3339_LENIENT),
        formatName + " not supported");
  }

  private static Stream<Arguments> getSupportedDatetimeFormats() {
    return SUPPORTED_NAMED_DATETIME_FORMATS.stream().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("getSupportedDatetimeFormats")
  public void check_datetime_format_names(FormatNames datetimeFormat) {
    String camelCaseName = datetimeFormat.getCamelCaseName();
    if (camelCaseName != null && !camelCaseName.isEmpty()) {
      OpenSearchDateType dateType = OpenSearchDateType.of(camelCaseName);
      assertSame(
          dateType.getExprCoreType(),
          TIMESTAMP,
          camelCaseName
              + " does not format to a TIMESTAMP type, instead got "
              + dateType.getExprCoreType());
    }

    String snakeCaseName = datetimeFormat.getSnakeCaseName();
    if (snakeCaseName != null && !snakeCaseName.isEmpty()) {
      OpenSearchDateType dateType = OpenSearchDateType.of(snakeCaseName);
      assertSame(
          dateType.getExprCoreType(),
          TIMESTAMP,
          snakeCaseName
              + " does not format to a TIMESTAMP type, instead got "
              + dateType.getExprCoreType());
    } else {
      fail();
    }
  }

  private static Stream<Arguments> getSupportedDateFormats() {
    return SUPPORTED_NAMED_DATE_FORMATS.stream().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("getSupportedDateFormats")
  public void check_date_format_names(FormatNames dateFormat) {
    String camelCaseName = dateFormat.getCamelCaseName();
    if (camelCaseName != null && !camelCaseName.isEmpty()) {
      OpenSearchDateType dateType = OpenSearchDateType.of(camelCaseName);
      assertSame(
          dateType.getExprCoreType(),
          DATE,
          camelCaseName
              + " does not format to a DATE type, instead got "
              + dateType.getExprCoreType());
    }

    String snakeCaseName = dateFormat.getSnakeCaseName();
    if (snakeCaseName != null && !snakeCaseName.isEmpty()) {
      OpenSearchDateType dateType = OpenSearchDateType.of(snakeCaseName);
      assertSame(
          dateType.getExprCoreType(),
          DATE,
          snakeCaseName
              + " does not format to a DATE type, instead got "
              + dateType.getExprCoreType());
    } else {
      fail();
    }
  }

  private static Stream<Arguments> getSupportedTimeFormats() {
    return SUPPORTED_NAMED_TIME_FORMATS.stream().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("getSupportedTimeFormats")
  public void check_time_format_names(FormatNames timeFormat) {
    String camelCaseName = timeFormat.getCamelCaseName();
    if (camelCaseName != null && !camelCaseName.isEmpty()) {
      OpenSearchDateType dateType = OpenSearchDateType.of(camelCaseName);
      assertSame(
          dateType.getExprCoreType(),
          TIME,
          camelCaseName
              + " does not format to a TIME type, instead got "
              + dateType.getExprCoreType());
    }

    String snakeCaseName = timeFormat.getSnakeCaseName();
    if (snakeCaseName != null && !snakeCaseName.isEmpty()) {
      OpenSearchDateType dateType = OpenSearchDateType.of(snakeCaseName);
      assertSame(
          dateType.getExprCoreType(),
          TIME,
          snakeCaseName
              + " does not format to a TIME type, instead got "
              + dateType.getExprCoreType());
    } else {
      fail();
    }
  }

  private static Stream<Arguments> get_format_combinations_for_test() {
    return Stream.of(
        Arguments.of(DATE, List.of("dd.MM.yyyy", "date"), "d && custom date"),
        Arguments.of(TIME, List.of("time", "HH:mm"), "t && custom time"),
        Arguments.of(TIMESTAMP, List.of("dd.MM.yyyy", "time"), "t && custom date"),
        Arguments.of(TIMESTAMP, List.of("date", "HH:mm"), "d && custom time"),
        Arguments.of(TIMESTAMP, List.of("dd.MM.yyyy HH:mm", "date_time"), "dt && custom datetime"),
        Arguments.of(TIMESTAMP, List.of("dd.MM.yyyy", "date_time"), "dt && custom date"),
        Arguments.of(TIMESTAMP, List.of("HH:mm", "date_time"), "dt && custom time"),
        Arguments.of(TIMESTAMP, List.of("dd.MM.yyyy", "epoch_second"), "custom date && num"),
        Arguments.of(TIMESTAMP, List.of("HH:mm", "epoch_second"), "custom time && num"),
        Arguments.of(TIMESTAMP, List.of("date_time", "epoch_second"), "dt && num"),
        Arguments.of(TIMESTAMP, List.of("date", "epoch_second"), "d && num"),
        Arguments.of(TIMESTAMP, List.of("time", "epoch_second"), "t && num"),
        Arguments.of(TIMESTAMP, List.of(""), "no formats given"),
        Arguments.of(TIMESTAMP, List.of("time", "date"), "t && d"),
        Arguments.of(TIMESTAMP, List.of("epoch_second"), "numeric"),
        Arguments.of(TIME, List.of("time"), "t"),
        Arguments.of(DATE, List.of("date"), "d"),
        Arguments.of(TIMESTAMP, List.of("date_time"), "dt"),
        Arguments.of(TIMESTAMP, List.of("unknown"), "unknown/incorrect"),
        Arguments.of(DATE, List.of("uuuu"), "incomplete date"),
        Arguments.of(TIME, List.of("HH"), "incomplete time"),
        Arguments.of(DATE, List.of("E-w"), "incomplete"),
        // E - day of week, w - week of year
        Arguments.of(DATE, List.of("uuuu", "E-w"), "incomplete with year"),
        Arguments.of(TIMESTAMP, List.of("---"), "incorrect"),
        Arguments.of(TIMESTAMP, List.of("dd.MM.yyyy", "HH:mm"), "custom date and time"),
        // D - day of year, N - nano of day
        Arguments.of(TIMESTAMP, List.of("dd.MM.yyyy N", "uuuu:D:HH:mm"), "custom datetime"),
        Arguments.of(DATE, List.of("dd.MM.yyyy", "uuuu:D"), "custom date"),
        Arguments.of(TIME, List.of("HH:mm", "N"), "custom time"));
  }

  @ParameterizedTest(name = "[{index}] {2}")
  @MethodSource("get_format_combinations_for_test")
  public void check_ExprCoreType_of_combinations_of_custom_and_predefined_formats(
      ExprCoreType expected, List<String> formats, String testName) {
    assertEquals(expected, OpenSearchDateType.of(String.join(" || ", formats)).getExprCoreType());
    formats = Lists.reverse(formats);
    assertEquals(expected, OpenSearchDateType.of(String.join(" || ", formats)).getExprCoreType());
  }

  @Test
  public void dont_use_incorrect_format_as_custom() {
    assertEquals(0, OpenSearchDateType.of(" ").getAllCustomFormatters().size());
  }

  @Test
  public void check_if_date_type_compatible() {
    assertTrue(isDateTypeCompatible(DATE));
    assertFalse(isDateTypeCompatible(OpenSearchDataType.of(OpenSearchDataType.MappingType.Text)));
  }

  @Test
  void test_valid_timestamp_with_custom_format() {
    String timestamp = "2021-11-08T17:00:00Z";
    String format = "strict_date_time_no_millis";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    ZonedDateTime zonedDateTime = dateType.getParsedDateTime(timestamp);

    assertEquals("2021-11-08T17:00:00Z", dateType.getFormattedDate(zonedDateTime.toInstant()));
    assertEquals(LocalDate.parse("2021-11-08"), zonedDateTime.toLocalDate());
    assertFalse(dateType.hasNoFormatter());
  }

  @Test
  void test_valid_timestamp_with_multiple_formats() {
    String timestamp = "2021-11-08T17:00:00Z";
    String timestamp2 = "2021/11/08T17:00:00Z";

    List<String> formats = Arrays.asList("strict_date_time_no_millis", "yyyy/MM/dd'T'HH:mm:ssX");
    OpenSearchDateType dateType = OpenSearchDateType.of(String.join(" || ", formats));

    // Testing with the first timestamp
    ZonedDateTime zonedDateTime1 = dateType.getParsedDateTime(timestamp);

    assertEquals("2021-11-08T17:00:00Z", dateType.getFormattedDate(zonedDateTime1.toInstant()));
    assertEquals(LocalDate.parse("2021-11-08"), zonedDateTime1.toLocalDate());
    assertFalse(dateType.hasNoFormatter());

    // Testing with the second timestamp
    ZonedDateTime zonedDateTime2 = dateType.getParsedDateTime(timestamp2);

    assertEquals("2021-11-08T17:00:00Z", dateType.getFormattedDate(zonedDateTime2.toInstant()));
    assertEquals(LocalDate.parse("2021-11-08"), zonedDateTime2.toLocalDate());
    assertFalse(dateType.hasNoFormatter());
  }

  @Test
  void test_openSearch_datetime_named_formatter() {
    String timestamp = "2019-03-23T21:34:46";
    String format = "strict_date_hour_minute_second";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    ZonedDateTime zonedDateTime = dateType.getParsedDateTime(timestamp);

    assertEquals("2019-03-23T21:34:46", dateType.getFormattedDate(zonedDateTime.toInstant()));
    assertEquals(LocalDate.parse("2019-03-23"), zonedDateTime.toLocalDate());
    assertEquals(LocalTime.parse("21:34:46"), zonedDateTime.toLocalTime());
    assertFalse(dateType.hasNoFormatter());
  }

  @Test
  void test_openSearch_datetime_with_default_formatter() {
    String timestamp = "2019-03-23T21:34:46";
    OpenSearchDateType dateType = OpenSearchDateType.of(TIMESTAMP);
    ZonedDateTime zonedDateTime = dateType.getParsedDateTime(timestamp);
    // formatted using OpenSearch default formatter
    assertEquals("2019-03-23T21:34:46Z", dateType.getFormattedDate(zonedDateTime.toInstant()));
    assertEquals(LocalDate.parse("2019-03-23"), zonedDateTime.toLocalDate());
    assertEquals(LocalTime.parse("21:34:46"), zonedDateTime.toLocalTime());
    assertTrue(dateType.hasNoFormatter());
  }

  @Test
  void test_invalid_date_with_named_formatter() {
    // Incorrect date
    String timestamp = "2019-23-23";
    String format = "strict_date_hour_minute_second";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    ZonedDateTime zonedDateTime = dateType.getParsedDateTime(timestamp);
    assertNull(zonedDateTime);
    assertFalse(dateType.hasNoFormatter());
  }

  @Test
  void test_invalid_time_with_custom_formatter() {
    String timestamp = "invalid-timestamp";
    List<String> formats = Arrays.asList("yyyy/MM/dd'T'HH:mm:ssX", "yyyy-MM-dd'T'HH:mm:ssX");
    OpenSearchDateType dateType = OpenSearchDateType.of(String.join(" || ", formats));
    ZonedDateTime zonedDateTime = dateType.getParsedDateTime(timestamp);
    assertNull(zonedDateTime);
    assertFalse(dateType.hasNoFormatter());
  }

  @Test
  void test_epoch_datetime_formatter() {
    long epochTimestamp = 1636390800000L; // Corresponds to "2021-11-08T17:00:00Z"
    String format = "epoch_millis";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    ZonedDateTime zonedDateTime = dateType.getParsedDateTime(String.valueOf(epochTimestamp));

    assertEquals(Long.toString(epochTimestamp), dateType.getFormattedDate(zonedDateTime));
    assertEquals(LocalDate.parse("2021-11-08"), zonedDateTime.toLocalDate());
    assertEquals(LocalTime.parse("17:00:00"), zonedDateTime.toLocalTime());
    assertFalse(dateType.hasNoFormatter());
  }

  @Test
  void test_timeStamp_format_with_default_formatters() {
    String timestamp = "2021-11-08 17:00:00";
    String format = "strict_date_time_no_millis || epoch_millis";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);
    assertNull(dateType.getParsedDateTime(timestamp));
  }

  @Test
  void test_valid_date_with_custom_formatter() {
    String dateString = "2021-11-08";
    String format = "yyyy-MM-dd";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);

    LocalDate expectedDate = LocalDate.parse(dateString, DateTimeFormatter.ISO_DATE);
    LocalDate parsedDate = dateType.getParsedDateTime(dateString).toLocalDate();

    assertEquals(expectedDate, parsedDate);
    assertEquals("2021-11-08", dateType.getFormattedDate(parsedDate));
  }

  @Test
  void test_valid_date_string_with_custom_formatter() {
    String dateString = "03-Jan-21";
    String format = "dd-MMM-yy";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);

    LocalDate parsedDate = dateType.getParsedDateTime(dateString).toLocalDate();

    assertEquals(LocalDate.parse("2021-01-03"), parsedDate);
    assertEquals("03-Jan-21", dateType.getFormattedDate(parsedDate));
    assertFalse(dateType.hasNoFormatter());
  }

  @Test
  void test_valid_date_with_multiple_formatters() {
    String dateString = "2021-11-08";
    String format = "yyyy/MM/dd || yyyy-MM-dd";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);

    LocalDate expectedDate = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    LocalDate parsedDate = dateType.getParsedDateTime(dateString).toLocalDate();

    assertEquals(expectedDate, parsedDate);
    assertEquals("2021/11/08", dateType.getFormattedDate(parsedDate));
  }

  @Test
  void test_valid_time_with_custom_formatter() {
    String timeString = "12:10:30.000";
    String format = "HH:mm:ss.SSS";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);

    LocalTime expectedTime = LocalTime.parse(timeString, DateTimeFormatter.ofPattern(format));
    LocalTime parsedTime = dateType.getParsedDateTime(timeString).toLocalTime();

    assertEquals(expectedTime, parsedTime);
    assertEquals("12:10:30.000", dateType.getFormattedDate(parsedTime));
  }

  @Test
  void test_valid_time_with_multiple_formatters() {
    String timeString = "12:10:30";
    String format = "HH:mm:ss.SSS || HH:mm:ss";
    OpenSearchDateType dateType = OpenSearchDateType.of(format);

    LocalTime expectedTime = LocalTime.parse(timeString, DateTimeFormatter.ofPattern("HH:mm:ss"));
    LocalTime parsedTime = dateType.getParsedDateTime(timeString).toLocalTime();

    assertEquals(expectedTime, parsedTime);
    assertEquals("12:10:30.000", dateType.getFormattedDate(parsedTime));
  }
}
