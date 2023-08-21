/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.DATETIME;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_DATETIME_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_DATE_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_INCOMPLETE_DATE_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_NUMERIC_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.SUPPORTED_NAMED_TIME_FORMATS;
import static org.opensearch.sql.opensearch.data.type.OpenSearchDateType.isDateTypeCompatible;

import com.google.common.collect.Lists;
import java.time.Instant;
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
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class OpenSearchDateTypeTest {
  private static final String defaultFormatString = "";

  private static final String dateFormatString = "date";

  private static final String timeFormatString = "hourMinuteSecond";

  private static final String datetimeFormatString = "basic_date_time";

  private static final OpenSearchDateType defaultDateType =
      OpenSearchDateType.of(defaultFormatString);
  private static final OpenSearchDateType dateDateType =
      OpenSearchDateType.of(dateFormatString);
  private static final OpenSearchDateType timeDateType =
      OpenSearchDateType.of(timeFormatString);
  private static final OpenSearchDateType datetimeDateType =
      OpenSearchDateType.of(datetimeFormatString);

  @Test
  public void isCompatible() {
    assertAll(
        // timestamp types is compatible with all date-types
        () -> assertTrue(TIMESTAMP.isCompatible(defaultDateType)),
        () -> assertTrue(TIMESTAMP.isCompatible(dateDateType)),
        () -> assertTrue(TIMESTAMP.isCompatible(timeDateType)),
        () -> assertTrue(TIMESTAMP.isCompatible(datetimeDateType)),

        // datetime
        () -> assertFalse(DATETIME.isCompatible(defaultDateType)),
        () -> assertTrue(DATETIME.isCompatible(dateDateType)),
        () -> assertTrue(DATETIME.isCompatible(timeDateType)),
        () -> assertFalse(DATETIME.isCompatible(datetimeDateType)),

        // time type
        () -> assertFalse(TIME.isCompatible(defaultDateType)),
        () -> assertFalse(TIME.isCompatible(dateDateType)),
        () -> assertTrue(TIME.isCompatible(timeDateType)),
        () -> assertFalse(TIME.isCompatible(datetimeDateType)),

        // date type
        () -> assertFalse(DATE.isCompatible(defaultDateType)),
        () -> assertTrue(DATE.isCompatible(dateDateType)),
        () -> assertFalse(DATE.isCompatible(timeDateType)),
        () -> assertFalse(DATE.isCompatible(datetimeDateType))
    );
  }

  // `typeName` and `legacyTypeName` return the same thing for date objects:
  // https://github.com/opensearch-project/sql/issues/1296
  @Test
  public void check_typeName() {
    assertAll(
        // always use the MappingType of "DATE"
        () -> assertEquals("TIMESTAMP", defaultDateType.typeName()),
        () -> assertEquals("TIME", timeDateType.typeName()),
        () -> assertEquals("DATE", dateDateType.typeName()),
        () -> assertEquals("TIMESTAMP", datetimeDateType.typeName())
    );
  }

  @Test
  public void check_legacyTypeName() {
    assertAll(
        // always use the legacy "DATE" type
        () -> assertEquals("TIMESTAMP", defaultDateType.legacyTypeName()),
        () -> assertEquals("TIME", timeDateType.legacyTypeName()),
        () -> assertEquals("DATE", dateDateType.legacyTypeName()),
        () -> assertEquals("TIMESTAMP", datetimeDateType.legacyTypeName())
    );
  }

  @Test
  public void check_exprTypeName() {
    assertAll(
        // exprType changes based on type (no datetime):
        () -> assertEquals(TIMESTAMP, defaultDateType.getExprType()),
        () -> assertEquals(TIME, timeDateType.getExprType()),
        () -> assertEquals(DATE, dateDateType.getExprType()),
        () -> assertEquals(TIMESTAMP, datetimeDateType.getExprType())
    );
  }

  private static Stream<Arguments> getAllSupportedFormats() {
    return EnumSet.allOf(FormatNames.class).stream().map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("getAllSupportedFormats")
  public void check_supported_format_names_coverage(FormatNames formatName) {
    assertTrue(SUPPORTED_NAMED_NUMERIC_FORMATS.contains(formatName)
          || SUPPORTED_NAMED_DATETIME_FORMATS.contains(formatName)
          || SUPPORTED_NAMED_DATE_FORMATS.contains(formatName)
          || SUPPORTED_NAMED_TIME_FORMATS.contains(formatName)
          || SUPPORTED_NAMED_INCOMPLETE_DATE_FORMATS.contains(formatName),
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
      OpenSearchDateType dateType =
          OpenSearchDateType.of(camelCaseName);
      assertSame(dateType.getExprType(), TIMESTAMP, camelCaseName
          + " does not format to a TIMESTAMP type, instead got " + dateType.getExprType());
    }

    String snakeCaseName = datetimeFormat.getSnakeCaseName();
    if (snakeCaseName != null && !snakeCaseName.isEmpty()) {
      OpenSearchDateType dateType = OpenSearchDateType.of(snakeCaseName);
      assertSame(dateType.getExprType(), TIMESTAMP, snakeCaseName
          + " does not format to a TIMESTAMP type, instead got " + dateType.getExprType());
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
      assertSame(dateType.getExprType(), DATE, camelCaseName
          + " does not format to a DATE type, instead got " + dateType.getExprType());
    }

    String snakeCaseName = dateFormat.getSnakeCaseName();
    if (snakeCaseName != null && !snakeCaseName.isEmpty()) {
      OpenSearchDateType dateType = OpenSearchDateType.of(snakeCaseName);
      assertSame(dateType.getExprType(), DATE, snakeCaseName
          + " does not format to a DATE type, instead got " + dateType.getExprType());
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
      assertSame(dateType.getExprType(), TIME, camelCaseName
          + " does not format to a TIME type, instead got " + dateType.getExprType());
    }

    String snakeCaseName = timeFormat.getSnakeCaseName();
    if (snakeCaseName != null && !snakeCaseName.isEmpty()) {
      OpenSearchDateType dateType = OpenSearchDateType.of(snakeCaseName);
      assertSame(dateType.getExprType(), TIME, snakeCaseName
          + " does not format to a TIME type, instead got " + dateType.getExprType());
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
        Arguments.of(TIME, List.of("HH:mm", "N"), "custom time")
    );
  }

  @ParameterizedTest(name = "[{index}] {2}")
  @MethodSource("get_format_combinations_for_test")
  public void check_ExprCoreType_of_combinations_of_custom_and_predefined_formats(
      ExprCoreType expected, List<String> formats, String testName) {
    assertEquals(expected, OpenSearchDateType.of(String.join(" || ", formats)).getExprType());
    formats = Lists.reverse(formats);
    assertEquals(expected, OpenSearchDateType.of(String.join(" || ", formats)).getExprType());
  }

  @Test
  public void dont_use_incorrect_format_as_custom() {
    assertEquals(0, OpenSearchDateType.of(" ").getAllCustomFormatters().size());
  }

  @Test
  public void check_if_date_type_compatible() {
    assertTrue(isDateTypeCompatible(DATE));
    assertFalse(isDateTypeCompatible(OpenSearchDataType.of(
        OpenSearchDataType.MappingType.Text)));
  }

  @Test
  public void throw_if_create_with_incompatible_type() {
    assertThrows(IllegalArgumentException.class, () -> OpenSearchDateType.of(STRING));
    assertThrows(IllegalArgumentException.class,
        () -> OpenSearchDateType.of(OpenSearchTextType.of()));
  }

  @Test
  public void convert_value() {
    ExprValue value = new ExprTimestampValue(Instant.ofEpochMilli(42));
    assertEquals(42L, OpenSearchDateType.of().convertValueForSearchQuery(value));
  }

  @Test
  public void shouldCast() {
    assertFalse(OpenSearchDateType.of().shouldCast(() -> null));
  }
}
