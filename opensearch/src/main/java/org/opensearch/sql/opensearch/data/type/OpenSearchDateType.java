/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.data.type;

import static org.opensearch.common.time.DateFormatter.splitCombinedPatterns;
import static org.opensearch.common.time.DateFormatter.strip8Prefix;
import static org.opensearch.sql.data.type.ExprCoreType.DATE;
import static org.opensearch.sql.data.type.ExprCoreType.TIME;
import static org.opensearch.sql.data.type.ExprCoreType.TIMESTAMP;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.FormatNames;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

/**
 * Date type with support for predefined and custom formats read from the index mapping.
 */
public class OpenSearchDateType extends OpenSearchDataType {

  private static final OpenSearchDateType instance = new OpenSearchDateType();

  /** Numeric formats which support full datetime. */
  public static final List<FormatNames> SUPPORTED_NAMED_NUMERIC_FORMATS = List.of(
      FormatNames.EPOCH_MILLIS,
      FormatNames.EPOCH_SECOND
  );

  /** List of named formats which support full datetime. */
  public static final List<FormatNames> SUPPORTED_NAMED_DATETIME_FORMATS = List.of(
      FormatNames.ISO8601,
      FormatNames.BASIC_DATE_TIME,
      FormatNames.BASIC_DATE_TIME_NO_MILLIS,
      FormatNames.BASIC_ORDINAL_DATE_TIME,
      FormatNames.BASIC_ORDINAL_DATE_TIME_NO_MILLIS,
      FormatNames.BASIC_WEEK_DATE_TIME,
      FormatNames.STRICT_BASIC_WEEK_DATE_TIME,
      FormatNames.BASIC_WEEK_DATE_TIME_NO_MILLIS,
      FormatNames.STRICT_BASIC_WEEK_DATE_TIME_NO_MILLIS,
      FormatNames.BASIC_WEEK_DATE,
      FormatNames.STRICT_BASIC_WEEK_DATE,
      FormatNames.DATE_OPTIONAL_TIME,
      FormatNames.STRICT_DATE_OPTIONAL_TIME,
      FormatNames.STRICT_DATE_OPTIONAL_TIME_NANOS,
      FormatNames.DATE_TIME,
      FormatNames.STRICT_DATE_TIME,
      FormatNames.DATE_TIME_NO_MILLIS,
      FormatNames.STRICT_DATE_TIME_NO_MILLIS,
      FormatNames.DATE_HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.DATE_HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.DATE_HOUR_MINUTE_SECOND_MILLIS,
      FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND_MILLIS,
      FormatNames.DATE_HOUR_MINUTE_SECOND,
      FormatNames.STRICT_DATE_HOUR_MINUTE_SECOND,
      FormatNames.DATE_HOUR_MINUTE,
      FormatNames.STRICT_DATE_HOUR_MINUTE,
      FormatNames.DATE_HOUR,
      FormatNames.STRICT_DATE_HOUR,
      FormatNames.ORDINAL_DATE_TIME,
      FormatNames.STRICT_ORDINAL_DATE_TIME,
      FormatNames.ORDINAL_DATE_TIME_NO_MILLIS,
      FormatNames.STRICT_ORDINAL_DATE_TIME_NO_MILLIS,
      FormatNames.WEEK_DATE_TIME,
      FormatNames.STRICT_WEEK_DATE_TIME,
      FormatNames.WEEK_DATE_TIME_NO_MILLIS,
      FormatNames.STRICT_WEEK_DATE_TIME_NO_MILLIS
  );

  /** List of named formats that only support year/month/day. */
  public static final List<FormatNames> SUPPORTED_NAMED_DATE_FORMATS = List.of(
      FormatNames.BASIC_DATE,
      FormatNames.BASIC_ORDINAL_DATE,
      FormatNames.DATE,
      FormatNames.STRICT_DATE,
      FormatNames.YEAR_MONTH_DAY,
      FormatNames.STRICT_YEAR_MONTH_DAY,
      FormatNames.ORDINAL_DATE,
      FormatNames.STRICT_ORDINAL_DATE,
      FormatNames.WEEK_DATE,
      FormatNames.STRICT_WEEK_DATE,
      FormatNames.WEEKYEAR_WEEK_DAY,
      FormatNames.STRICT_WEEKYEAR_WEEK_DAY
  );

  /** list of named formats which produce incomplete date,
   * e.g. 1 or 2 are missing from tuple year/month/day. */
  public static final List<FormatNames> SUPPORTED_NAMED_INCOMPLETE_DATE_FORMATS = List.of(
      FormatNames.YEAR_MONTH,
      FormatNames.STRICT_YEAR_MONTH,
      FormatNames.YEAR,
      FormatNames.STRICT_YEAR,
      FormatNames.WEEK_YEAR,
      FormatNames.WEEK_YEAR_WEEK,
      FormatNames.STRICT_WEEKYEAR_WEEK,
      FormatNames.WEEKYEAR,
      FormatNames.STRICT_WEEKYEAR
  );

  /** List of named formats that only support hour/minute/second. */
  public static final List<FormatNames> SUPPORTED_NAMED_TIME_FORMATS = List.of(
      FormatNames.BASIC_TIME,
      FormatNames.BASIC_TIME_NO_MILLIS,
      FormatNames.BASIC_T_TIME,
      FormatNames.BASIC_T_TIME_NO_MILLIS,
      FormatNames.TIME,
      FormatNames.STRICT_TIME,
      FormatNames.TIME_NO_MILLIS,
      FormatNames.STRICT_TIME_NO_MILLIS,
      FormatNames.HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.STRICT_HOUR_MINUTE_SECOND_FRACTION,
      FormatNames.HOUR_MINUTE_SECOND_MILLIS,
      FormatNames.STRICT_HOUR_MINUTE_SECOND_MILLIS,
      FormatNames.HOUR_MINUTE_SECOND,
      FormatNames.STRICT_HOUR_MINUTE_SECOND,
      FormatNames.HOUR_MINUTE,
      FormatNames.STRICT_HOUR_MINUTE,
      FormatNames.HOUR,
      FormatNames.STRICT_HOUR,
      FormatNames.T_TIME,
      FormatNames.STRICT_T_TIME,
      FormatNames.T_TIME_NO_MILLIS,
      FormatNames.STRICT_T_TIME_NO_MILLIS
  );

  /** Formatter symbols which used to format time or date correspondingly.
   * {@link java.time.format.DateTimeFormatter}. */
  private static final String CUSTOM_FORMAT_TIME_SYMBOLS = "nNASsmHkKha";
  private static final String CUSTOM_FORMAT_DATE_SYMBOLS = "FecEWwYqQgdMLDyuG";

  @EqualsAndHashCode.Exclude
  private final List<String> formats;

  private OpenSearchDateType() {
    super(MappingType.Date);
    this.formats = List.of();
  }

  private OpenSearchDateType(ExprCoreType exprCoreType) {
    this();
    this.exprCoreType = exprCoreType;
  }

  private OpenSearchDateType(String format) {
    super(MappingType.Date);
    this.formats = getFormatList(format);
    this.exprCoreType = getExprTypeFromFormatString(format);
  }

  public boolean hasFormats() {
    return !formats.isEmpty();
  }

  /**
   * Retrieves and splits a user defined format string from the mapping into a list of formats.
   * @return A list of format names and user defined formats.
   */
  private List<String> getFormatList(String format) {
    format = strip8Prefix(format);
    return splitCombinedPatterns(format).stream().map(String::trim).collect(Collectors.toList());
  }

  /**
   * Retrieves a list of named OpenSearch formatters given by user mapping.
   * @return a list of DateFormatters that can be used to parse a Date/Time/Timestamp.
   */
  public List<DateFormatter> getAllNamedFormatters() {
    return formats.stream()
        .filter(formatString -> FormatNames.forName(formatString) != null)
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  /**
   * Retrieves a list of numeric formatters that format for dates.
   * @return a list of DateFormatters that can be used to parse a Date.
   */
  public List<DateFormatter> getNumericNamedFormatters() {
    return formats.stream()
        .filter(formatString -> {
          FormatNames namedFormat = FormatNames.forName(formatString);
          return namedFormat != null && SUPPORTED_NAMED_NUMERIC_FORMATS.contains(namedFormat);
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  /**
   * Retrieves a list of custom formats defined by the user.
   * @return a list of formats as strings that can be used to parse a Date/Time/Timestamp.
   */
  public List<String> getAllCustomFormats() {
    return formats.stream()
        .filter(format -> FormatNames.forName(format) == null)
        .map(format -> {
          try {
            DateFormatter.forPattern(format);
            return format;
          } catch (Exception ignored) {
            // parsing failed
            return null;
          }
        })
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * Retrieves a list of custom formatters defined by the user.
   * @return a list of DateFormatters that can be used to parse a Date/Time/Timestamp.
   */
  public List<DateFormatter> getAllCustomFormatters() {
    return getAllCustomFormats().stream()
        .map(DateFormatter::forPattern)
        .collect(Collectors.toList());
  }

  /**
   * Retrieves a list of named formatters that format for dates.
   * @return a list of DateFormatters that can be used to parse a Date.
   */
  public List<DateFormatter> getDateNamedFormatters() {
    return formats.stream()
        .filter(formatString -> {
          FormatNames namedFormat = FormatNames.forName(formatString);
          return namedFormat != null && SUPPORTED_NAMED_DATE_FORMATS.contains(namedFormat);
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  /**
   * Retrieves a list of named formatters that format for Times.
   * @return a list of DateFormatters that can be used to parse a Time.
   */
  public List<DateFormatter> getTimeNamedFormatters() {
    return formats.stream()
        .filter(formatString -> {
          FormatNames namedFormat = FormatNames.forName(formatString);
          return namedFormat != null && SUPPORTED_NAMED_TIME_FORMATS.contains(namedFormat);
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  /**
   * Retrieves a list of named formatters that format for DateTimes.
   * @return a list of DateFormatters that can be used to parse a DateTime.
   */
  public List<DateFormatter> getDateTimeNamedFormatters() {
    return formats.stream()
        .filter(formatString -> {
          FormatNames namedFormat = FormatNames.forName(formatString);
          return namedFormat != null && SUPPORTED_NAMED_DATETIME_FORMATS.contains(namedFormat);
        })
        .map(DateFormatter::forPattern).collect(Collectors.toList());
  }

  private ExprCoreType getExprTypeFromCustomFormats(List<String> formats) {
    boolean isDate = false;
    boolean isTime = false;

    for (String format : formats) {
      if (!isTime) {
        for (char symbol : CUSTOM_FORMAT_TIME_SYMBOLS.toCharArray()) {
          if (format.contains(String.valueOf(symbol))) {
            isTime = true;
            break;
          }
        }
      }
      if (!isDate) {
        for (char symbol : CUSTOM_FORMAT_DATE_SYMBOLS.toCharArray()) {
          if (format.contains(String.valueOf(symbol))) {
            isDate = true;
            break;
          }
        }
      }
      if (isDate && isTime) {
        return TIMESTAMP;
      }
    }

    if (isDate) {
      return DATE;
    }
    if (isTime) {
      return TIME;
    }

    // Incomplete or incorrect formats: can't be converted to DATE nor TIME, for example `year`
    return TIMESTAMP;
  }

  private ExprCoreType getExprTypeFromFormatString(String formatString) {
    List<DateFormatter> datetimeFormatters = getDateTimeNamedFormatters();
    List<DateFormatter> numericFormatters = getNumericNamedFormatters();

    if (formatString.isEmpty() || !datetimeFormatters.isEmpty() || !numericFormatters.isEmpty()) {
      return TIMESTAMP;
    }

    List<DateFormatter> timeFormatters = getTimeNamedFormatters();
    List<DateFormatter> dateFormatters = getDateNamedFormatters();
    if (!timeFormatters.isEmpty() && !dateFormatters.isEmpty()) {
      return TIMESTAMP;
    }

    List<String> customFormatters = getAllCustomFormats();
    if (!customFormatters.isEmpty()) {
      ExprCoreType customFormatType = getExprTypeFromCustomFormats(customFormatters);
      ExprCoreType combinedByDefaultFormats = customFormatType;
      if (!dateFormatters.isEmpty()) {
        combinedByDefaultFormats = DATE;
      }
      if (!timeFormatters.isEmpty()) {
        combinedByDefaultFormats = TIME;
      }
      return customFormatType == combinedByDefaultFormats ? customFormatType : TIMESTAMP;
    }

    // if there is nothing in the dateformatter that accepts a year/month/day, then
    // we can assume the type is strictly a Time object
    if (!timeFormatters.isEmpty()) {
      return TIME;
    }

    // if there is nothing in the dateformatter that accepts a hour/minute/second, then
    // we can assume the type is strictly a Date object
    if (!dateFormatters.isEmpty()) {
      return DATE;
    }

    // Unknown or incorrect format provided
    return TIMESTAMP;
  }

  /**
   * Check if ExprType is compatible for creation of OpenSearchDateType object.
   *
   * @param exprType type of the field in the SQL query
   * @return a boolean if type is a date/time/timestamp type
   */
  public static boolean isDateTypeCompatible(ExprType exprType) {
    if (!(exprType instanceof ExprCoreType)) {
      return false;
    }
    switch ((ExprCoreType) exprType) {
      case TIMESTAMP:
      case DATETIME:
      case DATE:
      case TIME:
        return true;
      default:
        return false;
    }
  }

  /**
   * Create a Date type which has a LinkedHashMap defining all formats.
   * @return A new type object.
   */
  public static OpenSearchDateType of(String format) {
    return new OpenSearchDateType(format);
  }

  /** A public constructor replacement. */
  public static OpenSearchDateType of(ExprCoreType exprCoreType) {
    if (!isDateTypeCompatible(exprCoreType)) {
      throw new IllegalArgumentException(String.format("Not a date/time type: %s", exprCoreType));
    }
    return new OpenSearchDateType(exprCoreType);
  }

  /** A public constructor replacement. */
  public static OpenSearchDateType of(ExprType exprType) {
    if (!isDateTypeCompatible(exprType)) {
      throw new IllegalArgumentException(String.format("Not a date/time type: %s", exprType));
    }
    return new OpenSearchDateType((ExprCoreType) exprType);
  }

  public static OpenSearchDateType of() {
    return OpenSearchDateType.instance;
  }

  @Override
  public boolean shouldCast(ExprType other) {
    // TODO override to fix for https://github.com/opensearch-project/sql/issues/1847
    return false;
  }

  @Override
  protected OpenSearchDataType cloneEmpty() {
    if (formats.isEmpty()) {
      return OpenSearchDateType.of(exprCoreType);
    }
    return OpenSearchDateType.of(String.join(" || ", formats));
  }

  @Override
  public String typeName() {
    return exprCoreType.toString();
  }

  @Override
  public String legacyTypeName() {
    return exprCoreType.toString();
  }

  @Override
  public Object convertValueForSearchQuery(ExprValue value) {
    // TODO add here fix for https://github.com/opensearch-project/sql/issues/1847
    return value.timestampValue().toEpochMilli();
  }
}
