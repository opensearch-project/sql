/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import com.google.common.collect.ImmutableMap;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.IsoFields;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;

/** Utility class for POSIX-style strftime formatting. */
public class StrftimeFormatterUtil {

  // Constants
  private static final String PERCENT_PLACEHOLDER = "\u0001PERCENT\u0001";
  private static final String PERCENT_LITERAL = "%%";
  private static final int DEFAULT_NANOSECOND_PRECISION = 9;
  private static final int DEFAULT_MILLISECOND_PRECISION = 3;
  private static final int MICROSECOND_PRECISION = 6;
  private static final long NANOS_PER_SECOND = 1_000_000_000L;
  private static final long MILLIS_PER_SECOND = 1000L;
  private static final long MAX_UNIX_TIMESTAMP = 32536771199L;
  private static final int UNIX_TIMESTAMP_DIGITS = 10;

  // Pattern to match %N and %Q with optional precision digit
  private static final Pattern SUBSECOND_PATTERN = Pattern.compile("%(\\d)?([NQ])");

  @FunctionalInterface
  private interface StrftimeFormatHandler {
    String format(ZonedDateTime dateTime);
  }

  private static final Map<String, StrftimeFormatHandler> STRFTIME_HANDLERS = buildHandlers();

  private static Map<String, StrftimeFormatHandler> buildHandlers() {
    return ImmutableMap.<String, StrftimeFormatHandler>builder()
        // Date and time combinations
        .put(
            "%c",
            dt -> dt.format(DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss yyyy", Locale.ROOT)))
        .put(
            "%+",
            dt ->
                dt.format(DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ROOT)))

        // Time formats
        .put("%Ez", StrftimeFormatterUtil::formatTimezoneOffsetMinutes)
        .put("%f", dt -> String.format("%06d", dt.getNano() / 1000))
        .put("%H", dt -> dt.format(DateTimeFormatter.ofPattern("HH", Locale.ROOT)))
        .put("%I", dt -> dt.format(DateTimeFormatter.ofPattern("hh", Locale.ROOT)))
        .put("%k", dt -> String.format("%2d", dt.getHour()))
        .put("%M", dt -> dt.format(DateTimeFormatter.ofPattern("mm", Locale.ROOT)))
        .put("%p", dt -> dt.format(DateTimeFormatter.ofPattern("a", Locale.ROOT)))
        .put("%S", dt -> dt.format(DateTimeFormatter.ofPattern("ss", Locale.ROOT)))
        .put("%s", dt -> String.valueOf(dt.toEpochSecond()))
        .put("%T", dt -> dt.format(DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT)))
        .put("%X", dt -> dt.format(DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT)))

        // Timezone formats
        .put("%Z", dt -> dt.getZone().getDisplayName(TextStyle.SHORT, Locale.ROOT))
        .put("%z", dt -> dt.format(DateTimeFormatter.ofPattern("xx", Locale.ROOT)))
        .put("%:z", dt -> dt.format(DateTimeFormatter.ofPattern("xxx", Locale.ROOT)))
        .put("%::z", dt -> dt.format(DateTimeFormatter.ofPattern("xxx:ss", Locale.ROOT)))
        .put("%:::z", dt -> dt.format(DateTimeFormatter.ofPattern("x", Locale.ROOT)))

        // Date formats
        .put("%F", dt -> dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)))
        .put("%x", dt -> dt.format(DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.ROOT)))

        // Weekday formats
        .put("%A", dt -> dt.format(DateTimeFormatter.ofPattern("EEEE", Locale.ROOT)))
        .put("%a", dt -> dt.format(DateTimeFormatter.ofPattern("EEE", Locale.ROOT)))
        .put("%w", dt -> String.valueOf(dt.getDayOfWeek().getValue() % 7))

        // Day formats
        .put("%d", dt -> dt.format(DateTimeFormatter.ofPattern("dd", Locale.ROOT)))
        .put("%e", dt -> String.format("%2d", dt.getDayOfMonth()))
        .put("%j", dt -> dt.format(DateTimeFormatter.ofPattern("DDD", Locale.ROOT)))

        // Week formats
        .put("%V", dt -> String.format("%02d", dt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)))
        .put("%U", dt -> String.format("%02d", dt.get(WeekFields.SUNDAY_START.weekOfYear()) - 1))

        // Month formats
        .put("%b", dt -> dt.format(DateTimeFormatter.ofPattern("MMM", Locale.ROOT)))
        .put("%B", dt -> dt.format(DateTimeFormatter.ofPattern("MMMM", Locale.ROOT)))
        .put("%m", dt -> dt.format(DateTimeFormatter.ofPattern("MM", Locale.ROOT)))

        // Year formats
        .put("%C", dt -> String.format("%02d", dt.getYear() / 100))
        .put("%g", dt -> String.format("%02d", dt.get(IsoFields.WEEK_BASED_YEAR) % 100))
        .put("%G", dt -> String.format("%04d", dt.get(IsoFields.WEEK_BASED_YEAR)))
        .put("%y", dt -> dt.format(DateTimeFormatter.ofPattern("yy", Locale.ROOT)))
        .put("%Y", dt -> dt.format(DateTimeFormatter.ofPattern("yyyy", Locale.ROOT)))

        // Literal percent
        .put(PERCENT_LITERAL, dt -> "%")
        .build();
  }

  private StrftimeFormatterUtil() {}

  /**
   * Format a ZonedDateTime using STRFTIME format specifiers.
   *
   * @param dateTime The ZonedDateTime to format
   * @param formatString The STRFTIME format string
   * @return Formatted string as ExprValue, or ExprNullValue if inputs are null
   */
  public static ExprValue formatZonedDateTime(ZonedDateTime dateTime, String formatString) {
    String result = processFormatString(formatString, dateTime);
    return new ExprStringValue(result);
  }

  /** Process the format string and replace all format specifiers. */
  private static String processFormatString(String formatString, ZonedDateTime dateTime) {
    // Handle %N and %Q with precision first
    String result = handleSubSecondFormats(formatString, dateTime);

    // Escape %% by replacing with placeholder
    result = result.replace(PERCENT_LITERAL, PERCENT_PLACEHOLDER);

    // Replace all other format specifiers
    result = replaceFormatSpecifiers(result, dateTime);

    // Restore literal percent signs
    return result.replace(PERCENT_PLACEHOLDER, "%");
  }

  /** Replace all format specifiers in the string. */
  private static String replaceFormatSpecifiers(String input, ZonedDateTime dateTime) {
    String result = input;
    for (Map.Entry<String, StrftimeFormatHandler> entry : STRFTIME_HANDLERS.entrySet()) {
      String specifier = entry.getKey();
      if (result.contains(specifier)) {
        String replacement = entry.getValue().format(dateTime);
        result = result.replace(specifier, replacement);
      }
    }
    return result;
  }

  /** Handle %N and %Q subsecond formats with optional precision. */
  private static String handleSubSecondFormats(String format, ZonedDateTime dateTime) {
    StringBuilder result = new StringBuilder();
    Matcher matcher = SUBSECOND_PATTERN.matcher(format);

    while (matcher.find()) {
      String precisionStr = matcher.group(1);
      String type = matcher.group(2);

      int precision = parsePrecision(precisionStr, type);
      String replacement = formatSubseconds(dateTime, type, precision);

      matcher.appendReplacement(result, replacement);
    }
    matcher.appendTail(result);

    return result.toString();
  }

  /** Parse precision value for subsecond formats. */
  private static int parsePrecision(String precisionStr, String type) {
    if (precisionStr != null) {
      return Integer.parseInt(precisionStr);
    }
    // Default: %N=9 (nanoseconds), %Q=3 (milliseconds)
    return "N".equals(type) ? DEFAULT_NANOSECOND_PRECISION : DEFAULT_MILLISECOND_PRECISION;
  }

  /** Format subseconds based on type and precision. */
  private static String formatSubseconds(ZonedDateTime dateTime, String type, int precision) {
    if ("N".equals(type)) {
      // %N - subsecond digits (nanoseconds)
      return formatNanoseconds(dateTime.getNano(), precision);
    } else {
      // %Q - subsecond component
      return formatQSubseconds(dateTime, precision);
    }
  }

  /** Format nanoseconds with specified precision. */
  private static String formatNanoseconds(long nanos, int precision) {
    double scaled = (double) nanos / NANOS_PER_SECOND;
    long truncated = (long) (scaled * Math.pow(10, precision));
    return String.format("%0" + precision + "d", truncated);
  }

  /** Format Q-type subseconds based on precision. */
  private static String formatQSubseconds(ZonedDateTime dateTime, int precision) {
    switch (precision) {
      case MICROSECOND_PRECISION:
        // Microseconds
        long micros = dateTime.getNano() / 1000;
        return String.format("%06d", micros);

      case DEFAULT_NANOSECOND_PRECISION:
        // Nanoseconds
        return String.format("%09d", dateTime.getNano());

      default:
        // Default to milliseconds
        long millis = dateTime.toInstant().toEpochMilli() % MILLIS_PER_SECOND;
        return String.format("%03d", millis);
    }
  }

  /** Format timezone offset in minutes. */
  private static String formatTimezoneOffsetMinutes(ZonedDateTime dt) {
    int offsetMinutes = dt.getOffset().getTotalSeconds() / 60;
    return String.format("%+d", offsetMinutes);
  }

  /**
   * Extract the first 10 digits from a timestamp to get UNIX seconds. This handles timestamps that
   * may be in milliseconds or nanoseconds.
   *
   * @param timestamp The timestamp value
   * @return UNIX timestamp in seconds
   */
  public static long extractUnixSeconds(double timestamp) {
    // Return as-is if within valid Unix timestamp range
    if (timestamp >= -MAX_UNIX_TIMESTAMP && timestamp <= MAX_UNIX_TIMESTAMP) {
      return (long) timestamp;
    }

    // For larger absolute values, extract first 10 digits (assumes milliseconds/nanoseconds)
    return extractFirstNDigits(timestamp, UNIX_TIMESTAMP_DIGITS);
  }

  /** Extract the first N digits from a number. */
  private static long extractFirstNDigits(double value, int digits) {
    boolean isNegative = value < 0;
    long absValue = Math.abs((long) value);
    String valueStr = String.valueOf(absValue);

    long result =
        valueStr.length() <= digits ? absValue : Long.parseLong(valueStr.substring(0, digits));

    return isNegative ? -result : result;
  }
}
