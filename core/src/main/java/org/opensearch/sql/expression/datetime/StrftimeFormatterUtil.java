/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import com.google.common.collect.ImmutableMap;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.TextStyle;
import java.time.temporal.IsoFields;
import java.time.temporal.WeekFields;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprValue;

/** This class supports POSIX-style strftime formatting. */
public class StrftimeFormatterUtil {

  interface StrftimeFormatHandler {
    String getFormat(ZonedDateTime dateTime);
  }

  private static final Map<String, StrftimeFormatHandler> STRFTIME_HANDLERS =
      ImmutableMap.<String, StrftimeFormatHandler>builder()
          // Date and time variables
          .put(
              "%c",
              dt ->
                  dt.format(
                      DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss yyyy", Locale.getDefault())))
          .put(
              "%+",
              dt ->
                  dt.format(
                      DateTimeFormatter.ofPattern(
                          "EEE MMM dd HH:mm:ss zzz yyyy", Locale.getDefault())))

          // Time variables
          .put("%Ez", dt -> String.valueOf(dt.getOffset().getTotalSeconds() / 60))
          .put("%f", dt -> String.format("%06d", dt.getNano() / 1000))
          .put("%H", dt -> dt.format(DateTimeFormatter.ofPattern("HH", Locale.ROOT)))
          .put("%I", dt -> dt.format(DateTimeFormatter.ofPattern("hh", Locale.ROOT)))
          .put("%k", dt -> String.format("%2d", dt.getHour()))
          .put("%M", dt -> dt.format(DateTimeFormatter.ofPattern("mm", Locale.ROOT)))
          .put("%p", dt -> dt.format(DateTimeFormatter.ofPattern("a", Locale.getDefault())))
          .put("%S", dt -> dt.format(DateTimeFormatter.ofPattern("ss", Locale.ROOT)))
          .put("%s", dt -> String.valueOf(dt.toEpochSecond()))
          .put("%T", dt -> dt.format(DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT)))
          .put("%X", dt -> dt.format(DateTimeFormatter.ofPattern("HH:mm:ss", Locale.getDefault())))
          .put("%Z", dt -> dt.getZone().getDisplayName(TextStyle.SHORT, Locale.getDefault()))
          .put("%z", dt -> dt.format(DateTimeFormatter.ofPattern("xx", Locale.ROOT)))
          .put("%:z", dt -> dt.format(DateTimeFormatter.ofPattern("xxx", Locale.ROOT)))
          .put("%::z", dt -> dt.format(DateTimeFormatter.ofPattern("xxx:ss", Locale.ROOT)))
          .put("%:::z", dt -> dt.format(DateTimeFormatter.ofPattern("x", Locale.ROOT)))
          .put("%%", dt -> "%")

          // Date variables
          .put("%F", dt -> dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT)))
          .put(
              "%x", dt -> dt.format(DateTimeFormatter.ofPattern("MM/dd/yyyy", Locale.getDefault())))

          // Days and weeks
          .put("%A", dt -> dt.format(DateTimeFormatter.ofPattern("EEEE", Locale.getDefault())))
          .put("%a", dt -> dt.format(DateTimeFormatter.ofPattern("EEE", Locale.getDefault())))
          .put("%d", dt -> dt.format(DateTimeFormatter.ofPattern("dd", Locale.ROOT)))
          .put("%e", dt -> String.format("%2d", dt.getDayOfMonth()))
          .put("%j", dt -> dt.format(DateTimeFormatter.ofPattern("DDD", Locale.ROOT)))
          .put("%V", dt -> String.format("%02d", dt.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)))
          .put("%U", dt -> String.format("%02d", dt.get(WeekFields.SUNDAY_START.weekOfYear()) - 1))
          .put("%w", dt -> String.valueOf(dt.getDayOfWeek().getValue() % 7))

          // Months
          .put("%b", dt -> dt.format(DateTimeFormatter.ofPattern("MMM", Locale.getDefault())))
          .put("%B", dt -> dt.format(DateTimeFormatter.ofPattern("MMMM", Locale.getDefault())))
          .put("%m", dt -> dt.format(DateTimeFormatter.ofPattern("MM", Locale.ROOT)))

          // Year
          .put("%C", dt -> String.format("%02d", dt.getYear() / 100))
          .put("%g", dt -> String.format("%02d", dt.get(IsoFields.WEEK_BASED_YEAR) % 100))
          .put("%G", dt -> String.format("%04d", dt.get(IsoFields.WEEK_BASED_YEAR)))
          .put("%y", dt -> dt.format(DateTimeFormatter.ofPattern("yy", Locale.ROOT)))
          .put("%Y", dt -> dt.format(DateTimeFormatter.ofPattern("yyyy", Locale.ROOT)))
          .build();

  // Pattern to match %N and %Q with optional precision digit
  private static final Pattern SUBSECOND_PATTERN = Pattern.compile("%(\\d)?([NQ])");

  /**
   * Format a LocalDateTime using STRFTIME format specifiers.
   *
   * @param dateTime The LocalDateTime to format
   * @param formatString The STRFTIME format string
   * @return Formatted string
   */
  public static ExprValue formatDateTime(LocalDateTime dateTime, String formatString) {
    if (dateTime == null || formatString == null) {
      return ExprNullValue.of();
    }

    // Convert to ZonedDateTime with UTC
    ZonedDateTime zdt = dateTime.atZone(ZoneId.of("UTC"));
    return formatZonedDateTime(zdt, formatString);
  }

  /**
   * Format a ZonedDateTime using STRFTIME format specifiers.
   *
   * @param dateTime The ZonedDateTime to format
   * @param formatString The STRFTIME format string
   * @return Formatted string
   */
  public static ExprValue formatZonedDateTime(ZonedDateTime dateTime, String formatString) {
    if (dateTime == null || formatString == null) {
      return ExprNullValue.of();
    }

    String result = formatString;

    // Handle %N and %Q with precision
    result = handleSubsecondFormats(result, dateTime);

    // First, escape %% by replacing with a placeholder
    String percentPlaceholder = "\u0001PERCENT\u0001";
    result = result.replace("%%", percentPlaceholder);

    // Replace all other format specifiers
    for (Map.Entry<String, StrftimeFormatHandler> entry : STRFTIME_HANDLERS.entrySet()) {
      String specifier = entry.getKey();
      if (specifier.equals("%%")) {
        continue; // Skip %%, we handle it separately
      }
      StrftimeFormatHandler handler = entry.getValue();

      if (result.contains(specifier)) {
        String replacement = handler.getFormat(dateTime);
        result = result.replace(specifier, replacement);
      }
    }

    // Finally, replace the percent placeholder with literal %
    result = result.replace(percentPlaceholder, "%");

    return new ExprStringValue(result);
  }

  /** Handle %N and %Q subsecond formats with optional precision. */
  private static String handleSubsecondFormats(String format, ZonedDateTime dateTime) {
    StringBuffer result = new StringBuffer();
    Matcher matcher = SUBSECOND_PATTERN.matcher(format);

    while (matcher.find()) {
      String precisionStr = matcher.group(1);
      String type = matcher.group(2);

      int precision =
          (precisionStr != null)
              ? Integer.parseInt(precisionStr)
              : (type.equals("N") ? 9 : 3); // Default: %N=9, %Q=3

      String replacement;
      if (type.equals("N")) {
        // %N - subsecond digits (nanoseconds)
        long nanos = dateTime.getNano();
        replacement = formatSubseconds(nanos, precision, 1_000_000_000L);
      } else {
        // %Q - subsecond component (milliseconds by default)
        long millis = dateTime.toInstant().toEpochMilli() % 1000;
        if (precision == 6) {
          // Microseconds
          long micros = (dateTime.getNano() / 1000);
          replacement = String.format("%06d", micros);
        } else if (precision == 9) {
          // Nanoseconds
          replacement = String.format("%09d", dateTime.getNano());
        } else {
          // Default to milliseconds
          replacement = String.format("%03d", millis);
        }
      }

      matcher.appendReplacement(result, replacement);
    }
    matcher.appendTail(result);

    return result.toString();
  }

  /** Format subseconds with the specified precision. */
  private static String formatSubseconds(long value, int precision, long maxValue) {
    double scaled = (double) value / maxValue;
    long truncated = (long) (scaled * Math.pow(10, precision));
    return String.format("%0" + precision + "d", truncated);
  }

  /**
   * Extract the first 10 digits from a timestamp to get UNIX seconds.
   *
   * @param timestamp The timestamp value
   * @return UNIX timestamp in seconds
   */
  public static long extractUnixSeconds(double timestamp) {
    if (timestamp < 0) {
      return (long) timestamp;
    }

    // If the timestamp is already in the valid seconds range, return as-is
    if (timestamp <= 32536771199L) {
      return (long) timestamp;
    }

    // For larger values, extract first 10 digits (assumes nanoseconds or similar)
    String timestampStr = String.valueOf((long) timestamp);
    if (timestampStr.length() > 10) {
      timestampStr = timestampStr.substring(0, 10);
    }
    return Long.parseLong(timestampStr);
  }
}
