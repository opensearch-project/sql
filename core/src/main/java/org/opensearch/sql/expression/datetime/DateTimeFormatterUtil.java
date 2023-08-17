/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.datetime;

import static org.opensearch.sql.utils.DateTimeUtils.UTC_ZONE_ID;

import com.google.common.collect.ImmutableMap;
import java.text.ParsePosition;
import java.time.Clock;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.opensearch.sql.data.model.ExprNullValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

/**
 * This class converts a SQL style DATE_FORMAT format specifier and converts it to a Java
 * SimpleDateTime format.
 */
class DateTimeFormatterUtil {
  private static final int SUFFIX_SPECIAL_START_TH = 11;
  private static final int SUFFIX_SPECIAL_END_TH = 13;
  private static final String SUFFIX_SPECIAL_TH = "th";

  private static final String NANO_SEC_FORMAT = "'%06d'";

  private static final Map<Integer, String> SUFFIX_CONVERTER =
      ImmutableMap.<Integer, String>builder().put(1, "st").put(2, "nd").put(3, "rd").build();

  // The following have special cases that need handling outside of the format options provided
  // by the DateTimeFormatter class.
  interface DateTimeFormatHandler {
    String getFormat(LocalDateTime date);
  }

  private static final Map<String, DateTimeFormatHandler> DATE_HANDLERS =
      ImmutableMap.<String, DateTimeFormatHandler>builder()
          .put("%a", (date) -> "EEE") // %a => EEE - Abbreviated weekday name (Sun..Sat)
          .put("%b", (date) -> "LLL") // %b => LLL - Abbreviated month name (Jan..Dec)
          .put("%c", (date) -> "MM") // %c => MM - Month, numeric (0..12)
          .put("%d", (date) -> "dd") // %d => dd - Day of the month, numeric (00..31)
          .put("%e", (date) -> "d") // %e => d - Day of the month, numeric (0..31)
          .put("%H", (date) -> "HH") // %H => HH - (00..23)
          .put("%h", (date) -> "hh") // %h => hh - (01..12)
          .put("%I", (date) -> "hh") // %I => hh - (01..12)
          .put("%i", (date) -> "mm") // %i => mm - Minutes, numeric (00..59)
          .put("%j", (date) -> "DDD") // %j => DDD - (001..366)
          .put("%k", (date) -> "H") // %k => H - (0..23)
          .put("%l", (date) -> "h") // %l => h - (1..12)
          .put("%p", (date) -> "a") // %p => a - AM or PM
          .put("%M", (date) -> "LLLL") // %M => LLLL - Month name (January..December)
          .put("%m", (date) -> "MM") // %m => MM - Month, numeric (00..12)
          .put("%r", (date) -> "hh:mm:ss a") // %r => hh:mm:ss a - hh:mm:ss followed by AM or PM
          .put("%S", (date) -> "ss") // %S => ss - Seconds (00..59)
          .put("%s", (date) -> "ss") // %s => ss - Seconds (00..59)
          .put("%T", (date) -> "HH:mm:ss") // %T => HH:mm:ss
          .put("%W", (date) -> "EEEE") // %W => EEEE - Weekday name (Sunday..Saturday)
          .put("%Y", (date) -> "yyyy") // %Y => yyyy - Year, numeric, 4 digits
          .put("%y", (date) -> "yy") // %y => yy - Year, numeric, 2 digits
          // The following are not directly supported by DateTimeFormatter.
          .put(
              "%D",
              (date) -> // %w - Day of month with English suffix
              String.format("'%d%s'", date.getDayOfMonth(), getSuffix(date.getDayOfMonth())))
          .put(
              "%f",
              (date) -> // %f - Microseconds
              String.format(NANO_SEC_FORMAT, (date.getNano() / 1000)))
          .put(
              "%w",
              (date) -> // %w - Day of week (0 indexed)
              String.format("'%d'", date.getDayOfWeek().getValue()))
          .put(
              "%U",
              (date) -> // %U Week where Sunday is the first day - WEEK() mode 0
              String.format("'%d'", CalendarLookup.getWeekNumber(0, date.toLocalDate())))
          .put(
              "%u",
              (date) -> // %u Week where Monday is the first day - WEEK() mode 1
              String.format("'%d'", CalendarLookup.getWeekNumber(1, date.toLocalDate())))
          .put(
              "%V",
              (date) -> // %V Week where Sunday is the first day - WEEK() mode 2 used with %X
              String.format("'%d'", CalendarLookup.getWeekNumber(2, date.toLocalDate())))
          .put(
              "%v",
              (date) -> // %v Week where Monday is the first day - WEEK() mode 3 used with %x
              String.format("'%d'", CalendarLookup.getWeekNumber(3, date.toLocalDate())))
          .put(
              "%X",
              (date) -> // %X Year for week where Sunday is the first day, 4 digits used with %V
              String.format("'%d'", CalendarLookup.getYearNumber(2, date.toLocalDate())))
          .put(
              "%x",
              (date) -> // %x Year for week where Monday is the first day, 4 digits used with %v
              String.format("'%d'", CalendarLookup.getYearNumber(3, date.toLocalDate())))
          .build();

  // Handlers for the time_format function.
  // Some format specifiers return 0 or null to align with MySQL.
  // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_time-format
  private static final Map<String, DateTimeFormatHandler> TIME_HANDLERS =
      ImmutableMap.<String, DateTimeFormatHandler>builder()
          .put("%a", (date) -> null)
          .put("%b", (date) -> null)
          .put("%c", (date) -> "0")
          .put("%d", (date) -> "00")
          .put("%e", (date) -> "0")
          .put("%H", (date) -> "HH") // %H => HH - (00..23)
          .put("%h", (date) -> "hh") // %h => hh - (01..12)
          .put("%I", (date) -> "hh") // %I => hh - (01..12)
          .put("%i", (date) -> "mm") // %i => mm - Minutes, numeric (00..59)
          .put("%j", (date) -> null)
          .put("%k", (date) -> "H") // %k => H - (0..23)
          .put("%l", (date) -> "h") // %l => h - (1..12)
          .put("%p", (date) -> "a") // %p => a - AM or PM
          .put("%M", (date) -> null)
          .put("%m", (date) -> "00")
          .put("%r", (date) -> "hh:mm:ss a") // %r => hh:mm:ss a - hh:mm:ss followed by AM or PM
          .put("%S", (date) -> "ss") // %S => ss - Seconds (00..59)
          .put("%s", (date) -> "ss") // %s => ss - Seconds (00..59)
          .put("%T", (date) -> "HH:mm:ss") // %T => HH:mm:ss
          .put("%W", (date) -> null)
          .put("%Y", (date) -> "0000")
          .put("%y", (date) -> "00")
          .put("%D", (date) -> null)
          // %f - Microseconds
          .put("%f", (date) -> String.format(NANO_SEC_FORMAT, (date.getNano() / 1000)))
          .put("%w", (date) -> null)
          .put("%U", (date) -> null)
          .put("%u", (date) -> null)
          .put("%V", (date) -> null)
          .put("%v", (date) -> null)
          .put("%X", (date) -> null)
          .put("%x", (date) -> null)
          .build();

  private static final Map<String, String> STR_TO_DATE_FORMATS =
      ImmutableMap.<String, String>builder()
          .put("%a", "EEE") // %a => EEE - Abbreviated weekday name (Sun..Sat)
          .put("%b", "LLL") // %b => LLL - Abbreviated month name (Jan..Dec)
          .put("%c", "M") // %c => MM - Month, numeric (0..12)
          .put("%d", "d") // %d => dd - Day of the month, numeric (00..31)
          .put("%e", "d") // %e => d - Day of the month, numeric (0..31)
          .put("%H", "H") // %H => HH - (00..23)
          .put("%h", "H") // %h => hh - (01..12)
          .put("%I", "h") // %I => hh - (01..12)
          .put("%i", "m") // %i => mm - Minutes, numeric (00..59)
          .put("%j", "DDD") // %j => DDD - (001..366)
          .put("%k", "H") // %k => H - (0..23)
          .put("%l", "h") // %l => h - (1..12)
          .put("%p", "a") // %p => a - AM or PM
          .put("%M", "LLLL") // %M => LLLL - Month name (January..December)
          .put("%m", "M") // %m => MM - Month, numeric (00..12)
          .put("%r", "hh:mm:ss a") // %r => hh:mm:ss a - hh:mm:ss followed by AM or PM
          .put("%S", "s") // %S => ss - Seconds (00..59)
          .put("%s", "s") // %s => ss - Seconds (00..59)
          .put("%T", "HH:mm:ss") // %T => HH:mm:ss
          .put("%W", "EEEE") // %W => EEEE - Weekday name (Sunday..Saturday)
          .put("%Y", "u") // %Y => yyyy - Year, numeric, 4 digits
          .put("%y", "u") // %y => yy - Year, numeric, 2 digits
          .put("%f", "n") // %f => n - Nanoseconds
          // The following have been implemented but cannot be aligned with
          // MySQL due to the limitations of the DatetimeFormatter
          .put("%D", "d") // %w - Day of month with English suffix
          .put("%w", "e") // %w - Day of week (0 indexed)
          .put("%U", "w") // %U Week where Sunday is the first day - WEEK() mode 0
          .put("%u", "w") // %u Week where Monday is the first day - WEEK() mode 1
          .put("%V", "w") // %V Week where Sunday is the first day - WEEK() mode 2
          .put("%v", "w") // %v Week where Monday is the first day - WEEK() mode 3
          .put("%X", "u") // %X Year for week where Sunday is the first day
          .put("%x", "u") // %x Year for week where Monday is the first day
          .build();

  private static final Pattern pattern = Pattern.compile("%.");
  private static final Pattern CHARACTERS_WITH_NO_MOD_LITERAL_BEHIND_PATTERN =
      Pattern.compile("(?<!%)[a-zA-Z&&[^aydmshiHIMYDSEL]]+");
  private static final String MOD_LITERAL = "%";

  private DateTimeFormatterUtil() {}

  static StringBuffer getCleanFormat(ExprValue formatExpr) {
    final StringBuffer cleanFormat = new StringBuffer();
    final Matcher m =
        CHARACTERS_WITH_NO_MOD_LITERAL_BEHIND_PATTERN.matcher(formatExpr.stringValue());

    while (m.find()) {
      m.appendReplacement(cleanFormat, String.format("'%s'", m.group()));
    }
    m.appendTail(cleanFormat);

    return cleanFormat;
  }

  /**
   * Helper function to format a DATETIME according to a provided handler and matcher.
   *
   * @param formatExpr ExprValue containing the format expression
   * @param handler Map of character patterns to their associated datetime format
   * @param datetime The datetime argument being formatted
   * @return A formatted string expression
   */
  static ExprValue getFormattedString(
      ExprValue formatExpr, Map<String, DateTimeFormatHandler> handler, LocalDateTime datetime) {
    StringBuffer cleanFormat = getCleanFormat(formatExpr);

    final Matcher matcher = pattern.matcher(cleanFormat.toString());
    final StringBuffer format = new StringBuffer();
    try {
      while (matcher.find()) {
        matcher.appendReplacement(
            format,
            handler
                .getOrDefault(
                    matcher.group(),
                    (d) -> String.format("'%s'", matcher.group().replaceFirst(MOD_LITERAL, "")))
                .getFormat(datetime));
      }
    } catch (Exception e) {
      return ExprNullValue.of();
    }
    matcher.appendTail(format);

    // English Locale matches SQL requirements.
    // 'AM'/'PM' instead of 'a.m.'/'p.m.'
    // 'Sat' instead of 'Sat.' etc
    return new ExprStringValue(
        datetime.format(DateTimeFormatter.ofPattern(format.toString(), Locale.ENGLISH)));
  }

  /**
   * Format the date using the date format String.
   *
   * @param dateExpr the date ExprValue of Date/Timestamp/String type.
   * @param formatExpr the format ExprValue of String type.
   * @return Date formatted using format and returned as a String.
   */
  static ExprValue getFormattedDate(ExprValue dateExpr, ExprValue formatExpr) {
    final LocalDateTime date = dateExpr.timestampValue().atZone(UTC_ZONE_ID).toLocalDateTime();
    return getFormattedString(formatExpr, DATE_HANDLERS, date);
  }

  static ExprValue getFormattedDateOfToday(ExprValue formatExpr, ExprValue time, Clock current) {
    final LocalDateTime date = LocalDateTime.of(LocalDate.now(current), time.timeValue());

    return getFormattedString(formatExpr, DATE_HANDLERS, date);
  }

  /**
   * Format the date using the date format String.
   *
   * @param timeExpr the date ExprValue of Date/Datetime/Timestamp/String type.
   * @param formatExpr the format ExprValue of String type.
   * @return Date formatted using format and returned as a String.
   */
  static ExprValue getFormattedTime(ExprValue timeExpr, ExprValue formatExpr) {
    // Initializes DateTime with LocalDate.now(). This is safe because the date is ignored.
    // The time_format function will only return 0 or null for invalid string format specifiers.
    final LocalDateTime time = LocalDateTime.of(LocalDate.now(), timeExpr.timeValue());

    return getFormattedString(formatExpr, TIME_HANDLERS, time);
  }

  private static boolean canGetDate(TemporalAccessor ta) {
    return (ta.isSupported(ChronoField.YEAR)
        && ta.isSupported(ChronoField.MONTH_OF_YEAR)
        && ta.isSupported(ChronoField.DAY_OF_MONTH));
  }

  private static boolean canGetTime(TemporalAccessor ta) {
    return (ta.isSupported(ChronoField.HOUR_OF_DAY)
        && ta.isSupported(ChronoField.MINUTE_OF_HOUR)
        && ta.isSupported(ChronoField.SECOND_OF_MINUTE));
  }

  static ExprValue parseStringWithDateOrTime(
      FunctionProperties fp, ExprValue datetimeStringExpr, ExprValue formatExpr) {

    // Replace patterns with % for Java DateTimeFormatter
    StringBuffer cleanFormat = getCleanFormat(formatExpr);
    final Matcher matcher = pattern.matcher(cleanFormat.toString());
    final StringBuffer format = new StringBuffer();

    while (matcher.find()) {
      matcher.appendReplacement(
          format,
          STR_TO_DATE_FORMATS.getOrDefault(
              matcher.group(),
              String.format("'%s'", matcher.group().replaceFirst(MOD_LITERAL, ""))));
    }
    matcher.appendTail(format);

    TemporalAccessor taWithMissingFields;
    // Return NULL for invalid parse in string to align with MySQL
    try {
      // Get Temporal Accessor to initially parse string without default values
      taWithMissingFields =
          new DateTimeFormatterBuilder()
              .appendPattern(format.toString())
              .toFormatter()
              .withResolverStyle(ResolverStyle.STRICT)
              .parseUnresolved(datetimeStringExpr.stringValue(), new ParsePosition(0));
      if (taWithMissingFields == null) {
        throw new DateTimeException("Input string could not be parsed properly.");
      }
      if (!canGetDate(taWithMissingFields) && !canGetTime(taWithMissingFields)) {
        throw new DateTimeException("Not enough data to build a valid Date, Time, or Datetime.");
      }
    } catch (DateTimeException e) {
      return ExprNullValue.of();
    }

    int year =
        taWithMissingFields.isSupported(ChronoField.YEAR)
            ? taWithMissingFields.get(ChronoField.YEAR)
            : 2000;

    int month =
        taWithMissingFields.isSupported(ChronoField.MONTH_OF_YEAR)
            ? taWithMissingFields.get(ChronoField.MONTH_OF_YEAR)
            : 1;

    int day =
        taWithMissingFields.isSupported(ChronoField.DAY_OF_MONTH)
            ? taWithMissingFields.get(ChronoField.DAY_OF_MONTH)
            : 1;

    int hour =
        taWithMissingFields.isSupported(ChronoField.HOUR_OF_DAY)
            ? taWithMissingFields.get(ChronoField.HOUR_OF_DAY)
            : 0;

    int minute =
        taWithMissingFields.isSupported(ChronoField.MINUTE_OF_HOUR)
            ? taWithMissingFields.get(ChronoField.MINUTE_OF_HOUR)
            : 0;

    int second =
        taWithMissingFields.isSupported(ChronoField.SECOND_OF_MINUTE)
            ? taWithMissingFields.get(ChronoField.SECOND_OF_MINUTE)
            : 0;

    // Fill returned datetime with current date if only Time information was parsed
    LocalDateTime output;
    if (!canGetDate(taWithMissingFields)) {
      output =
          LocalDateTime.of(
              LocalDate.now(fp.getQueryStartClock()), LocalTime.of(hour, minute, second));
    } else {
      output = LocalDateTime.of(year, month, day, hour, minute, second);
    }

    return new ExprTimestampValue(output);
  }

  /**
   * Returns English suffix of incoming value.
   *
   * @param val Incoming value.
   * @return English suffix as String (st, nd, rd, th)
   */
  private static String getSuffix(int val) {
    // The numbers 11, 12, and 13 do not follow general suffix rules.
    if ((SUFFIX_SPECIAL_START_TH <= val) && (val <= SUFFIX_SPECIAL_END_TH)) {
      return SUFFIX_SPECIAL_TH;
    }
    return SUFFIX_CONVERTER.getOrDefault(val % 10, SUFFIX_SPECIAL_TH);
  }
}
