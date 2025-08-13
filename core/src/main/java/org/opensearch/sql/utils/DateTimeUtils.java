/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Locale;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

@UtilityClass
public class DateTimeUtils {

  private static final Pattern OFFSET_PATTERN = Pattern.compile("([+-])(\\d+)([smhdwMy]?)");
  private static final DateTimeFormatter DIRECT_FORMATTER =
      DateTimeFormatter.ofPattern("MM/dd/yyyy:HH:mm:ss");

  /**
   * Util method to round the date/time with given unit.
   *
   * @param utcMillis Date/time value to round, given in utc millis
   * @param unitMillis Date/time interval unit in utc millis
   * @return Rounded date/time value in utc millis
   */
  public static long roundFloor(long utcMillis, long unitMillis) {
    long res = utcMillis - utcMillis % unitMillis;
    return (utcMillis < 0 && res != utcMillis) ? res - unitMillis : res;
  }

  /**
   * Util method to round the date/time in week(s).
   *
   * @param utcMillis Date/time value to round, given in utc millis
   * @param interval Number of weeks as the rounding interval
   * @return Rounded date/time value in utc millis
   */
  public static long roundWeek(long utcMillis, int interval) {
    return roundFloor(utcMillis + 259200000L, 604800000L * interval) - 259200000L;
  }

  /**
   * Util method to round the date/time in month(s).
   *
   * @param utcMillis Date/time value to round, given in utc millis
   * @param interval Number of months as the rounding interval
   * @return Rounded date/time value in utc millis
   */
  public static long roundMonth(long utcMillis, int interval) {
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, UTC_ZONE_ID);
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(utcMillis).atZone(UTC_ZONE_ID).plusMonths(interval);
    long monthDiff =
        (zonedDateTime.getYear() - initDateTime.getYear()) * 12L
            + zonedDateTime.getMonthValue()
            - initDateTime.getMonthValue();
    long multiplier = monthDiff / interval - 1;
    if (monthDiff < 0 && monthDiff % interval != 0) --multiplier;
    long monthToAdd = multiplier * interval;
    return initDateTime.plusMonths(monthToAdd).toInstant().toEpochMilli();
  }

  /**
   * Util method to round the date/time in quarter(s).
   *
   * @param utcMillis Date/time value to round, given in utc millis
   * @param interval Number of quarters as the rounding interval
   * @return Rounded date/time value in utc millis
   */
  public static long roundQuarter(long utcMillis, int interval) {
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, UTC_ZONE_ID);
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(utcMillis).atZone(UTC_ZONE_ID).plusMonths(interval * 3L);
    long monthDiff =
        ((zonedDateTime.getYear() - initDateTime.getYear()) * 12L
            + zonedDateTime.getMonthValue()
            - initDateTime.getMonthValue());
    long multiplier = monthDiff / (interval * 3L) - 1;
    if (monthDiff < 0 && monthDiff % (interval * 3L) != 0) --multiplier;
    long monthToAdd = multiplier * interval * 3;
    return initDateTime.plusMonths(monthToAdd).toInstant().toEpochMilli();
  }

  /**
   * Util method to round the date/time in year(s).
   *
   * @param utcMillis Date/time value to round, given in utc millis
   * @param interval Number of years as the rounding interval
   * @return Rounded date/time value in utc millis
   */
  public static long roundYear(long utcMillis, int interval) {
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, UTC_ZONE_ID);
    ZonedDateTime zonedDateTime = Instant.ofEpochMilli(utcMillis).atZone(UTC_ZONE_ID);
    int yearDiff = zonedDateTime.getYear() - initDateTime.getYear();
    int multiplier = yearDiff / interval;
    if (yearDiff < 0 && yearDiff % interval != 0) --multiplier;
    int yearToAdd = multiplier * interval;
    return initDateTime.plusYears(yearToAdd).toInstant().toEpochMilli();
  }

  /**
   * Get window start time which aligns with the given size.
   *
   * @param timestamp event timestamp
   * @param size defines a window's start time to align with
   * @return start timestamp of the window
   */
  public long getWindowStartTime(long timestamp, long size) {
    return timestamp - timestamp % size;
  }

  /**
   * isValidMySqlTimeZoneId for timezones which match timezone the range set by MySQL.
   *
   * @param zone ZoneId of ZoneId type.
   * @return Boolean.
   */
  public Boolean isValidMySqlTimeZoneId(ZoneId zone) {
    String timeZoneMax = "+14:00";
    String timeZoneMin = "-13:59";
    String timeZoneZero = "+00:00";

    ZoneId maxTz = ZoneId.of(timeZoneMax);
    ZoneId minTz = ZoneId.of(timeZoneMin);
    ZoneId defaultTz = ZoneId.of(timeZoneZero);

    ZonedDateTime defaultDateTime = LocalDateTime.of(2000, 1, 2, 12, 0).atZone(defaultTz);

    ZonedDateTime maxTzValidator =
        defaultDateTime.withZoneSameInstant(maxTz).withZoneSameLocal(defaultTz);
    ZonedDateTime minTzValidator =
        defaultDateTime.withZoneSameInstant(minTz).withZoneSameLocal(defaultTz);
    ZonedDateTime passedTzValidator =
        defaultDateTime.withZoneSameInstant(zone).withZoneSameLocal(defaultTz);

    return (passedTzValidator.isBefore(maxTzValidator) || passedTzValidator.isEqual(maxTzValidator))
        && (passedTzValidator.isAfter(minTzValidator) || passedTzValidator.isEqual(minTzValidator));
  }

  /**
   * Extracts LocalDateTime from a datetime ExprValue. Uses `FunctionProperties` for
   * `ExprTimeValue`.
   */
  public static LocalDateTime extractDateTime(
      ExprValue value, FunctionProperties functionProperties) {
    return value instanceof ExprTimeValue
        ? ((ExprTimeValue) value).datetimeValue(functionProperties)
        : value.datetimeValue();
  }

  /**
   * Extracts LocalDateTime from a datetime ExprValue. Uses `FunctionProperties` for
   * `ExprTimeValue`.
   */
  public static Instant extractTimestamp(ExprValue value, FunctionProperties functionProperties) {
    return value instanceof ExprTimeValue
            ? ((ExprTimeValue) value).timestampValue(functionProperties)
            : value.timestampValue();
  }

  /**
   * Extracts LocalDate from a datetime ExprValue. Uses `FunctionProperties` for `ExprTimeValue`.
   */
  public static LocalDate extractDate(ExprValue value, FunctionProperties functionProperties) {
    return value instanceof ExprTimeValue
        ? ((ExprTimeValue) value).dateValue(functionProperties)
        : value.dateValue();
  }

  public static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");

  public static ZonedDateTime getRelativeZonedDateTime(String input, ZonedDateTime baseTime) {
    try {
      Instant parsed = LocalDateTime.parse(input, DIRECT_FORMATTER).toInstant(ZoneOffset.UTC);
      return parsed.atZone(baseTime.getZone());
    } catch (DateTimeParseException ignored) {
    }

    if ("now".equalsIgnoreCase(input) || "now()".equalsIgnoreCase(input)) {
      return baseTime;
    }

    ZonedDateTime result = baseTime;
    int i = 0;
    while (i < input.length()) {
      char c = input.charAt(i);
      if (c == '@') {
        int j = i + 1;
        while (j < input.length() && Character.isLetterOrDigit(input.charAt(j))) {
          j++;
        }
        String rawUnit = input.substring(i + 1, j);
        result = applySnap(result, rawUnit);
        i = j;
      } else if (c == '+' || c == '-') {
        int j = i + 1;
        while (j < input.length() && Character.isDigit(input.charAt(j))) {
          j++;
        }
        String valueStr = input.substring(i + 1, j);
        int value = valueStr.isEmpty() ? 1 : Integer.parseInt(valueStr);

        int k = j;
        while (k < input.length() && Character.isLetter(input.charAt(k))) {
          k++;
        }
        String rawUnit = input.substring(j, k);
        result = applyOffset(result, String.valueOf(c), value, rawUnit);
        i = k;
      } else {
        throw new IllegalArgumentException(
            "Unexpected character '" + c + "' at position " + i + " in input: " + input);
      }
    }

    return result;
  }

  private static ZonedDateTime applyOffset(
      ZonedDateTime base, String sign, int value, String rawUnit) {
    String unit = normalizeUnit(rawUnit);
    if ("q".equals(unit)) {
      int months = value * 3;
      return sign.equals("-") ? base.minusMonths(months) : base.plusMonths(months);
    }

    ChronoUnit chronoUnit;
    switch (unit) {
      case "s":
        chronoUnit = ChronoUnit.SECONDS;
        break;
      case "m":
        chronoUnit = ChronoUnit.MINUTES;
        break;
      case "h":
        chronoUnit = ChronoUnit.HOURS;
        break;
      case "d":
        chronoUnit = ChronoUnit.DAYS;
        break;
      case "w":
        chronoUnit = ChronoUnit.WEEKS;
        break;
      case "M":
        chronoUnit = ChronoUnit.MONTHS;
        break;
      case "y":
        chronoUnit = ChronoUnit.YEARS;
        break;
      default:
        throw new IllegalArgumentException("Unsupported offset unit: " + rawUnit);
    }


    return sign.equals("-") ? base.minus(value, chronoUnit) : base.plus(value, chronoUnit);
  }

  private static ZonedDateTime applySnap(ZonedDateTime base, String rawUnit) {
    String unit = normalizeUnit(rawUnit);

    ZonedDateTime result;
    switch (unit) {
      case "s":
        result = base.truncatedTo(ChronoUnit.SECONDS);
        break;
      case "m":
        result = base.truncatedTo(ChronoUnit.MINUTES);
        break;
      case "h":
        result = base.truncatedTo(ChronoUnit.HOURS);
        break;
      case "d":
        result = base.truncatedTo(ChronoUnit.DAYS);
        break;
      case "w":
        result = base.minusDays((base.getDayOfWeek().getValue() % 7)).truncatedTo(ChronoUnit.DAYS);
        break;
      case "M":
        result = base.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
        break;
      case "y":
        result = base.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS);
        break;
      case "q":
        int month = base.getMonthValue();
        int quarterStart = ((month - 1) / 3) * 3 + 1;
        result = base.withMonth(quarterStart).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
        break;
      default:
        if (unit.matches("w[0-7]")) {
          int targetDay = unit.equals("w0") || unit.equals("w7") ? 7 : Integer.parseInt(unit.substring(1));
          int diff = (base.getDayOfWeek().getValue() - targetDay + 7) % 7;
          result = base.minusDays(diff).truncatedTo(ChronoUnit.DAYS);
        } else {
          throw new IllegalArgumentException("Unsupported snap unit: " + rawUnit);
        }
        break;
    }
    return result;
  }

  private static String normalizeUnit(String rawUnit) {
    // strict minute (m or M)
    String lower = rawUnit.toLowerCase(Locale.ROOT);
    switch (lower) {
      case "m":
      case "min":
      case "mins":
      case "minute":
      case "minutes":
        return "m";
      case "s":
      case "sec":
      case "secs":
      case "second":
      case "seconds":
        return "s";
      case "h":
      case "hr":
      case "hrs":
      case "hour":
      case "hours":
        return "h";
      case "d":
      case "day":
      case "days":
        return "d";
      case "w":
      case "wk":
      case "wks":
      case "week":
      case "weeks":
        return "w";
      case "mon":
      case "month":
      case "months":
        return "M";
      case "y":
      case "yr":
      case "yrs":
      case "year":
      case "years":
        return "y";
      case "q":
      case "qtr":
      case "qtrs":
      case "quarter":
      case "quarters":
        return "q";
      default:
        if (lower.matches("w[0-7]")) return lower;
        throw new IllegalArgumentException("Unsupported unit alias: " + rawUnit);
    }
  }
}
