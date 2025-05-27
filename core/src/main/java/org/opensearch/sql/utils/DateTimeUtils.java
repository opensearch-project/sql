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
import java.util.regex.Matcher;
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
    return utcMillis - utcMillis % unitMillis;
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
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC).plusMonths(interval);
    long monthDiff =
        (zonedDateTime.getYear() - initDateTime.getYear()) * 12L
            + zonedDateTime.getMonthValue()
            - initDateTime.getMonthValue();
    long monthToAdd = (monthDiff / interval - 1) * interval;
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
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC).plusMonths(interval * 3L);
    long monthDiff =
        ((zonedDateTime.getYear() - initDateTime.getYear()) * 12L
            + zonedDateTime.getMonthValue()
            - initDateTime.getMonthValue());
    long monthToAdd = (monthDiff / (interval * 3L) - 1) * interval * 3;
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
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime zonedDateTime = Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC);
    int yearDiff = zonedDateTime.getYear() - initDateTime.getYear();
    int yearToAdd = (yearDiff / interval) * interval;
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

  public static ZonedDateTime getRelativeZonedDateTime(String input, ZonedDateTime baseTime) {
    try {
      Instant localDateTime =
          LocalDateTime.parse(input, DIRECT_FORMATTER).toInstant(ZoneOffset.UTC);
      return localDateTime.atZone(baseTime.getZone());
    } catch (DateTimeParseException ignored) {
    }

    if ("now".equalsIgnoreCase(input) || "now()".equalsIgnoreCase(input)) {
      return baseTime;
    }

    // 1. extract snap（like @d）
    String snapUnit = null;
    int atIndex = input.indexOf('@');
    if (atIndex != -1) {
      snapUnit = input.substring(atIndex + 1);
      input = input.substring(0, atIndex);
    }

    // 2. apply snap
    ZonedDateTime result = baseTime;
    if (snapUnit != null && !snapUnit.isEmpty()) {
      result = applySnap(result, snapUnit);
    }

    // 3. apply offset one by one（like -1d+2h-10m）
    Matcher matcher = OFFSET_PATTERN.matcher(input);
    while (matcher.find()) {
      String sign = matcher.group(1);
      int value = Integer.parseInt(matcher.group(2));
      String unit = matcher.group(3);
      if (unit == null || unit.isEmpty()) {
        unit = "s"; // default value is second
      }
      result = applyOffset(result, sign, value, unit);
    }

    return result;
  }

  private static ZonedDateTime applyOffset(
      ZonedDateTime base, String sign, int value, String unit) {
    ChronoUnit chronoUnit = parseUnit(unit);
    return sign.equals("-") ? base.minus(value, chronoUnit) : base.plus(value, chronoUnit);
  }

  private static ZonedDateTime applySnap(ZonedDateTime base, String unit) {
    switch (unit) {
      case "s":
        return base.truncatedTo(ChronoUnit.SECONDS);
      case "m":
        return base.truncatedTo(ChronoUnit.MINUTES);
      case "h":
        return base.truncatedTo(ChronoUnit.HOURS);
      case "d":
        return base.truncatedTo(ChronoUnit.DAYS);
      case "w":
        return base.minusDays((base.getDayOfWeek().getValue() % 7)).truncatedTo(ChronoUnit.DAYS);
      case "M":
        return base.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
      case "y":
        return base.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS);
      default:
        throw new IllegalArgumentException("Unsupported snap unit: " + unit);
    }
  }

  private static ChronoUnit parseUnit(String unit) {
    switch (unit) {
      case "s":
        return ChronoUnit.SECONDS;
      case "m":
        return ChronoUnit.MINUTES;
      case "h":
        return ChronoUnit.HOURS;
      case "d":
        return ChronoUnit.DAYS;
      case "w":
        return ChronoUnit.WEEKS;
      case "M":
        return ChronoUnit.MONTHS;
      case "y":
        return ChronoUnit.YEARS;
      default:
        throw new IllegalArgumentException("Unsupported time unit: " + unit);
    }
  }
}
