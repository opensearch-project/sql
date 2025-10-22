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
import java.util.Optional;
import java.util.Set;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;

@UtilityClass
public class DateTimeUtils {

  private static final DateTimeFormatter DIRECT_FORMATTER =
      DateTimeFormatter.ofPattern("MM/dd/yyyy:HH:mm:ss");
  public static final Set<DateTimeFormatter> SUPPORTED_FORMATTERS =
      Set.of(
          DIRECT_FORMATTER,
          DateTimeFormatters.DATE_TIMESTAMP_FORMATTER,
          DateTimeFormatter.ISO_DATE_TIME);

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
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC).plusMonths(interval);
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
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC).plusMonths(interval * 3L);
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
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime zonedDateTime = Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC);
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
    Optional<ZonedDateTime> parsed = tryParseAbsoluteTime(input);
    if (parsed.isPresent()) {
      return parsed.get().withZoneSameInstant(baseTime.getZone());
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

    ChronoUnit chronoUnit = mapChronoUnit(unit, "Unsupported offset unit: " + rawUnit);
    return sign.equals("-") ? base.minus(value, chronoUnit) : base.plus(value, chronoUnit);
  }

  private static ZonedDateTime applySnap(ZonedDateTime base, String rawUnit) {
    String unit = normalizeUnit(rawUnit);

    return switch (unit) {
      case "s" -> base.truncatedTo(ChronoUnit.SECONDS);
      case "m" -> base.truncatedTo(ChronoUnit.MINUTES);
      case "h" -> base.truncatedTo(ChronoUnit.HOURS);
      case "d" -> base.truncatedTo(ChronoUnit.DAYS);
      case "w" -> base.minusDays((base.getDayOfWeek().getValue() % 7)).truncatedTo(ChronoUnit.DAYS);
      case "M" -> base.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
      case "y" -> base.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS);
      case "q" -> {
        int month = base.getMonthValue();
        int quarterStart = ((month - 1) / 3) * 3 + 1;
        yield base.withMonth(quarterStart).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
      }
      default -> {
        if (unit.matches("w[0-7]")) {
          int targetDay =
              unit.equals("w0") || unit.equals("w7") ? 7 : Integer.parseInt(unit.substring(1));
          int diff = (base.getDayOfWeek().getValue() - targetDay + 7) % 7;
          yield base.minusDays(diff).truncatedTo(ChronoUnit.DAYS);
        } else {
          throw new IllegalArgumentException("Unsupported snap unit: " + rawUnit);
        }
      }
    };
  }

  private static String normalizeUnit(String rawUnit) {
    // strict minute (m or M)
    switch (rawUnit.toLowerCase(Locale.ROOT)) {
      case "m", "min", "mins", "minute", "minutes" -> {
        return "m";
      }
      case "s", "sec", "secs", "second", "seconds" -> {
        return "s";
      }
      case "h", "hr", "hrs", "hour", "hours" -> {
        return "h";
      }
      case "d", "day", "days" -> {
        return "d";
      }
      case "w", "wk", "wks", "week", "weeks" -> {
        return "w";
      }
      case "mon", "month", "months" -> {
        return "M"; // month
      }
      case "y", "yr", "yrs", "year", "years" -> {
        return "y";
      }
      case "q", "qtr", "qtrs", "quarter", "quarters" -> {
        return "q";
      }
      default -> {
        String lower = rawUnit.toLowerCase();
        if (lower.matches("w[0-7]")) return lower;
        throw new IllegalArgumentException("Unsupported unit alias: " + rawUnit);
      }
    }
  }

  /**
   * Translate a PPL time modifier expression to a <a
   * href="https://docs.opensearch.org/latest/field-types/supported-field-types/date/#date-math"
   * >OpenSearch date math expression</a>.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>2020-12-10 12:00:00.123 -> 2020-12-10T12:00:00.123Z
   *   <li>now, now() -> now
   *   <li>-30seconds, -30s -> now-30s
   *   <li>-1h@d -> now-1h/d
   *   <li>2020-12-10 12:00:00.123@month -> 2020-12-10T12:00:00.123Z/M
   * </ul>
   *
   * @param timeModifier The time modifier string in PPL format
   * @return The time string in OpenSearch date math format
   */
  public static String resolveTimeModifier(String timeModifier) {
    return resolveTimeModifier(timeModifier, ZonedDateTime.now(ZoneOffset.UTC));
  }

  /**
   * Convert time modifier with a reference for now. This is mainly useful for quarter conversion,
   * which is time-sensitive.
   *
   * <p>Background: PPL time modifier supports alignment to quarter, while OpenSearch does not
   * support so in time math. We implement a workaround by first aligning the date to the current
   * month then subtracting 0 to 2 months from it. In order to know how many months to subtract, it
   * is useful to know when is it now.
   */
  static String resolveTimeModifier(String input, ZonedDateTime nowReference) {
    if (input == null || input.isEmpty()) {
      return null;
    }

    if ("now".equalsIgnoreCase(input) || "now()".equalsIgnoreCase(input)) {
      return "now";
    }

    String absoluteTime = tryParseAbsoluteTimeAndFormat(input);
    if (absoluteTime != null) {
      return absoluteTime;
    }

    return parseRelativeTimeExpression(input, nowReference);
  }

  /**
   * Try to parse the input as an absolute datetime.
   *
   * @param input The time string
   * @return ISO formatted datetime string or null if parsing fails
   */
  private static String tryParseAbsoluteTimeAndFormat(String input) {
    Optional<ZonedDateTime> parsed = tryParseAbsoluteTime(input);
    return parsed
        .map(zonedDateTime -> zonedDateTime.format(DateTimeFormatter.ISO_INSTANT))
        .orElse(null);
  }

  private static Optional<ZonedDateTime> tryParseAbsoluteTime(String input) {
    for (DateTimeFormatter formatter : SUPPORTED_FORMATTERS) {
      try {
        ZonedDateTime parsed;
        if (formatter == DateTimeFormatter.ISO_DATE_TIME) {
          // ISO_DATE_TIME can handle zone information
          parsed = ZonedDateTime.parse(input, formatter);
        } else {
          // Treat LocalDateTime formatters as UTC
          LocalDateTime localDateTime = LocalDateTime.parse(input, formatter);
          parsed = localDateTime.atZone(ZoneOffset.UTC);
        }
        return Optional.of(parsed);
      } catch (DateTimeParseException ignored) {
        // Try next formatter
      }
    }
    return Optional.empty();
  }

  /**
   * Parse a PPL relative time expression and convert it to OpenSearch date math format. This
   * overload accepts a reference time for date calculations, particularly useful for quarters.
   */
  private static String parseRelativeTimeExpression(String input, ZonedDateTime nowReference) {
    if (!containsRelativeTimeOperators(input)) {
      return input;
    }

    StringBuilder result = new StringBuilder("now");
    int position = 0;
    ZonedDateTime currentReference =
        nowReference; // Track current reference time as operations are applied

    while (position < input.length()) {
      char currentChar = input.charAt(position);

      if (currentChar == '@') {
        // Handle snap operation (@unit -> /unit)
        position = processSnap(input, position, result, currentReference);
      } else if (currentChar == '+' || currentChar == '-') {
        // Handle offset operation (+/-value[unit]) and update reference time
        OffsetResult offsetResult = processOffset(input, position, result, currentReference);
        position = offsetResult.newPosition;
        currentReference = offsetResult.updatedReference; // Update the reference time
      } else {
        throw new IllegalArgumentException(
            "Unexpected character '"
                + currentChar
                + "' at position "
                + position
                + " in input: "
                + input);
      }
    }
    return result.toString();
  }

  /** Helper class to return multiple values from processOffset */
  private record OffsetResult(int newPosition, ZonedDateTime updatedReference) {}

  /** Check if the input contains relative time operators (+ - @). */
  private static boolean containsRelativeTimeOperators(String input) {
    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (c == '+' || c == '-' || c == '@') {
        return true;
      }
    }
    return false;
  }

  /** Process a snap operation in the input string. Returns the new position after processing. */
  private static int processSnap(
      String input, int position, StringBuilder result, ZonedDateTime nowReference) {
    // Skip the '@' character
    int nextPosition = position + 1;

    // Extract the unit
    int endOfUnit = nextPosition;
    while (endOfUnit < input.length() && Character.isLetterOrDigit(input.charAt(endOfUnit))) {
      endOfUnit++;
    }

    String rawUnit = input.substring(nextPosition, endOfUnit);
    String normalizedUnit = normalizeUnit(rawUnit);

    // Special handling for quarter
    if ("q".equals(normalizedUnit)) {
      return processQuarterSnap(result, nowReference, endOfUnit);
    }

    // Special handling for week and week days
    // In PPL, plain @w is equivalent to @w0 (Sunday)
    if ("w".equals(normalizedUnit)) {
      return processWeekDaySnap(result, "w0", nowReference, endOfUnit);
    }
    // Special handling for week days (w0-w7)
    else if (normalizedUnit.matches("w[0-7]")) {
      return processWeekDaySnap(result, normalizedUnit, nowReference, endOfUnit);
    }

    String osUnit = convertToOsUnit(rawUnit);

    // Append the OpenSearch format (/unit)
    result.append('/').append(osUnit);

    return endOfUnit;
  }

  /**
   * Process a week day snap operation, mapping a normalized week day unit (w0-w7) to the OpenSearch
   * date math format.
   *
   * <p>PPL and OpenSearch have different week start conventions:
   *
   * <ul>
   *   <li>PPL: Week starts on Sunday (@w0 or @w7), @w1 is Monday, ..., @w6 is Saturday
   *   <li>OpenSearch: Week starts on Monday (/w)
   * </ul>
   *
   * <p>Additionally, PPL always aligns to the most recent occurrence of the specified day in the
   * past. This means the result will depend on the current reference day (nowReference).
   *
   * <p>Examples with different reference days:
   *
   * <ul>
   *   <li>Reference=Wednesday, Target=Tuesday: now/w+1d (Tuesday in current week)
   *   <li>Reference=Wednesday, Target=Thursday: now/w-4d (Thursday from previous week)
   *   <li>Reference=Sunday, Target=Monday: now/w (Align to last Monday)
   *   <li>Reference=Sunday, Target=Sunday: now/w+6d (PPL expects to align to today, so we align to
   *       Monday then plus 6 days)
   * </ul>
   */
  private static int processWeekDaySnap(
      StringBuilder result, String weekDay, ZonedDateTime nowReference, int endOfUnit) {
    int dayNumber;
    try {
      dayNumber = Integer.parseInt(weekDay.substring(1));
      if (dayNumber < 0 || dayNumber > 7) {
        throw new IllegalArgumentException("Invalid week day: " + weekDay);
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid week day format: " + weekDay);
    }

    result.append("/w");

    int dayAdjust = 0;
    int dayNumberReference = nowReference.getDayOfWeek().getValue();
    if (dayNumber == 0 || dayNumber == 7) {
      dayNumber = 7; // normalize to 7
      if (dayNumber == dayNumberReference) {
        // If aligning to Sunday on Sunday, align to Monday then plus 6 days
        dayAdjust = 6;
      } else {
        // Otherwise, align to Monday then subtract 1 day
        dayAdjust = -1;
      }
    } else if (dayNumber > 1) {
      // Other days are (dayNumber-1) days after Monday
      dayAdjust = dayNumber - 1;
      // If the expected day is greater than today, then align to this day in the last week.
      if (dayNumber > dayNumberReference) {
        dayAdjust -= 7; // Move to the previous week
      }
    }

    if (dayAdjust > 0) {
      result.append("+").append(dayAdjust).append("d");
    } else if (dayAdjust < 0) {
      result.append(dayAdjust).append("d");
    }

    return endOfUnit;
  }

  /** Process a quarter snap operation, aligning to the start of the current quarter. */
  private static int processQuarterSnap(
      StringBuilder result, ZonedDateTime nowReference, int endOfUnit) {
    // Calculate which month to snap to based on the current month
    int month = nowReference.getMonthValue();
    int quarterStartMonth = ((month - 1) / 3) * 3 + 1; // 1->1, 2->1, 3->1, 4->4, etc.
    int monthsToSubtract = month - quarterStartMonth;

    // Format for OpenSearch: now/M-NM where N is the number of months to subtract
    result.append("/M");
    if (monthsToSubtract > 0) {
      result.append("-").append(monthsToSubtract).append("M");
    }

    return endOfUnit;
  }

  /**
   * Process an offset operation in the input string. It returns an OffsetResult containing new
   * position and updated reference time
   */
  private static OffsetResult processOffset(
      String input, int position, StringBuilder result, ZonedDateTime nowReference) {
    // Get the sign (+ or -)
    char sign = input.charAt(position);
    result.append(sign);
    int nextPosition = position + 1;

    // Extract the value
    int endOfValue = nextPosition;
    while (endOfValue < input.length() && Character.isDigit(input.charAt(endOfValue))) {
      endOfValue++;
    }
    String valueStr = input.substring(nextPosition, endOfValue);
    int value = valueStr.isEmpty() ? 1 : Integer.parseInt(valueStr);
    result.append(value);

    // Extract the unit
    int endOfUnit = endOfValue;
    while (endOfUnit < input.length() && Character.isLetter(input.charAt(endOfUnit))) {
      endOfUnit++;
    }
    String rawUnit = input.substring(endOfValue, endOfUnit);
    String normalizedUnit = normalizeUnit(rawUnit);

    // Calculate the updated reference time based on the offset
    ZonedDateTime updatedReference =
        applyOffsetToReference(nowReference, sign, value, normalizedUnit);

    // Special handling for quarter
    if ("q".equals(normalizedUnit)) {
      int newPosition = processQuarterOffset(value, result, endOfUnit);
      return new OffsetResult(newPosition, updatedReference);
    }
    String osUnit = convertToOsUnit(rawUnit);
    result.append(osUnit);

    return new OffsetResult(endOfUnit, updatedReference);
  }

  /** Apply an offset with normalized unit to the reference time */
  private static ZonedDateTime applyOffsetToReference(
      ZonedDateTime reference, char sign, int value, String unit) {
    if ("q".equals(unit)) {
      // Convert quarters to months
      int months = value * 3;
      return sign == '+' ? reference.plusMonths(months) : reference.minusMonths(months);
    }

    ChronoUnit chronoUnit = mapChronoUnit(unit, "Unsupported offset unit: " + unit);

    return sign == '+' ? reference.plus(value, chronoUnit) : reference.minus(value, chronoUnit);
  }

  private static ChronoUnit mapChronoUnit(String unit, String s) {
    return switch (unit) {
      case "s" -> ChronoUnit.SECONDS;
      case "m" -> ChronoUnit.MINUTES;
      case "h" -> ChronoUnit.HOURS;
      case "d" -> ChronoUnit.DAYS;
      case "w" -> ChronoUnit.WEEKS;
      case "M" -> ChronoUnit.MONTHS;
      case "y" -> ChronoUnit.YEARS;
      default -> throw new IllegalArgumentException(s);
    };
  }

  /**
   * Process a quarter offset operation, converting it to months for OpenSearch. Returns the new
   * position after processing
   */
  private static int processQuarterOffset(int value, StringBuilder result, int endOfUnit) {
    // Convert quarters to months (1 quarter = 3 months)
    int months = value * 3;

    // We already added the sign and value to the result string in the calling method,
    // so we need to remove what was added before adding the correct value
    result.delete(result.length() - String.valueOf(value).length(), result.length());

    // Now append the correct value in months with the M unit
    result.append(months).append("M");

    return endOfUnit;
  }

  /** Convert PPL time unit to OpenSearch time unit. */
  private static String convertToOsUnit(String PPLUnit) {
    String normalizedUnit = normalizeUnit(PPLUnit);

    // Special handling for quarter
    if ("q".equals(normalizedUnit)) {
      // OpenSearch doesn't have native quarter support, use month (M) instead
      return "M";
    } else if ("M".equals(normalizedUnit)) {
      // Month is already correctly represented as 'M' in OpenSearch
      return normalizedUnit;
    } else {
      // For other units, use the normalized unit directly
      // Note: week day specifications (w0-w7) are handled separately in processWeekDaySnap
      return normalizedUnit;
    }
  }
}
