/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.utils;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_DATE;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIME;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT.EXPR_TIMESTAMP;

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
import java.util.Objects;
import java.util.regex.Pattern;
import lombok.experimental.UtilityClass;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.model.ExprTimeValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.function.FunctionProperties;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

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

    ChronoUnit chronoUnit =
        switch (unit) {
          case "s" -> ChronoUnit.SECONDS;
          case "m" -> ChronoUnit.MINUTES;
          case "h" -> ChronoUnit.HOURS;
          case "d" -> ChronoUnit.DAYS;
          case "w" -> ChronoUnit.WEEKS;
          case "M" -> ChronoUnit.MONTHS;
          case "y" -> ChronoUnit.YEARS;
          default -> throw new IllegalArgumentException("Unsupported offset unit: " + rawUnit);
        };

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

  public static RexNode transferCompareForDateRelated(
      RexNode candidate, CalcitePlanContext context, SqlTypeName castTarget) {
    if (!(Objects.isNull(castTarget))) {
      switch (castTarget) {
        case DATE:
          if (!(candidate.getType() instanceof ExprSqlType
              && ((ExprSqlType) candidate.getType()).getUdt() == EXPR_DATE)) {
            return context.rexBuilder.makeCall(PPLBuiltinOperators.DATE, candidate);
          }
          break;
        case TIME:
          if (!(candidate.getType() instanceof ExprSqlType
              && ((ExprSqlType) candidate.getType()).getUdt() == EXPR_TIME)) {
            return context.rexBuilder.makeCall(PPLBuiltinOperators.TIME, candidate);
          }
          break;
        case TIMESTAMP:
          if (!(candidate.getType() instanceof ExprSqlType
              && ((ExprSqlType) candidate.getType()).getUdt() == EXPR_TIMESTAMP)) {
            return context.rexBuilder.makeCall(PPLBuiltinOperators.TIMESTAMP, candidate);
          }
          break;
        default:
          return candidate;
      }
    }
    return candidate;
  }

  public static SqlTypeName findCastType(RexNode left, RexNode right) {
    SqlTypeName leftType = returnCorrespondingSqlType(left);
    SqlTypeName rightType = returnCorrespondingSqlType(right);
    if (leftType != null && rightType != null) {
      return SqlTypeName.TIMESTAMP;
    }
    return leftType == null ? rightType : leftType;
  }

  public static SqlTypeName returnCorrespondingSqlType(RexNode node) {
    if (node.getType() instanceof ExprSqlType) {
      OpenSearchTypeFactory.ExprUDT udt = ((ExprSqlType) node.getType()).getUdt();
      return switch (udt) {
        case EXPR_DATE -> SqlTypeName.DATE;
        case EXPR_TIME -> SqlTypeName.TIME;
        case EXPR_TIMESTAMP -> SqlTypeName.TIMESTAMP;
        default -> null;
      };
    }
    return null;
  }
}
