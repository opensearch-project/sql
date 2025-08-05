/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Dedicated span function implementation for bin command only. This is completely separate from the
 * aggregation span functionality to avoid shared infrastructure that could break customer queries.
 */
public class BinSpanFunction {

  /**
   * Creates a bin-specific time span expression for SPL-compatible time binning. This
   * implementation is completely separate from aggregation span functionality.
   */
  public static RexNode createBinTimeSpanExpression(
      RexNode fieldExpr,
      int intervalValue,
      String unit,
      long alignmentOffsetMillis,
      CalcitePlanContext context) {

    // Convert alignment offset from milliseconds to appropriate unit
    long alignmentInUnit = convertAlignmentOffset(alignmentOffsetMillis, unit);

    switch (unit.toLowerCase()) {
      case "us":
        return createMicrosecondsSpan(fieldExpr, intervalValue, alignmentInUnit, context);
      case "ms":
        return createMillisecondsSpan(fieldExpr, intervalValue, alignmentInUnit, context);
      case "cs":
        return createCentisecondsSpan(fieldExpr, intervalValue, alignmentInUnit, context);
      case "ds":
        return createDecisecondsSpan(fieldExpr, intervalValue, alignmentInUnit, context);
      case "s":
      case "sec":
      case "second":
      case "seconds":
        return createSecondsSpan(fieldExpr, intervalValue, alignmentInUnit, context);
      case "m":
      case "min":
      case "minute":
      case "minutes":
        return createMinutesSpan(fieldExpr, intervalValue, alignmentInUnit, context);
      case "h":
      case "hr":
      case "hour":
      case "hours":
        return createHoursSpan(fieldExpr, intervalValue, alignmentInUnit, context);
      case "d":
      case "day":
      case "days":
        return createDaysSpan(fieldExpr, intervalValue, context);
      case "M":
      case "mon":
      case "month":
      case "months":
        return createMonthsSpan(fieldExpr, intervalValue, context);
      case "y":
      case "yr":
      case "year":
      case "years":
        return createYearsSpan(fieldExpr, intervalValue, context);
      default:
        throw new IllegalArgumentException("Unsupported time unit for bin span: " + unit);
    }
  }

  /** Convert alignment offset from milliseconds to the target unit. */
  private static long convertAlignmentOffset(long offsetMillis, String unit) {
    if (offsetMillis == 0) {
      return 0;
    }

    return switch (unit.toLowerCase()) {
      case "us" -> offsetMillis * 1000L; // milliseconds to microseconds
      case "ms" -> offsetMillis; // already in milliseconds
      case "cs" -> offsetMillis / 10L; // milliseconds to centiseconds
      case "ds" -> offsetMillis / 100L; // milliseconds to deciseconds
      case "s", "sec", "second", "seconds" -> offsetMillis / 1000L;
      case "m", "min", "minute", "minutes" -> offsetMillis / (60 * 1000L);
      case "h", "hr", "hour", "hours" -> offsetMillis / (60 * 60 * 1000L);
      case "d", "day", "days" -> offsetMillis / (24 * 60 * 60 * 1000L);
      default -> 0; // For months/years, ignore alignment
    };
  }

  /** Create seconds-based span expression using bin-specific algorithm. */
  private static RexNode createSecondsSpan(
      RexNode fieldExpr, int intervalSeconds, long alignmentOffset, CalcitePlanContext context) {

    // Convert timestamp to epoch seconds
    RexNode epochSeconds =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);

    // Apply alignment offset
    RexNode adjustedSeconds = epochSeconds;
    if (alignmentOffset != 0) {
      adjustedSeconds =
          context.relBuilder.call(
              SqlStdOperatorTable.MINUS, epochSeconds, context.relBuilder.literal(alignmentOffset));
    }

    // Calculate bin: FLOOR(adjusted_seconds / interval) * interval
    RexNode intervalLiteral = context.relBuilder.literal(intervalSeconds);
    RexNode divided =
        context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjustedSeconds, intervalLiteral);
    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
    RexNode binSeconds =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, intervalLiteral);

    // Add back alignment offset
    if (alignmentOffset != 0) {
      binSeconds =
          context.relBuilder.call(
              SqlStdOperatorTable.PLUS, binSeconds, context.relBuilder.literal(alignmentOffset));
    }

    // Convert back to timestamp
    return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binSeconds);
  }

  /** Create minutes-based span expression using bin-specific algorithm. */
  private static RexNode createMinutesSpan(
      RexNode fieldExpr, int intervalMinutes, long alignmentOffset, CalcitePlanContext context) {

    // Convert to seconds and use seconds span
    return createSecondsSpan(fieldExpr, intervalMinutes * 60, alignmentOffset * 60, context);
  }

  /** Create hours-based span expression using bin-specific algorithm. */
  private static RexNode createHoursSpan(
      RexNode fieldExpr, int intervalHours, long alignmentOffset, CalcitePlanContext context) {

    // Convert to seconds and use seconds span
    return createSecondsSpan(fieldExpr, intervalHours * 3600, alignmentOffset * 3600, context);
  }

  /**
   * Create days-based span expression using bin-specific algorithm. Uses Unix epoch reference for
   * consistent alignment.
   */
  private static RexNode createDaysSpan(
      RexNode fieldExpr, int intervalDays, CalcitePlanContext context) {

    // Extract date part (ignoring time component)
    RexNode inputDate = context.rexBuilder.makeCall(PPLBuiltinOperators.DATE, fieldExpr);

    // Calculate days since Unix epoch using DATEDIFF
    RexNode epochDate = context.relBuilder.literal("1970-01-01");
    RexNode daysSinceEpoch =
        context.rexBuilder.makeCall(PPLBuiltinOperators.DATEDIFF, inputDate, epochDate);

    // Find bin using modular arithmetic
    RexNode intervalLiteral = context.relBuilder.literal(intervalDays);
    RexNode positionInCycle =
        context.relBuilder.call(SqlStdOperatorTable.MOD, daysSinceEpoch, intervalLiteral);
    RexNode binStartDays =
        context.relBuilder.call(SqlStdOperatorTable.MINUS, daysSinceEpoch, positionInCycle);

    // Convert back to date
    RexNode binStartDate =
        context.rexBuilder.makeCall(PPLBuiltinOperators.ADDDATE, epochDate, binStartDays);

    // Convert to timestamp at midnight
    return context.rexBuilder.makeCall(PPLBuiltinOperators.TIMESTAMP, binStartDate);
  }

  /**
   * Create months-based span expression using bin-specific algorithm. Uses Unix epoch reference for
   * consistent alignment.
   */
  private static RexNode createMonthsSpan(
      RexNode fieldExpr, int intervalMonths, CalcitePlanContext context) {

    // Extract date part
    RexNode inputDate = context.rexBuilder.makeCall(PPLBuiltinOperators.DATE, fieldExpr);

    // Calculate months since Unix epoch (January 1970)
    RexNode inputYear = context.rexBuilder.makeCall(PPLBuiltinOperators.YEAR, inputDate);
    RexNode inputMonth = context.rexBuilder.makeCall(PPLBuiltinOperators.MONTH, inputDate);

    // months_since_epoch = (year - 1970) * 12 + (month - 1)
    RexNode yearsSinceEpoch =
        context.relBuilder.call(
            SqlStdOperatorTable.MINUS, inputYear, context.relBuilder.literal(1970));
    RexNode monthsFromYears =
        context.relBuilder.call(
            SqlStdOperatorTable.MULTIPLY, yearsSinceEpoch, context.relBuilder.literal(12));
    RexNode monthsSinceEpoch =
        context.relBuilder.call(
            SqlStdOperatorTable.PLUS,
            monthsFromYears,
            context.relBuilder.call(
                SqlStdOperatorTable.MINUS, inputMonth, context.relBuilder.literal(1)));

    // Find bin using modular arithmetic
    RexNode intervalLiteral = context.relBuilder.literal(intervalMonths);
    RexNode positionInCycle =
        context.relBuilder.call(SqlStdOperatorTable.MOD, monthsSinceEpoch, intervalLiteral);
    RexNode binStartMonths =
        context.relBuilder.call(SqlStdOperatorTable.MINUS, monthsSinceEpoch, positionInCycle);

    // Convert back to year and month
    RexNode binStartYear =
        context.relBuilder.call(
            SqlStdOperatorTable.PLUS,
            context.relBuilder.literal(1970),
            context.relBuilder.call(
                SqlStdOperatorTable.DIVIDE, binStartMonths, context.relBuilder.literal(12)));
    RexNode binStartMonth =
        context.relBuilder.call(
            SqlStdOperatorTable.PLUS,
            context.relBuilder.call(
                SqlStdOperatorTable.MOD, binStartMonths, context.relBuilder.literal(12)),
            context.relBuilder.literal(1));

    // Create first day of the month as timestamp
    RexNode firstDay = context.relBuilder.literal(1);
    RexNode binDate =
        context.rexBuilder.makeCall(
            PPLBuiltinOperators.MAKEDATE, binStartYear, binStartMonth, firstDay);

    return context.rexBuilder.makeCall(PPLBuiltinOperators.TIMESTAMP, binDate);
  }

  /** Create years-based span expression using bin-specific algorithm. */
  private static RexNode createYearsSpan(
      RexNode fieldExpr, int intervalYears, CalcitePlanContext context) {

    // Extract year from input
    RexNode inputDate = context.rexBuilder.makeCall(PPLBuiltinOperators.DATE, fieldExpr);
    RexNode inputYear = context.rexBuilder.makeCall(PPLBuiltinOperators.YEAR, inputDate);

    // Calculate years since Unix epoch (1970)
    RexNode yearsSinceEpoch =
        context.relBuilder.call(
            SqlStdOperatorTable.MINUS, inputYear, context.relBuilder.literal(1970));

    // Find bin using modular arithmetic
    RexNode intervalLiteral = context.relBuilder.literal(intervalYears);
    RexNode positionInCycle =
        context.relBuilder.call(SqlStdOperatorTable.MOD, yearsSinceEpoch, intervalLiteral);
    RexNode binStartYears =
        context.relBuilder.call(SqlStdOperatorTable.MINUS, yearsSinceEpoch, positionInCycle);

    // Convert back to year
    RexNode binStartYear =
        context.relBuilder.call(
            SqlStdOperatorTable.PLUS, context.relBuilder.literal(1970), binStartYears);

    // Create January 1st of the bin year as timestamp
    RexNode january = context.relBuilder.literal(1);
    RexNode firstDay = context.relBuilder.literal(1);
    RexNode binDate =
        context.rexBuilder.makeCall(PPLBuiltinOperators.MAKEDATE, binStartYear, january, firstDay);

    return context.rexBuilder.makeCall(PPLBuiltinOperators.TIMESTAMP, binDate);
  }

  /**
   * Utility method to round timestamp using bin-specific floor division algorithm. This replaces
   * the shared DateTimeUtils.roundFloor method for bin command.
   */
  public static long binRoundFloor(long utcMillis, long unitMillis) {
    return utcMillis - utcMillis % unitMillis;
  }

  /**
   * Utility method to round timestamp in weeks using bin-specific algorithm. This replaces the
   * shared DateTimeUtils.roundWeek method for bin command.
   */
  public static long binRoundWeek(long utcMillis, int interval) {
    return binRoundFloor(utcMillis + 259200000L, 604800000L * interval) - 259200000L;
  }

  /**
   * Utility method to round timestamp in months using bin-specific algorithm. This replaces the
   * shared DateTimeUtils.roundMonth method for bin command.
   */
  public static long binRoundMonth(long utcMillis, int interval) {
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime zonedDateTime = Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC);

    // Calculate total months since epoch
    long totalMonths =
        (zonedDateTime.getYear() - 1970L) * 12L + (zonedDateTime.getMonthValue() - 1);

    // Find bin start using modular arithmetic
    long binStartMonths = totalMonths - (totalMonths % interval);

    // Convert back to timestamp
    return initDateTime.plusMonths(binStartMonths).toInstant().toEpochMilli();
  }

  /**
   * Utility method to round timestamp in years using bin-specific algorithm. This replaces the
   * shared DateTimeUtils.roundYear method for bin command.
   */
  public static long binRoundYear(long utcMillis, int interval) {
    ZonedDateTime initDateTime = ZonedDateTime.of(1970, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
    ZonedDateTime zonedDateTime = Instant.ofEpochMilli(utcMillis).atZone(ZoneOffset.UTC);

    // Calculate years since epoch
    int yearsSinceEpoch = zonedDateTime.getYear() - 1970;

    // Find bin start using modular arithmetic
    int binStartYears = yearsSinceEpoch - (yearsSinceEpoch % interval);

    return initDateTime.plusYears(binStartYears).toInstant().toEpochMilli();
  }

  /** Create microseconds-based span expression using bin-specific algorithm. */
  private static RexNode createMicrosecondsSpan(
      RexNode fieldExpr,
      int intervalMicroseconds,
      long alignmentOffset,
      CalcitePlanContext context) {

    // Convert timestamp to epoch microseconds (multiply milliseconds by 1000)
    RexNode epochMillis =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);
    RexNode epochMicros =
        context.relBuilder.call(
            SqlStdOperatorTable.MULTIPLY, epochMillis, context.relBuilder.literal(1000L));

    // Apply alignment offset
    RexNode adjustedMicros = epochMicros;
    if (alignmentOffset != 0) {
      adjustedMicros =
          context.relBuilder.call(
              SqlStdOperatorTable.MINUS, epochMicros, context.relBuilder.literal(alignmentOffset));
    }

    // Calculate bin: FLOOR(adjusted_micros / interval) * interval
    RexNode intervalLiteral = context.relBuilder.literal(intervalMicroseconds);
    RexNode divided =
        context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjustedMicros, intervalLiteral);
    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
    RexNode binMicros =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, intervalLiteral);

    // Add back alignment offset
    if (alignmentOffset != 0) {
      binMicros =
          context.relBuilder.call(
              SqlStdOperatorTable.PLUS, binMicros, context.relBuilder.literal(alignmentOffset));
    }

    // Convert back to milliseconds and then to timestamp
    RexNode binMillis =
        context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE, binMicros, context.relBuilder.literal(1000L));
    return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binMillis);
  }

  /** Create milliseconds-based span expression using bin-specific algorithm. */
  private static RexNode createMillisecondsSpan(
      RexNode fieldExpr,
      int intervalMilliseconds,
      long alignmentOffset,
      CalcitePlanContext context) {

    // Convert timestamp to epoch milliseconds
    RexNode epochMillis =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);

    // Apply alignment offset
    RexNode adjustedMillis = epochMillis;
    if (alignmentOffset != 0) {
      adjustedMillis =
          context.relBuilder.call(
              SqlStdOperatorTable.MINUS, epochMillis, context.relBuilder.literal(alignmentOffset));
    }

    // Calculate bin: FLOOR(adjusted_millis / interval) * interval
    RexNode intervalLiteral = context.relBuilder.literal(intervalMilliseconds);
    RexNode divided =
        context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjustedMillis, intervalLiteral);
    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
    RexNode binMillis =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, intervalLiteral);

    // Add back alignment offset
    if (alignmentOffset != 0) {
      binMillis =
          context.relBuilder.call(
              SqlStdOperatorTable.PLUS, binMillis, context.relBuilder.literal(alignmentOffset));
    }

    // Convert back to timestamp
    return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binMillis);
  }

  /** Create centiseconds-based span expression using bin-specific algorithm. */
  private static RexNode createCentisecondsSpan(
      RexNode fieldExpr,
      int intervalCentiseconds,
      long alignmentOffset,
      CalcitePlanContext context) {

    // Convert timestamp to epoch centiseconds (multiply milliseconds by 0.1)
    RexNode epochMillis =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);
    RexNode epochCentis =
        context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE, epochMillis, context.relBuilder.literal(10L));

    // Apply alignment offset
    RexNode adjustedCentis = epochCentis;
    if (alignmentOffset != 0) {
      adjustedCentis =
          context.relBuilder.call(
              SqlStdOperatorTable.MINUS, epochCentis, context.relBuilder.literal(alignmentOffset));
    }

    // Calculate bin: FLOOR(adjusted_centis / interval) * interval
    RexNode intervalLiteral = context.relBuilder.literal(intervalCentiseconds);
    RexNode divided =
        context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjustedCentis, intervalLiteral);
    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
    RexNode binCentis =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, intervalLiteral);

    // Add back alignment offset
    if (alignmentOffset != 0) {
      binCentis =
          context.relBuilder.call(
              SqlStdOperatorTable.PLUS, binCentis, context.relBuilder.literal(alignmentOffset));
    }

    // Convert back to milliseconds and then to timestamp
    RexNode binMillis =
        context.relBuilder.call(
            SqlStdOperatorTable.MULTIPLY, binCentis, context.relBuilder.literal(10L));
    return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binMillis);
  }

  /** Create deciseconds-based span expression using bin-specific algorithm. */
  private static RexNode createDecisecondsSpan(
      RexNode fieldExpr,
      int intervalDeciseconds,
      long alignmentOffset,
      CalcitePlanContext context) {

    // Convert timestamp to epoch deciseconds (multiply milliseconds by 0.01)
    RexNode epochMillis =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);
    RexNode epochDecis =
        context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE, epochMillis, context.relBuilder.literal(100L));

    // Apply alignment offset
    RexNode adjustedDecis = epochDecis;
    if (alignmentOffset != 0) {
      adjustedDecis =
          context.relBuilder.call(
              SqlStdOperatorTable.MINUS, epochDecis, context.relBuilder.literal(alignmentOffset));
    }

    // Calculate bin: FLOOR(adjusted_decis / interval) * interval
    RexNode intervalLiteral = context.relBuilder.literal(intervalDeciseconds);
    RexNode divided =
        context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjustedDecis, intervalLiteral);
    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
    RexNode binDecis =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, intervalLiteral);

    // Add back alignment offset
    if (alignmentOffset != 0) {
      binDecis =
          context.relBuilder.call(
              SqlStdOperatorTable.PLUS, binDecis, context.relBuilder.literal(alignmentOffset));
    }

    // Convert back to milliseconds and then to timestamp
    RexNode binMillis =
        context.relBuilder.call(
            SqlStdOperatorTable.MULTIPLY, binDecis, context.relBuilder.literal(100L));
    return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binMillis);
  }
}
