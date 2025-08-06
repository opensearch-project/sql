/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.Map;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/**
 * Dedicated span function implementation for bin command only. This is completely separate from the
 * aggregation span functionality to avoid shared infrastructure that could break customer queries.
 */
public class BinSpanFunction {

  // Time unit conversion constants
  private static final long MILLIS_PER_SECOND = 1000L;
  private static final long MILLIS_PER_MINUTE = 60 * MILLIS_PER_SECOND;
  private static final long MILLIS_PER_HOUR = 60 * MILLIS_PER_MINUTE;
  private static final long MILLIS_PER_DAY = 24 * MILLIS_PER_HOUR;

  // Sub-second unit conversion constants
  private static final long MICROS_PER_MILLI = 1000L;
  private static final long MILLIS_PER_CENTISECOND = 10L;
  private static final long MILLIS_PER_DECISECOND = 100L;

  // Historical reference points
  private static final int UNIX_EPOCH_YEAR = 1970;
  private static final String UNIX_EPOCH_DATE = "1970-01-01";

  /** Time unit configuration for different span types. */
  private enum TimeUnitConfig {
    MICROSECONDS("us", 1, MICROS_PER_MILLI, true),
    MILLISECONDS("ms", 1, 1, true),
    CENTISECONDS("cs", 1, MILLIS_PER_CENTISECOND, true),
    DECISECONDS("ds", 1, MILLIS_PER_DECISECOND, true),
    SECONDS("s", (int) MILLIS_PER_SECOND, 1, true),
    MINUTES("m", (int) MILLIS_PER_MINUTE, 1, true),
    HOURS("h", (int) MILLIS_PER_HOUR, 1, true),
    DAYS("d", (int) MILLIS_PER_DAY, 1, false),
    MONTHS("M", 0, 1, false); // Special handling

    final String unit;
    final int multiplierSeconds;
    final long divisionFactor;
    final boolean supportsAlignment;

    TimeUnitConfig(
        String unit, int multiplierSeconds, long divisionFactor, boolean supportsAlignment) {
      this.unit = unit;
      this.multiplierSeconds = multiplierSeconds;
      this.divisionFactor = divisionFactor;
      this.supportsAlignment = supportsAlignment;
    }
  }

  // Unit mapping for all supported variations
  private static final Map<String, TimeUnitConfig> UNIT_MAPPING =
      Map.ofEntries(
          // Microseconds
          Map.entry("us", TimeUnitConfig.MICROSECONDS),

          // Milliseconds
          Map.entry("ms", TimeUnitConfig.MILLISECONDS),

          // Centiseconds
          Map.entry("cs", TimeUnitConfig.CENTISECONDS),

          // Deciseconds
          Map.entry("ds", TimeUnitConfig.DECISECONDS),

          // Seconds
          Map.entry("s", TimeUnitConfig.SECONDS),
          Map.entry("sec", TimeUnitConfig.SECONDS),
          Map.entry("second", TimeUnitConfig.SECONDS),
          Map.entry("seconds", TimeUnitConfig.SECONDS),

          // Minutes
          Map.entry("m", TimeUnitConfig.MINUTES),
          Map.entry("min", TimeUnitConfig.MINUTES),
          Map.entry("minute", TimeUnitConfig.MINUTES),
          Map.entry("minutes", TimeUnitConfig.MINUTES),

          // Hours
          Map.entry("h", TimeUnitConfig.HOURS),
          Map.entry("hr", TimeUnitConfig.HOURS),
          Map.entry("hour", TimeUnitConfig.HOURS),
          Map.entry("hours", TimeUnitConfig.HOURS),

          // Days
          Map.entry("d", TimeUnitConfig.DAYS),
          Map.entry("day", TimeUnitConfig.DAYS),
          Map.entry("days", TimeUnitConfig.DAYS),

          // Months (case-sensitive M)
          Map.entry("M", TimeUnitConfig.MONTHS),
          Map.entry("mon", TimeUnitConfig.MONTHS),
          Map.entry("month", TimeUnitConfig.MONTHS),
          Map.entry("months", TimeUnitConfig.MONTHS));

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

    TimeUnitConfig config = UNIT_MAPPING.get(unit.toLowerCase());
    if (config == null && !unit.equals("M")) { // M is case-sensitive for months
      config = UNIT_MAPPING.get(unit);
    }

    if (config == null) {
      throw new IllegalArgumentException("Unsupported time unit for bin span: " + unit);
    }

    return switch (config) {
      case MICROSECONDS,
          MILLISECONDS,
          CENTISECONDS,
          DECISECONDS,
          SECONDS,
          MINUTES,
          HOURS -> createStandardTimeSpan(
          fieldExpr, intervalValue, config, alignmentOffsetMillis, context);
      case DAYS -> createDaysSpan(fieldExpr, intervalValue, context);
      case MONTHS -> createMonthsSpan(fieldExpr, intervalValue, context);
    };
  }

  /**
   * Creates span expressions for standard time units (microseconds through hours) using a unified
   * approach.
   */
  private static RexNode createStandardTimeSpan(
      RexNode fieldExpr,
      int intervalValue,
      TimeUnitConfig config,
      long alignmentOffsetMillis,
      CalcitePlanContext context) {

    // Convert timestamp to the target unit
    RexNode epochValue = convertToTargetUnit(fieldExpr, config, context);

    // Convert and apply alignment offset
    long alignmentOffset = convertAlignmentOffset(alignmentOffsetMillis, config);
    RexNode adjustedValue = applyAlignmentOffset(epochValue, alignmentOffset, context);

    // Perform binning calculation: FLOOR(adjusted_value / interval) * interval
    RexNode binValue = performBinning(adjustedValue, intervalValue, context);

    // Add back alignment offset
    if (alignmentOffset != 0) {
      binValue =
          context.relBuilder.call(
              SqlStdOperatorTable.PLUS, binValue, context.relBuilder.literal(alignmentOffset));
    }

    // Convert back to timestamp
    return convertFromTargetUnit(binValue, config, context);
  }

  /** Converts timestamp to the target time unit. */
  private static RexNode convertToTargetUnit(
      RexNode fieldExpr, TimeUnitConfig config, CalcitePlanContext context) {
    RexNode epochMillis =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);

    if (config.divisionFactor == 1) {
      return epochMillis;
    } else if (config.divisionFactor > 1) {
      // For sub-second units or conversion to larger units
      return context.relBuilder.call(
          SqlStdOperatorTable.DIVIDE,
          epochMillis,
          context.relBuilder.literal(config.divisionFactor));
    } else {
      // For microseconds (multiply by 1000)
      return context.relBuilder.call(
          SqlStdOperatorTable.MULTIPLY, epochMillis, context.relBuilder.literal(MICROS_PER_MILLI));
    }
  }

  /** Converts from target unit back to timestamp. */
  private static RexNode convertFromTargetUnit(
      RexNode binValue, TimeUnitConfig config, CalcitePlanContext context) {
    RexNode binMillis;

    if (config.divisionFactor == 1) {
      binMillis = binValue;
    } else if (config.divisionFactor > 1) {
      binMillis =
          context.relBuilder.call(
              SqlStdOperatorTable.MULTIPLY,
              binValue,
              context.relBuilder.literal(config.divisionFactor));
    } else {
      // For microseconds (divide by 1000)
      binMillis =
          context.relBuilder.call(
              SqlStdOperatorTable.DIVIDE, binValue, context.relBuilder.literal(MICROS_PER_MILLI));
    }

    return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binMillis);
  }

  /** Applies alignment offset to the epoch value. */
  private static RexNode applyAlignmentOffset(
      RexNode epochValue, long alignmentOffset, CalcitePlanContext context) {
    if (alignmentOffset == 0) {
      return epochValue;
    }
    return context.relBuilder.call(
        SqlStdOperatorTable.MINUS, epochValue, context.relBuilder.literal(alignmentOffset));
  }

  /** Performs the core binning calculation: FLOOR(value / interval) * interval */
  private static RexNode performBinning(
      RexNode adjustedValue, int intervalValue, CalcitePlanContext context) {
    RexNode intervalLiteral = context.relBuilder.literal(intervalValue);
    RexNode divided =
        context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjustedValue, intervalLiteral);
    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
    return context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, intervalLiteral);
  }

  /** Converts alignment offset from milliseconds to the target unit. */
  private static long convertAlignmentOffset(long offsetMillis, TimeUnitConfig config) {
    if (offsetMillis == 0 || !config.supportsAlignment) {
      return 0;
    }

    return switch (config) {
      case MICROSECONDS -> offsetMillis * MICROS_PER_MILLI;
      case MILLISECONDS -> offsetMillis;
      case CENTISECONDS -> offsetMillis / MILLIS_PER_CENTISECOND;
      case DECISECONDS -> offsetMillis / MILLIS_PER_DECISECOND;
      case SECONDS -> offsetMillis / MILLIS_PER_SECOND;
      case MINUTES -> offsetMillis / MILLIS_PER_MINUTE;
      case HOURS -> offsetMillis / MILLIS_PER_HOUR;
      default -> 0;
    };
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
    RexNode epochDate = context.relBuilder.literal(UNIX_EPOCH_DATE);
    RexNode daysSinceEpoch =
        context.rexBuilder.makeCall(PPLBuiltinOperators.DATEDIFF, inputDate, epochDate);

    // Find bin using modular arithmetic
    RexNode binStartDays = calculateBinStart(daysSinceEpoch, intervalDays, context);

    // Convert back to timestamp at midnight
    RexNode binStartDate =
        context.rexBuilder.makeCall(PPLBuiltinOperators.ADDDATE, epochDate, binStartDays);
    return context.rexBuilder.makeCall(PPLBuiltinOperators.TIMESTAMP, binStartDate);
  }

  /**
   * Create months-based span expression using bin-specific algorithm. Uses Unix epoch reference for
   * consistent alignment.
   */
  private static RexNode createMonthsSpan(
      RexNode fieldExpr, int intervalMonths, CalcitePlanContext context) {

    // Extract date components
    RexNode inputDate = context.rexBuilder.makeCall(PPLBuiltinOperators.DATE, fieldExpr);
    RexNode inputYear = context.rexBuilder.makeCall(PPLBuiltinOperators.YEAR, inputDate);
    RexNode inputMonth = context.rexBuilder.makeCall(PPLBuiltinOperators.MONTH, inputDate);

    // Calculate months since epoch: (year - 1970) * 12 + (month - 1)
    RexNode monthsSinceEpoch = calculateMonthsSinceEpoch(inputYear, inputMonth, context);

    // Find bin using modular arithmetic
    RexNode binStartMonths = calculateBinStart(monthsSinceEpoch, intervalMonths, context);

    // Convert back to year and month
    RexNode binStartYear = calculateBinStartYear(binStartMonths, context);
    RexNode binStartMonth = calculateBinStartMonth(binStartMonths, context);

    // Create first day of the month as timestamp
    RexNode binDate =
        context.rexBuilder.makeCall(
            PPLBuiltinOperators.MAKEDATE,
            binStartYear,
            binStartMonth,
            context.relBuilder.literal(1));

    return context.rexBuilder.makeCall(PPLBuiltinOperators.TIMESTAMP, binDate);
  }

  // === HELPER METHODS FOR CALCULATIONS ===

  /** Generic bin start calculation using modular arithmetic: value - (value % interval) */
  private static RexNode calculateBinStart(
      RexNode value, int interval, CalcitePlanContext context) {
    RexNode intervalLiteral = context.relBuilder.literal(interval);
    RexNode positionInCycle =
        context.relBuilder.call(SqlStdOperatorTable.MOD, value, intervalLiteral);
    return context.relBuilder.call(SqlStdOperatorTable.MINUS, value, positionInCycle);
  }

  /** Calculate months since Unix epoch: (year - 1970) * 12 + (month - 1) */
  private static RexNode calculateMonthsSinceEpoch(
      RexNode inputYear, RexNode inputMonth, CalcitePlanContext context) {
    RexNode yearsSinceEpoch =
        context.relBuilder.call(
            SqlStdOperatorTable.MINUS, inputYear, context.relBuilder.literal(UNIX_EPOCH_YEAR));
    RexNode monthsFromYears =
        context.relBuilder.call(
            SqlStdOperatorTable.MULTIPLY, yearsSinceEpoch, context.relBuilder.literal(12));
    return context.relBuilder.call(
        SqlStdOperatorTable.PLUS,
        monthsFromYears,
        context.relBuilder.call(
            SqlStdOperatorTable.MINUS, inputMonth, context.relBuilder.literal(1)));
  }

  /** Calculate bin start year from months since epoch: 1970 + (binStartMonths / 12) */
  private static RexNode calculateBinStartYear(RexNode binStartMonths, CalcitePlanContext context) {
    return context.relBuilder.call(
        SqlStdOperatorTable.PLUS,
        context.relBuilder.literal(UNIX_EPOCH_YEAR),
        context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE, binStartMonths, context.relBuilder.literal(12)));
  }

  /** Calculate bin start month from months since epoch: (binStartMonths % 12) + 1 */
  private static RexNode calculateBinStartMonth(
      RexNode binStartMonths, CalcitePlanContext context) {
    return context.relBuilder.call(
        SqlStdOperatorTable.PLUS,
        context.relBuilder.call(
            SqlStdOperatorTable.MOD, binStartMonths, context.relBuilder.literal(12)),
        context.relBuilder.literal(1));
  }
}
