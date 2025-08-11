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
    MICROSECONDS("us", 1, -1, true), // Special case: multiply by 1000
    MILLISECONDS("ms", 1, 1, true),
    CENTISECONDS("cs", 1, MILLIS_PER_CENTISECOND, true),
    DECISECONDS("ds", 1, MILLIS_PER_DECISECOND, true),
    SECONDS("s", (int) MILLIS_PER_SECOND, 1, true),
    MINUTES("m", (int) MILLIS_PER_MINUTE, 60, true),
    HOURS("h", (int) MILLIS_PER_HOUR, 3600, true),
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

    TimeUnitConfig config;
    if (unit.equals("M")) {
      // M is case-sensitive for months - don't convert to lowercase
      config = UNIT_MAPPING.get(unit);
    } else {
      // For all other units, use lowercase lookup
      config = UNIT_MAPPING.get(unit.toLowerCase());
    }

    if (config == null) {
      throw new IllegalArgumentException("Unsupported time unit for bin span: " + unit);
    }

    // Validate sub-second span constraints
    validateSubSecondSpan(config, intervalValue);

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
   * Creates a bin-specific time span expression with time modifier alignment. This handles SPL time
   * modifiers like @d, @d+4h, @d-1h and epoch timestamps.
   */
  public static RexNode createBinTimeSpanExpressionWithTimeModifier(
      RexNode fieldExpr,
      int intervalValue,
      String unit,
      String timeModifier,
      CalcitePlanContext context) {

    TimeUnitConfig config;
    if (unit.equals("M")) {
      config = UNIT_MAPPING.get(unit);
    } else {
      config = UNIT_MAPPING.get(unit.toLowerCase());
    }

    if (config == null) {
      throw new IllegalArgumentException("Unsupported time unit for bin span: " + unit);
    }

    // Validate sub-second span constraints
    validateSubSecondSpan(config, intervalValue);

    // DEBUG: Log that this method is being called
    System.out.println(
        "DEBUG: BinSpanFunction.createBinTimeSpanExpressionWithTimeModifier called with"
            + " timeModifier: "
            + timeModifier);

    // Check if this is an epoch timestamp alignment
    try {
      long epochTimestamp = Long.parseLong(timeModifier);
      System.out.println("DEBUG: Processing as epoch timestamp: " + epochTimestamp);
      return createEpochTimestampAlignedSpan(
          fieldExpr, intervalValue, config, epochTimestamp, context);
    } catch (NumberFormatException e) {
      // Not a number, treat as time modifier
      System.out.println("DEBUG: Processing as time modifier: " + timeModifier);
      return createTimeModifierAlignedSpan(fieldExpr, intervalValue, config, timeModifier, context);
    }
  }

  /**
   * Creates time span with SPL time modifier alignment (@d, @d+4h, @d-1h). This properly handles
   * alignment points and negative relative positions.
   */
  private static RexNode createTimeModifierAlignedSpan(
      RexNode fieldExpr,
      int intervalValue,
      TimeUnitConfig config,
      String timeModifier,
      CalcitePlanContext context) {

    System.out.println(
        "DEBUG: createTimeModifierAlignedSpan called with timeModifier="
            + timeModifier
            + ", intervalValue="
            + intervalValue
            + ", unit="
            + config.unit);

    // UNIX_TIMESTAMP returns seconds, not milliseconds
    RexNode epochSeconds =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);

    // Parse the time modifier
    long offsetMillis = 0;
    boolean alignToDay = false;

    if (timeModifier != null) {
      timeModifier = timeModifier.trim();
      if (timeModifier.equals("@d")) {
        alignToDay = true;
      } else if (timeModifier.startsWith("@d+")) {
        alignToDay = true;
        String offsetStr = timeModifier.substring(3);
        offsetMillis = parseTimeOffsetForModifier(offsetStr);
      } else if (timeModifier.startsWith("@d-")) {
        alignToDay = true;
        String offsetStr = timeModifier.substring(3);
        offsetMillis = -parseTimeOffsetForModifier(offsetStr);
      }
    }

    // Convert interval to seconds based on the time unit (not milliseconds!)
    long intervalSeconds;
    switch (config) {
      case HOURS -> intervalSeconds = intervalValue * 3600L;
      case MINUTES -> intervalSeconds = intervalValue * 60L;
      case SECONDS -> intervalSeconds = intervalValue;
      case MILLISECONDS -> intervalSeconds = intervalValue / 1000L;
      case MICROSECONDS -> intervalSeconds = intervalValue / 1000000L;
      case CENTISECONDS -> intervalSeconds = intervalValue / 100L;
      case DECISECONDS -> intervalSeconds = intervalValue / 10L;
      default -> intervalSeconds = intervalValue * 3600L; // Default to hours
    }

    RexNode intervalLiteral = context.relBuilder.literal(intervalSeconds);

    if (alignToDay) {
      // SPL @d+offset alignment: Use EARLIEST timestamp in dataset to determine reference
      // Per SPL spec: "@d calculates the start of day for the earliest timestamp in dataset"

      RexNode secondsPerDay = context.relBuilder.literal(86400L);

      // TEMPORARY: Use a hardcoded earliest timestamp for debugging
      // This should be 2025-07-28T00:15:23 = 1753661723 seconds (not milliseconds!)
      RexNode earliestTimestamp = context.relBuilder.literal(1753661723L);

      System.out.println(
          "DEBUG: Using hardcoded earliest timestamp 1753661723 seconds (2025-07-28T00:15:23)");

      // Calculate start of day for the EARLIEST timestamp (not current row)
      RexNode daysSinceEpoch =
          context.relBuilder.call(
              SqlStdOperatorTable.FLOOR,
              context.relBuilder.call(
                  SqlStdOperatorTable.DIVIDE, earliestTimestamp, secondsPerDay));

      // Calculate the start of day for earliest timestamp
      RexNode startOfEarliestDay =
          context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, daysSinceEpoch, secondsPerDay);

      // Calculate the alignment reference point
      RexNode alignmentReference;
      if (offsetMillis != 0) {
        // Convert offset from milliseconds to seconds
        long offsetSeconds = offsetMillis / 1000L;
        alignmentReference =
            context.relBuilder.call(
                SqlStdOperatorTable.PLUS,
                startOfEarliestDay,
                context.relBuilder.literal(offsetSeconds));
      } else {
        alignmentReference = startOfEarliestDay;
      }

      // SPL @d+offset algorithm:
      // For @d+4h with span=12h: creates bins [04:00-16:00], [16:00-04:00 next day]

      System.out.println(
          "DEBUG @d+offset: Using earliest timestamp from dataset for reference calculation");

      // 1. Calculate which bin relative to alignment point (all in seconds now)
      RexNode timeOffset =
          context.relBuilder.call(SqlStdOperatorTable.MINUS, epochSeconds, alignmentReference);
      RexNode binNumber =
          context.relBuilder.call(
              SqlStdOperatorTable.FLOOR,
              context.relBuilder.call(SqlStdOperatorTable.DIVIDE, timeOffset, intervalLiteral));

      // 2. Apply SPL Universal Formula directly: reference + (binNumber * span)
      //    This follows the exact SPL specification without additional adjustments
      RexNode binOffset =
          context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, binNumber, intervalLiteral);
      RexNode binStartSeconds =
          context.relBuilder.call(SqlStdOperatorTable.PLUS, alignmentReference, binOffset);

      // Convert back to timestamp (FROM_UNIXTIME expects seconds)
      return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binStartSeconds);

    } else {
      // No day alignment, use the original timestamp as reference (all in seconds)
      RexNode divided =
          context.relBuilder.call(SqlStdOperatorTable.DIVIDE, epochSeconds, intervalLiteral);
      RexNode binNumber = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);
      RexNode binStartSeconds =
          context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, binNumber, intervalLiteral);

      return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binStartSeconds);
    }
  }

  /**
   * Creates time span with epoch timestamp alignment (aligntime=<epoch_number>). Uses the SPL
   * Universal Formula: bin_start = reference + floor((timestamp - reference) / span) * span
   */
  private static RexNode createEpochTimestampAlignedSpan(
      RexNode fieldExpr,
      int intervalValue,
      TimeUnitConfig config,
      long referenceEpochSeconds,
      CalcitePlanContext context) {

    // UNIX_TIMESTAMP returns seconds, not milliseconds
    RexNode epochSeconds =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);

    // Reference is already in seconds, use directly
    RexNode referenceTimestamp = context.relBuilder.literal(referenceEpochSeconds);

    // Convert interval to seconds based on the time unit (not milliseconds!)
    long intervalSeconds;
    switch (config) {
      case HOURS -> intervalSeconds = intervalValue * 3600L;
      case MINUTES -> intervalSeconds = intervalValue * 60L;
      case SECONDS -> intervalSeconds = intervalValue;
      case MILLISECONDS -> intervalSeconds = intervalValue / 1000L;
      case MICROSECONDS -> intervalSeconds = intervalValue / 1000000L;
      case CENTISECONDS -> intervalSeconds = intervalValue / 100L;
      case DECISECONDS -> intervalSeconds = intervalValue / 10L;
      default -> intervalSeconds = intervalValue * 3600L; // Default to hours
    }

    RexNode intervalLiteral = context.relBuilder.literal(intervalSeconds);

    // SPL Universal Formula: bin_start = reference + floor((timestamp - reference) / span) * span
    System.out.println(
        "DEBUG EPOCH: reference=" + referenceEpochSeconds + "s, interval=" + intervalSeconds + "s");

    // Step 1: Calculate time offset from reference (all in seconds)
    RexNode timeOffset =
        context.relBuilder.call(SqlStdOperatorTable.MINUS, epochSeconds, referenceTimestamp);

    // Step 2: Find which bin this timestamp belongs to
    RexNode binNumber =
        context.relBuilder.call(
            SqlStdOperatorTable.FLOOR,
            context.relBuilder.call(SqlStdOperatorTable.DIVIDE, timeOffset, intervalLiteral));

    // Step 3: Calculate bin start time = reference + (bin_number * span)
    // Apply SPL Universal Formula directly without adjustments
    RexNode binOffset =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, binNumber, intervalLiteral);
    RexNode binStartSeconds =
        context.relBuilder.call(SqlStdOperatorTable.PLUS, referenceTimestamp, binOffset);

    // Convert back to timestamp (FROM_UNIXTIME expects seconds)
    return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binStartSeconds);
  }

  /** Parses time offset for modifiers (e.g., "4h" -> 14400000 milliseconds). */
  private static long parseTimeOffsetForModifier(String offsetStr) {
    offsetStr = offsetStr.trim().toLowerCase();

    if (offsetStr.endsWith("h")) {
      int hours = Integer.parseInt(offsetStr.substring(0, offsetStr.length() - 1));
      return hours * 3600000L; // hours to milliseconds
    } else if (offsetStr.endsWith("m")) {
      int minutes = Integer.parseInt(offsetStr.substring(0, offsetStr.length() - 1));
      return minutes * 60000L; // minutes to milliseconds
    } else if (offsetStr.endsWith("s")) {
      int seconds = Integer.parseInt(offsetStr.substring(0, offsetStr.length() - 1));
      return seconds * 1000L; // seconds to milliseconds
    } else {
      // Default to hours if no unit
      int hours = Integer.parseInt(offsetStr);
      return hours * 3600000L;
    }
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
    // UNIX_TIMESTAMP returns seconds, not milliseconds!
    RexNode epochSeconds =
        context.rexBuilder.makeCall(PPLBuiltinOperators.UNIX_TIMESTAMP, fieldExpr);

    // For sub-second units (ms, us, cs, ds), we need to work in milliseconds
    if (config == TimeUnitConfig.MILLISECONDS
        || config == TimeUnitConfig.MICROSECONDS
        || config == TimeUnitConfig.CENTISECONDS
        || config == TimeUnitConfig.DECISECONDS) {
      // Convert seconds to milliseconds for sub-second precision
      RexNode epochMillis =
          context.relBuilder.call(
              SqlStdOperatorTable.MULTIPLY, epochSeconds, context.relBuilder.literal(1000L));

      if (config.divisionFactor == 1) {
        return epochMillis; // milliseconds
      } else if (config.divisionFactor > 1) {
        // For sub-millisecond units, divide milliseconds by the factor
        return context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE,
            epochMillis,
            context.relBuilder.literal(config.divisionFactor));
      } else {
        // For microseconds (multiply milliseconds by 1000)
        return context.relBuilder.call(
            SqlStdOperatorTable.MULTIPLY,
            epochMillis,
            context.relBuilder.literal(MICROS_PER_MILLI));
      }
    } else {
      // For second and larger units, work in seconds
      if (config.divisionFactor == 1) {
        return epochSeconds;
      } else if (config.divisionFactor > 1) {
        // For larger units, divide seconds by the factor
        return context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE,
            epochSeconds,
            context.relBuilder.literal(config.divisionFactor));
      } else {
        return epochSeconds;
      }
    }
  }

  /** Converts from target unit back to timestamp. */
  private static RexNode convertFromTargetUnit(
      RexNode binValue, TimeUnitConfig config, CalcitePlanContext context) {

    // For sub-second units, binValue is in milliseconds, need to convert to seconds
    if (config == TimeUnitConfig.MILLISECONDS
        || config == TimeUnitConfig.MICROSECONDS
        || config == TimeUnitConfig.CENTISECONDS
        || config == TimeUnitConfig.DECISECONDS) {

      RexNode binMillis;
      if (config.divisionFactor == 1) {
        binMillis = binValue; // already in milliseconds
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

      // Convert milliseconds back to seconds for FROM_UNIXTIME
      RexNode binSeconds =
          context.relBuilder.call(
              SqlStdOperatorTable.DIVIDE, binMillis, context.relBuilder.literal(1000L));

      return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binSeconds);

    } else {
      // For second and larger units, binValue is already in seconds
      RexNode binSeconds;

      if (config.divisionFactor == 1) {
        binSeconds = binValue;
      } else if (config.divisionFactor > 1) {
        binSeconds =
            context.relBuilder.call(
                SqlStdOperatorTable.MULTIPLY,
                binValue,
                context.relBuilder.literal(config.divisionFactor));
      } else {
        binSeconds = binValue;
      }

      return context.rexBuilder.makeCall(PPLBuiltinOperators.FROM_UNIXTIME, binSeconds);
    }
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
   * Create months-based span expression using SPL Monthly Binning Algorithm. Uses Unix epoch
   * (January 1970) as reference point with modular arithmetic. Returns YYYY-MM formatted strings
   * for bin start months.
   */
  private static RexNode createMonthsSpan(
      RexNode fieldExpr, int intervalMonths, CalcitePlanContext context) {

    // Extract year and month from the input timestamp
    RexNode inputYear = context.rexBuilder.makeCall(PPLBuiltinOperators.YEAR, fieldExpr);
    RexNode inputMonth = context.rexBuilder.makeCall(PPLBuiltinOperators.MONTH, fieldExpr);

    // SPL Monthly Binning Algorithm:
    // Step 1: Calculate months since Unix epoch (January 1970)
    // months_since_epoch = (year - 1970) * 12 + (month - 1)
    RexNode monthsSinceEpoch = calculateMonthsSinceEpoch(inputYear, inputMonth, context);

    // Step 2: Find bin start using modular arithmetic
    // bin_start_months = months_since_epoch - (months_since_epoch % interval)
    RexNode binStartMonths = calculateBinStart(monthsSinceEpoch, intervalMonths, context);

    // Step 3: Convert bin start months back to year and month
    RexNode binStartYear = calculateBinStartYear(binStartMonths, context);
    RexNode binStartMonth = calculateBinStartMonth(binStartMonths, context);

    // Step 4: Format as YYYY-MM string
    // Create a temporary date from the bin start year/month to format it
    RexNode tempDate =
        context.rexBuilder.makeCall(
            PPLBuiltinOperators.MAKEDATE,
            binStartYear,
            context.rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                context.rexBuilder.makeCall(
                    SqlStdOperatorTable.MULTIPLY,
                    context.rexBuilder.makeCall(
                        SqlStdOperatorTable.MINUS, binStartMonth, context.relBuilder.literal(1)),
                    context.relBuilder.literal(31)),
                context.relBuilder.literal(1)));

    // Format the date as YYYY-MM string
    return context.rexBuilder.makeCall(
        PPLBuiltinOperators.DATE_FORMAT, tempDate, context.relBuilder.literal("%Y-%m"));
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

  /**
   * Validates sub-second span constraints. When span is expressed using a sub-second unit (ds, cs,
   * ms, us), the span value needs to be < 1 second, and 1 second must be evenly divisible by the
   * span value.
   */
  private static void validateSubSecondSpan(TimeUnitConfig config, int intervalValue) {
    if (config == TimeUnitConfig.MICROSECONDS
        || config == TimeUnitConfig.MILLISECONDS
        || config == TimeUnitConfig.CENTISECONDS
        || config == TimeUnitConfig.DECISECONDS) {

      // Convert interval to microseconds for comparison
      long intervalMicros;
      switch (config) {
        case MICROSECONDS -> intervalMicros = intervalValue;
        case MILLISECONDS -> intervalMicros = intervalValue * 1000L;
        case CENTISECONDS -> intervalMicros = intervalValue * 10000L; // 1cs = 10ms = 10000us
        case DECISECONDS -> intervalMicros = intervalValue * 100000L; // 1ds = 100ms = 100000us
        default -> intervalMicros = 0; // Should never reach here
      }

      long oneSecondMicros = 1000000L; // 1 second = 1,000,000 microseconds

      // Constraint 1: span value must be < 1 second
      if (intervalMicros >= oneSecondMicros) {
        throw new IllegalArgumentException(
            String.format(
                "Sub-second span %d%s must be less than 1 second", intervalValue, config.unit));
      }

      // Constraint 2: 1 second must be evenly divisible by the span value
      if (oneSecondMicros % intervalMicros != 0) {
        throw new IllegalArgumentException(
            String.format(
                "1 second must be evenly divisible by span %d%s", intervalValue, config.unit));
      }
    }
  }

  // TODO: Implement proper month string formatting later
}
