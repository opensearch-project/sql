/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.binning;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.type.ExprSqlType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.calcite.utils.binning.BinConstants;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * WIDTH_BUCKET(field_value, num_bins, data_range, max_value) - Histogram bucketing function.
 *
 * <p>This function creates equal-width bins for histogram operations. It uses a mathematical O(1)
 * algorithm to determine optimal bin widths based on powers of 10.
 *
 * <p>Parameters:
 *
 * <ul>
 *   <li>field_value - The numeric value to bin
 *   <li>num_bins - Number of bins to create
 *   <li>data_range - Range of the data (MAX - MIN)
 *   <li>max_value - Maximum value in the dataset
 * </ul>
 *
 * <p>Implements the same binning logic as BinCalculatorFunction for 'bins' type.
 */
public class WidthBucketFunction extends ImplementorUDF {

  public WidthBucketFunction() {
    super(new WidthBucketImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return (opBinding) -> {
      RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
      RelDataType arg0Type = opBinding.getOperandType(0);
      return dateRelatedType(arg0Type)
          ? arg0Type
          : typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000), true);
    };
  }

  public static boolean dateRelatedType(RelDataType type) {
    return type instanceof ExprSqlType exprSqlType
        && List.of(ExprUDT.EXPR_DATE, ExprUDT.EXPR_TIME, ExprUDT.EXPR_TIMESTAMP)
            .contains(exprSqlType.getUdt());
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC_NUMERIC_NUMERIC_NUMERIC;
  }

  public static class WidthBucketImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression fieldValue = translatedOperands.get(0);
      Expression numBins = translatedOperands.get(1);
      Expression minValue = translatedOperands.get(2);
      Expression maxValue = translatedOperands.get(3);

      // Pass the field type information to help detect timestamps
      RelDataType fieldType = call.getOperands().get(0).getType();
      boolean isTimestampField = dateRelatedType(fieldType);
      Expression isTimestamp = Expressions.constant(isTimestampField);

      // For timestamp fields, keep as-is (don't convert to Number)
      // For numeric fields, convert to Number
      Expression fieldValueExpr =
          isTimestampField ? fieldValue : Expressions.convert_(fieldValue, Number.class);
      Expression minValueExpr =
          isTimestampField ? minValue : Expressions.convert_(minValue, Number.class);
      Expression maxValueExpr =
          isTimestampField ? maxValue : Expressions.convert_(maxValue, Number.class);

      return Expressions.call(
          WidthBucketImplementor.class,
          "calculateWidthBucket",
          fieldValueExpr,
          Expressions.convert_(numBins, Number.class),
          minValueExpr,
          maxValueExpr,
          isTimestamp);
    }

    /** Width bucket calculation using nice number algorithm. */
    public static String calculateWidthBucket(
        Object fieldValue,
        Number numBinsParam,
        Object minValue,
        Object maxValue,
        boolean isTimestamp) {
      if (fieldValue == null || numBinsParam == null || minValue == null || maxValue == null) {
        return null;
      }

      int numBins = numBinsParam.intValue();
      if (numBins < BinConstants.MIN_BINS || numBins > BinConstants.MAX_BINS) {
        return null;
      }

      // Handle timestamp fields differently
      if (isTimestamp) {
        // Convert all timestamp values to milliseconds
        long fieldMillis = convertTimestampToMillis(fieldValue);
        long minMillis = convertTimestampToMillis(minValue);
        long maxMillis = convertTimestampToMillis(maxValue);

        // Calculate range
        long rangeMillis = maxMillis - minMillis;
        if (rangeMillis <= 0) {
          return null;
        }

        return calculateTimestampBucket(fieldMillis, numBins, rangeMillis, minMillis);
      }

      // Numeric field handling (existing logic)
      Number numericValue = (Number) fieldValue;
      Number numericMin = (Number) minValue;
      Number numericMax = (Number) maxValue;

      double value = numericValue.doubleValue();
      double min = numericMin.doubleValue();
      double max = numericMax.doubleValue();

      // Calculate range
      double range = max - min;
      if (range <= 0) {
        return null;
      }

      // Calculate optimal width using nice number algorithm
      double width = calculateOptimalWidth(range, max, numBins);
      if (width <= 0) {
        return null;
      }

      double binStart = Math.floor(value / width) * width;
      double binEnd = binStart + width;

      return formatRange(binStart, binEnd, width);
    }

    /** Calculate optimal width using mathematical O(1) algorithm. */
    private static double calculateOptimalWidth(
        double dataRange, double maxValue, int requestedBins) {
      if (dataRange <= 0 || requestedBins <= 0) {
        return 1.0; // Safe fallback
      }

      // Calculate target width: target_width = data_range / requested_bins
      double targetWidth = dataRange / requestedBins;

      // Find optimal starting point: exponent = CEIL(LOG10(target_width))
      double exponent = Math.ceil(Math.log10(targetWidth));

      // Select optimal width: 10^exponent
      double optimalWidth = Math.pow(10.0, exponent);

      // Account for boundaries: If the maximum value falls exactly on a bin boundary, add one extra
      // bin
      double actualBins = Math.ceil(dataRange / optimalWidth);
      if (maxValue % optimalWidth == 0) {
        actualBins++;
      }

      // If we exceed requested bins, we need to go to next magnitude level
      if (actualBins > requestedBins) {
        optimalWidth = Math.pow(10.0, exponent + 1);
      }

      return optimalWidth;
    }

    /**
     * Convert timestamp value to milliseconds. Handles both numeric (Long) milliseconds and String
     * formatted timestamps.
     */
    private static long convertTimestampToMillis(Object timestamp) {
      if (timestamp instanceof Number) {
        return ((Number) timestamp).longValue();
      } else if (timestamp instanceof String) {
        // Parse timestamp string "yyyy-MM-dd HH:mm:ss" to milliseconds
        // Use LocalDateTime to parse without timezone, then convert to UTC
        String timestampStr = (String) timestamp;
        java.time.LocalDateTime localDateTime =
            java.time.LocalDateTime.parse(
                timestampStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        // Assume the timestamp is in UTC and convert to epoch millis
        return localDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli();
      } else {
        throw new IllegalArgumentException("Unsupported timestamp type: " + timestamp.getClass());
      }
    }

    /**
     * Calculate timestamp bucket using auto_date_histogram interval selection. Timestamps are in
     * milliseconds since epoch. Bins are aligned to the minimum timestamp, not to calendar
     * boundaries.
     */
    private static String calculateTimestampBucket(
        long timestampMillis, int numBins, long rangeMillis, long minMillis) {
      // Calculate target width in milliseconds
      long targetWidthMillis = rangeMillis / numBins;

      // Select appropriate time interval (same as OpenSearch auto_date_histogram)
      long intervalMillis = selectTimeInterval(targetWidthMillis);

      // Floor timestamp to the interval boundary aligned with minMillis
      // This ensures bins start at the data's minimum value, like OpenSearch auto_date_histogram
      long offsetFromMin = timestampMillis - minMillis;
      long intervalsSinceMin = offsetFromMin / intervalMillis;
      long binStartMillis = minMillis + (intervalsSinceMin * intervalMillis);

      // Format as ISO 8601 timestamp string
      return formatTimestamp(binStartMillis);
    }

    /**
     * Select the appropriate time interval based on target width. Uses the same intervals as
     * OpenSearch auto_date_histogram: 1s, 5s, 10s, 30s, 1m, 5m, 10m, 30m, 1h, 3h, 12h, 1d, 7d, 1M,
     * 1y
     */
    private static long selectTimeInterval(long targetWidthMillis) {
      // Define nice time intervals in milliseconds
      long[] intervals = {
        1000L, // 1 second
        5000L, // 5 seconds
        10000L, // 10 seconds
        30000L, // 30 seconds
        60000L, // 1 minute
        300000L, // 5 minutes
        600000L, // 10 minutes
        1800000L, // 30 minutes
        3600000L, // 1 hour
        10800000L, // 3 hours
        43200000L, // 12 hours
        86400000L, // 1 day
        604800000L, // 7 days
        2592000000L, // 30 days (approximate month)
        31536000000L // 365 days (approximate year)
      };

      // Find the smallest interval that is >= target width
      for (long interval : intervals) {
        if (interval >= targetWidthMillis) {
          return interval;
        }
      }

      // If target is larger than all intervals, use the largest
      return intervals[intervals.length - 1];
    }

    /** Format timestamp in milliseconds as ISO 8601 string. Format: "yyyy-MM-dd HH:mm:ss" */
    private static String formatTimestamp(long timestampMillis) {
      Instant instant = Instant.ofEpochMilli(timestampMillis);
      ZonedDateTime zdt = instant.atZone(ZoneOffset.UTC);
      return zdt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /** Format range string with appropriate precision. */
    private static String formatRange(double binStart, double binEnd, double span) {
      if (isIntegerSpan(span) && isIntegerValue(binStart) && isIntegerValue(binEnd)) {
        return String.format("%d-%d", (long) binStart, (long) binEnd);
      } else {
        return formatFloatingPointRange(binStart, binEnd, span);
      }
    }

    /** Checks if the span represents an integer value. */
    private static boolean isIntegerSpan(double span) {
      return span == Math.floor(span) && !Double.isInfinite(span);
    }

    /** Checks if a value is effectively an integer. */
    private static boolean isIntegerValue(double value) {
      return Math.abs(value - Math.round(value)) < 1e-10;
    }

    /** Formats floating-point ranges with appropriate precision. */
    private static String formatFloatingPointRange(double binStart, double binEnd, double span) {
      int decimalPlaces = getAppropriateDecimalPlaces(span);
      String format = String.format("%%.%df-%%.%df", decimalPlaces, decimalPlaces);
      return String.format(format, binStart, binEnd);
    }

    /** Determines appropriate decimal places for formatting based on span size. */
    private static int getAppropriateDecimalPlaces(double span) {
      if (span >= 1.0) {
        return 1;
      } else if (span >= 0.1) {
        return 2;
      } else if (span >= 0.01) {
        return 3;
      } else {
        return 4;
      }
    }
  }
}
