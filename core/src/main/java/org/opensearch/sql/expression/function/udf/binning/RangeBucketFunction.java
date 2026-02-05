/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.binning;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * RANGE_BUCKET(field_value, data_min, data_max, start_param, end_param) - Range-based bucketing
 * function.
 *
 * <p>This function creates bins based on range boundaries (start/end) using magnitude-based width
 * calculation. It implements the sophisticated range expansion algorithm where effective range only
 * expands, never shrinks.
 *
 * <p>Parameters:
 *
 * <ul>
 *   <li>field_value - The numeric value to bin
 *   <li>data_min - Minimum value from the dataset (from MIN window function)
 *   <li>data_max - Maximum value from the dataset (from MAX window function)
 *   <li>start_param - User-specified start value (or null)
 *   <li>end_param - User-specified end value (or null)
 * </ul>
 *
 * <p>The function calculates effective min/max using expansion algorithm and determines width using
 * magnitude-based calculation.
 */
public class RangeBucketFunction extends ImplementorUDF {

  public RangeBucketFunction() {
    super(new RangeBucketImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000;
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC_NUMERIC_NUMERIC_NUMERIC_NUMERIC;
  }

  public static class RangeBucketImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression fieldValue = translatedOperands.get(0);
      Expression dataMin = translatedOperands.get(1);
      Expression dataMax = translatedOperands.get(2);
      Expression startParam = translatedOperands.get(3);
      Expression endParam = translatedOperands.get(4);

      return Expressions.call(
          RangeBucketImplementor.class,
          "calculateRangeBucket",
          Expressions.convert_(Expressions.box(fieldValue), Number.class),
          Expressions.convert_(Expressions.box(dataMin), Number.class),
          Expressions.convert_(Expressions.box(dataMax), Number.class),
          Expressions.convert_(Expressions.box(startParam), Number.class),
          Expressions.convert_(Expressions.box(endParam), Number.class));
    }

    /** Range bucket calculation with expansion algorithm and magnitude-based width. */
    public static String calculateRangeBucket(
        Number fieldValue, Number dataMin, Number dataMax, Number startParam, Number endParam) {
      if (fieldValue == null || dataMin == null || dataMax == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      double dMin = dataMin.doubleValue();
      double dMax = dataMax.doubleValue();

      // Calculate effective min using expansion algorithm (only expand, never shrink)
      double effectiveMin = dMin;
      if (startParam != null) {
        double start = startParam.doubleValue();
        effectiveMin = Math.min(start, dMin);
      }

      // Calculate effective max using expansion algorithm (only expand, never shrink)
      double effectiveMax = dMax;
      if (endParam != null) {
        double end = endParam.doubleValue();
        effectiveMax = Math.max(end, dMax);
      }

      // Calculate effective range
      double effectiveRange = effectiveMax - effectiveMin;
      if (effectiveRange <= 0) {
        return null;
      }

      // Calculate magnitude-based width with boundary handling
      double width = calculateMagnitudeBasedWidth(effectiveRange);
      if (width <= 0) {
        return null;
      }

      // Calculate first bin start aligned to the width
      double firstBinStart = Math.floor(effectiveMin / width) * width;

      // Calculate bin value for current field
      double adjustedField = value - firstBinStart;
      double binIndex = Math.floor(adjustedField / width);
      double binStart = binIndex * width + firstBinStart;
      double binEnd = binStart + width;

      return formatRange(binStart, binEnd, width);
    }

    /** Calculate magnitude-based width with boundary handling. */
    private static double calculateMagnitudeBasedWidth(double effectiveRange) {
      double log10Range = Math.log10(effectiveRange);
      double floorLog = Math.floor(log10Range);

      // Check if effective_range is exactly a power of 10
      boolean isExactPowerOf10 = Math.abs(log10Range - floorLog) < 1e-10;

      // If exact power of 10: width = 10^(FLOOR(LOG10(effective_range)) - 1)
      // Otherwise: width = 10^FLOOR(LOG10(effective_range))
      double adjustedMagnitude = isExactPowerOf10 ? floorLog - 1.0 : floorLog;

      return Math.pow(10.0, adjustedMagnitude);
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
