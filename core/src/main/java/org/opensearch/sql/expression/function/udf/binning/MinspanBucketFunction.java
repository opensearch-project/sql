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
 * MINSPAN_BUCKET(field_value, min_span, data_range, max_value) - Minimum span bucketing function.
 *
 * <p>This function creates bins with a minimum span width using magnitude-based logic. The actual
 * bin width is determined by comparing the minimum span with the data range magnitude.
 *
 * <p>Parameters:
 *
 * <ul>
 *   <li>field_value - The numeric value to bin
 *   <li>min_span - The minimum span width required
 *   <li>data_range - Range of the data (MAX - MIN)
 *   <li>max_value - Maximum value in the dataset (currently unused but kept for compatibility)
 * </ul>
 *
 * <p>Implements the same binning logic as BinCalculatorFunction for 'minspan' type.
 */
public class MinspanBucketFunction extends ImplementorUDF {

  public MinspanBucketFunction() {
    super(new MinspanBucketImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000;
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC_NUMERIC_NUMERIC_NUMERIC;
  }

  public static class MinspanBucketImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression fieldValue = translatedOperands.get(0);
      Expression minSpan = translatedOperands.get(1);
      Expression dataRange = translatedOperands.get(2);
      Expression maxValue = translatedOperands.get(3);

      return Expressions.call(
          MinspanBucketImplementor.class,
          "calculateMinspanBucket",
          Expressions.convert_(Expressions.box(fieldValue), Number.class),
          Expressions.convert_(Expressions.box(minSpan), Number.class),
          Expressions.convert_(Expressions.box(dataRange), Number.class),
          Expressions.convert_(Expressions.box(maxValue), Number.class));
    }

    /** Minspan bucket calculation. */
    public static String calculateMinspanBucket(
        Number fieldValue, Number minSpanParam, Number dataRange, Number maxValue) {
      if (fieldValue == null || minSpanParam == null || dataRange == null || maxValue == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      double minSpan = minSpanParam.doubleValue();

      if (minSpan <= 0) {
        return null;
      }

      double range = dataRange.doubleValue();

      if (range <= 0) {
        return null;
      }

      // Calculate minspan width using magnitude-based logic
      double log10Minspan = Math.log10(minSpan);
      double ceilLog = Math.ceil(log10Minspan);
      double minspanWidth = Math.pow(10, ceilLog);

      double log10Range = Math.log10(range);
      double floorLog = Math.floor(log10Range);
      double defaultWidth = Math.pow(10, floorLog);

      // Choose between default width and minspan width
      boolean useDefault = defaultWidth >= minSpan;
      double selectedWidth = useDefault ? defaultWidth : minspanWidth;

      double binStart = Math.floor(value / selectedWidth) * selectedWidth;
      double binEnd = binStart + selectedWidth;

      return formatRange(binStart, binEnd, selectedWidth);
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
