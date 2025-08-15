/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function.udf.math;

import java.util.List;
import org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.opensearch.sql.calcite.utils.PPLOperandTypes;
import org.opensearch.sql.expression.function.ImplementorUDF;
import org.opensearch.sql.expression.function.UDFOperandMetadata;

/**
 * <code>BIN_NUMERIC_SPAN_CALCULATOR(field_value, span_value, start_value, end_value)</code>
 * calculates the bin range string for numeric span-based binning.
 *
 * <p>This function implements the "never shrink the data range" principle: start/end parameters
 * only expand the effective range, never create "Other" categories.
 *
 * <p>This function replaces the complex nested SqlOperators previously used in BinUtils, reducing
 * SQL plan complexity and improving maintainability with direct Java calculations.
 */
public class BinNumericSpanCalculatorFunction extends ImplementorUDF {

  public BinNumericSpanCalculatorFunction() {
    super(new BinNumericSpanCalculatorImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC_NUMERIC_NUMERIC_NUMERIC;
  }

  public static class BinNumericSpanCalculatorImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression fieldValue = translatedOperands.get(0);
      Expression spanValue = translatedOperands.get(1);
      Expression startValue = translatedOperands.get(2);
      Expression endValue = translatedOperands.get(3);

      return Expressions.call(
          BinNumericSpanCalculatorImplementor.class,
          "calculateBinRange",
          Expressions.convert_(fieldValue, Number.class),
          Expressions.convert_(spanValue, Number.class),
          Expressions.convert_(startValue, Number.class),
          Expressions.convert_(endValue, Number.class));
    }

    /**
     * Calculates the bin range string for numeric span-based binning.
     *
     * <p>Implements the "never shrink the data range" principle by ignoring start/end parameters.
     * This ensures no data values are excluded from binning (no "Other" categories created).
     *
     * @param fieldValue the numeric value to bin
     * @param spanValue the span (bin width) value
     * @param startValue the start parameter (ignored to implement "never shrink" principle)
     * @param endValue the end parameter (ignored to implement "never shrink" principle)
     * @return the bin range string (e.g., "32-33"), or null if inputs are invalid
     */
    @SuppressWarnings("unused") // startValue and endValue ignored for "never shrink" principle
    public static String calculateBinRange(
        Number fieldValue, Number spanValue, Number startValue, Number endValue) {
      if (fieldValue == null || spanValue == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      double span = spanValue.doubleValue();

      if (span <= 0) {
        return null;
      }

      // "Never shrink the data range" principle: start/end parameters are intentionally ignored
      // This ensures no data values are excluded (no "Other" categories created)
      // Standard span-based binning is applied regardless of start/end values

      // Calculate bin boundaries
      // For span-based binning: bin_start = floor(value / span) * span
      double binStart = Math.floor(value / span) * span;
      double binEnd = binStart + span;

      // Format the range string
      if (isIntegerSpan(span) && isIntegerValue(binStart) && isIntegerValue(binEnd)) {
        // Use integer formatting for clean output
        return String.format("%d-%d", (long) binStart, (long) binEnd);
      } else {
        // Use floating-point formatting with appropriate precision
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
      // Determine appropriate decimal places based on span magnitude
      int decimalPlaces = getAppropriateDecimalPlaces(span);
      String format = String.format("%%.%df-%%.%df", decimalPlaces, decimalPlaces);
      return String.format(format, binStart, binEnd);
    }

    /** Determines appropriate decimal places for formatting based on span size. */
    private static int getAppropriateDecimalPlaces(double span) {
      if (span >= 1.0) {
        return 1; // e.g., "1.5-2.5"
      } else if (span >= 0.1) {
        return 2; // e.g., "0.15-0.25"
      } else if (span >= 0.01) {
        return 3; // e.g., "0.015-0.025"
      } else {
        return 4; // e.g., "0.0015-0.0025"
      }
    }
  }
}
