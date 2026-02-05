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
 * SPAN_BUCKET(field_value, span_value) - Fixed-width span bucketing function.
 *
 * <p>This function creates fixed-width bins based on a specified span value. Each bin has exactly
 * the specified width.
 *
 * <p>Parameters:
 *
 * <ul>
 *   <li>field_value - The numeric value to bin
 *   <li>span_value - The width of each bin
 * </ul>
 *
 * <p>Implements the same binning logic as BinCalculatorFunction for 'span' type.
 */
public class SpanBucketFunction extends ImplementorUDF {

  public SpanBucketFunction() {
    super(new SpanBucketImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000;
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC_NUMERIC;
  }

  public static class SpanBucketImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression fieldValue = translatedOperands.get(0);
      Expression spanValue = translatedOperands.get(1);

      return Expressions.call(
          SpanBucketImplementor.class,
          "calculateSpanBucket",
          Expressions.convert_(Expressions.box(fieldValue), Number.class),
          Expressions.convert_(Expressions.box(spanValue), Number.class));
    }

    /** Span bucket calculation. */
    public static String calculateSpanBucket(Number fieldValue, Number spanParam) {
      if (fieldValue == null || spanParam == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      double span = spanParam.doubleValue();
      if (span <= 0) {
        return null;
      }

      double binStart = Math.floor(value / span) * span;
      double binEnd = binStart + span;

      return formatRange(binStart, binEnd, span);
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
