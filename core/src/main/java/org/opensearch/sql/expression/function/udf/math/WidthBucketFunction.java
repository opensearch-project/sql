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
 * WIDTH_BUCKET(field_value, num_bins, data_range, max_value) - Histogram bucketing function.
 *
 * <p>This function creates equal-width bins for histogram operations. It uses the nice number
 * algorithm to determine optimal bin widths.
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
    return ReturnTypes.VARCHAR_2000;
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
      Expression dataRange = translatedOperands.get(2);
      Expression maxValue = translatedOperands.get(3);

      return Expressions.call(
          WidthBucketImplementor.class,
          "calculateWidthBucket",
          Expressions.convert_(fieldValue, Number.class),
          Expressions.convert_(numBins, Number.class),
          Expressions.convert_(dataRange, Number.class),
          Expressions.convert_(maxValue, Number.class));
    }

    /** Width bucket calculation using nice number algorithm. */
    public static String calculateWidthBucket(
        Number fieldValue, Number numBinsParam, Number dataRange, Number maxValue) {
      if (fieldValue == null || numBinsParam == null || dataRange == null || maxValue == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      int numBins = numBinsParam.intValue();

      if (numBins <= 0) {
        return null;
      }

      double range = dataRange.doubleValue();
      double max = maxValue.doubleValue();

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

    /** Calculate optimal width using nice number algorithm. */
    private static double calculateOptimalWidth(
        double dataRange, double maxValue, int requestedBins) {
      // Nice widths array
      double[] niceWidths = {
        0.001, 0.01, 0.1, 1.0, 10.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0
      };

      for (double width : niceWidths) {
        double theoreticalBins = Math.ceil(dataRange / width);
        if (theoreticalBins > Integer.MAX_VALUE) {
          continue;
        }

        int actualBins = (int) theoreticalBins;
        if (maxValue % width == 0) {
          actualBins++;
        }

        if (actualBins <= requestedBins) {
          return width;
        }
      }

      return dataRange / requestedBins; // Fallback
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
