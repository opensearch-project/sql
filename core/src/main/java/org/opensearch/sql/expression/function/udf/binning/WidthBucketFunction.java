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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jspecify.annotations.NonNull;
import org.opensearch.sql.calcite.utils.OpenSearchTypeUtil;
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
      return OpenSearchTypeUtil.isDatetime(arg0Type)
          ? arg0Type
          : typeFactory.createTypeWithNullability(
              typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000), true);
    };
  }

  @Override
  public @NonNull UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.WIDTH_BUCKET_OPERAND;
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
          Expressions.convert_(Expressions.box(fieldValue), Number.class),
          Expressions.convert_(Expressions.box(numBins), Number.class),
          Expressions.convert_(Expressions.box(dataRange), Number.class),
          Expressions.convert_(Expressions.box(maxValue), Number.class));
    }

    /** Width bucket calculation using nice number algorithm. */
    public static String calculateWidthBucket(
        Number fieldValue, Number numBinsParam, Number dataRange, Number maxValue) {
      if (fieldValue == null || numBinsParam == null || dataRange == null || maxValue == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      int numBins = numBinsParam.intValue();

      if (numBins < BinConstants.MIN_BINS || numBins > BinConstants.MAX_BINS) {
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
