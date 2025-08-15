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
 * <code>BIN_WIDTH_CALCULATOR(data_range, max_value, requested_bins)</code> calculates the optimal
 * bin width using the nice number algorithm for histogram binning.
 *
 * <p>This function encapsulates the complex logic that was previously implemented as nested
 * SqlOperators, reducing SQL plan complexity and improving maintainability.
 */
public class BinWidthCalculatorFunction extends ImplementorUDF {

  public BinWidthCalculatorFunction() {
    super(new BinWidthCalculatorImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.DOUBLE_FORCE_NULLABLE;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.NUMERIC_NUMERIC_NUMERIC;
  }

  public static class BinWidthCalculatorImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression dataRange = translatedOperands.get(0);
      Expression maxValue = translatedOperands.get(1);
      Expression requestedBins = translatedOperands.get(2);

      return Expressions.call(
          BinWidthCalculatorImplementor.class,
          "calculateOptimalWidth",
          Expressions.convert_(dataRange, Number.class),
          Expressions.convert_(maxValue, Number.class),
          Expressions.convert_(requestedBins, Number.class));
    }

    /**
     * Calculates the optimal bin width using the nice number algorithm.
     *
     * @param dataRange the range of data (max - min)
     * @param maxValue the maximum value in the dataset
     * @param requestedBins the requested number of bins
     * @return the optimal width, or null if inputs are invalid
     */
    public static Double calculateOptimalWidth(
        Number dataRange, Number maxValue, Number requestedBins) {
      if (dataRange == null || maxValue == null || requestedBins == null) {
        return null;
      }

      double range = dataRange.doubleValue();
      double max = maxValue.doubleValue();
      int bins = requestedBins.intValue();

      // Validate inputs
      if (range <= 0 || bins <= 0) {
        return null;
      }

      // Calculate target width and find the optimal nice width
      double targetWidth = range / bins;

      // Try nice widths starting from the target width
      double baseExponent = Math.log10(targetWidth);

      // Start from the smallest power of 10 >= targetWidth
      int startExponent = (int) Math.ceil(baseExponent);

      // Try powers of 10 starting from startExponent (up to 1E9 to match original behavior)
      for (int exponent = startExponent; exponent <= 9; exponent++) {
        double niceWidth = Math.pow(10, exponent);

        // Validate that this nice width works with our constraints
        double theoreticalBinsDouble = Math.ceil(range / niceWidth);

        // Check for overflow
        if (theoreticalBinsDouble <= Integer.MAX_VALUE) {
          int theoreticalBins = (int) theoreticalBinsDouble;
          int extraBin = (max % niceWidth == 0) ? 1 : 0;

          // Check for overflow when adding extra bin
          if (theoreticalBins < Integer.MAX_VALUE || extraBin == 0) {
            int actualBins = theoreticalBins + extraBin;

            if (actualBins <= bins) {
              return niceWidth;
            }
          }
        }
      }

      // Fallback: exact width if no nice width works
      return range / bins;
    }
  }
}
