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
 * <code>BIN_CALCULATOR(field_value, bin_type, param1, param2, param3, data_range, max_value)</code>
 * - Unified UDF for all binning operations.
 *
 * <p>This function consolidates all binning logic into a single UDF, eliminating complex nested
 * SqlOperators and providing ultra-clean SQL explain output.
 *
 * <p>Supported binning types:
 *
 * <ul>
 *   <li>'span' - span-based binning: BIN_CALCULATOR(field, 'span', span_value, start, end, -1, -1)
 *   <li>'bins' - bins-based binning: BIN_CALCULATOR(field, 'bins', num_bins, start, end,
 *       data_range, max_value)
 *   <li>'minspan' - minspan-based binning: BIN_CALCULATOR(field, 'minspan', min_span, start, end,
 *       data_range, max_value)
 * </ul>
 *
 * <p>For bins and minspan, data_range and max_value are computed using window functions. For
 * span-based binning, these parameters are ignored (passed as -1).
 *
 * <p>Implements the "never shrink the data range" principle across all binning types.
 */
public class BinCalculatorFunction extends ImplementorUDF {

  public BinCalculatorFunction() {
    super(new BinCalculatorImplementor(), NullPolicy.ANY);
  }

  @Override
  public SqlReturnTypeInference getReturnTypeInference() {
    return ReturnTypes.VARCHAR_2000;
  }

  @Override
  public UDFOperandMetadata getOperandMetadata() {
    return PPLOperandTypes.STRING_OR_NUMERIC_NUMERIC_NUMERIC_NUMERIC_NUMERIC_NUMERIC;
  }

  public static class BinCalculatorImplementor implements NotNullImplementor {

    @Override
    public Expression implement(
        RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
      Expression fieldValue = translatedOperands.get(0);
      Expression param1 = translatedOperands.get(2);
      Expression param2 = translatedOperands.get(3);
      Expression param3 = translatedOperands.get(4);
      Expression dataRange = translatedOperands.get(5);
      Expression maxValue = translatedOperands.get(6);

      // Determine bin type at compile time if possible
      if (call.operands.get(1) instanceof org.apache.calcite.rex.RexLiteral) {
        org.apache.calcite.rex.RexLiteral literal =
            (org.apache.calcite.rex.RexLiteral) call.operands.get(1);
        String type = literal.getValueAs(String.class);
        if (type != null) {
          String lowerType = type.toLowerCase();
          switch (lowerType) {
            case "span":
              return Expressions.call(
                  BinCalculatorImplementor.class,
                  "calculateSpanBin",
                  Expressions.convert_(fieldValue, Number.class),
                  Expressions.convert_(param1, Number.class));
            case "bins":
              return Expressions.call(
                  BinCalculatorImplementor.class,
                  "calculateBinsBin",
                  Expressions.convert_(fieldValue, Number.class),
                  Expressions.convert_(param1, Number.class),
                  Expressions.convert_(dataRange, Number.class),
                  Expressions.convert_(maxValue, Number.class));
            case "minspan":
              return Expressions.call(
                  BinCalculatorImplementor.class,
                  "calculateMinspanBin",
                  Expressions.convert_(fieldValue, Number.class),
                  Expressions.convert_(param1, Number.class),
                  Expressions.convert_(dataRange, Number.class),
                  Expressions.convert_(maxValue, Number.class));
          }
        }
      }

      // Fallback to runtime switch for dynamic bin types
      Expression binType = translatedOperands.get(1);
      return Expressions.call(
          BinCalculatorImplementor.class,
          "calculateBin",
          Expressions.convert_(fieldValue, Number.class),
          Expressions.convert_(binType, String.class),
          Expressions.convert_(param1, Number.class),
          Expressions.convert_(param2, Number.class),
          Expressions.convert_(param3, Number.class),
          Expressions.convert_(dataRange, Number.class),
          Expressions.convert_(maxValue, Number.class));
    }

    /**
     * Unified bin calculation supporting all binning types.
     *
     * @param fieldValue the numeric value to bin
     * @param binType the type of binning: "span", "bins", or "minspan"
     * @param param1 span_value (for span), num_bins (for bins), min_span (for minspan)
     * @param param2 start_value (optional, use -1 for unspecified)
     * @param param3 end_value (optional, use -1 for unspecified)
     * @param dataRange the data range (max - min) for bins/minspan, ignored for span
     * @param maxValue the maximum value for bins/minspan, ignored for span
     * @return the bin range string or null if inputs are invalid
     */
    public static String calculateBin(
        Number fieldValue,
        String binType,
        Number param1,
        Number param2,
        Number param3,
        Number dataRange,
        Number maxValue) {
      if (fieldValue == null || binType == null || param1 == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      String type = binType.toLowerCase();

      switch (type) {
        case "span":
          return calculateSpanBin(value, param1.doubleValue());
        case "bins":
          return calculateBinsBin(value, param1.intValue(), dataRange, maxValue);
        case "minspan":
          return calculateMinspanBin(value, param1.doubleValue(), dataRange, maxValue);
        default:
          return null;
      }
    }

    /** Span-based binning calculation. */
    public static String calculateSpanBin(Number fieldValue, Number spanParam) {
      if (fieldValue == null || spanParam == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      double span = spanParam.doubleValue();
      if (span <= 0) {
        return null;
      }

      // "Never shrink the data range" principle: start/end parameters are not used
      double binStart = Math.floor(value / span) * span;
      double binEnd = binStart + span;

      return formatRange(binStart, binEnd, span);
    }

    /** Bins-based binning calculation using nice number algorithm. */
    public static String calculateBinsBin(
        Number fieldValue, Number numBinsParam, Number dataRange, Number maxValue) {
      if (fieldValue == null || numBinsParam == null || dataRange == null || maxValue == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      int numBins = numBinsParam.intValue();

      if (numBins <= 0) {
        return null;
      }

      // Use the exact same nice number algorithm as BinWidthCalculatorFunction
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

    /** Minspan-based binning calculation. */
    public static String calculateMinspanBin(
        Number fieldValue, Number minSpanParam, Number dataRange, Number maxValue) {
      if (fieldValue == null || minSpanParam == null || dataRange == null || maxValue == null) {
        return null;
      }

      double value = fieldValue.doubleValue();
      double minSpan = minSpanParam.doubleValue();

      if (minSpan <= 0) {
        return null;
      }

      // Implement the exact same magnitude-based minspan algorithm as the original
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
