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
      Expression dataRange = translatedOperands.get(5);
      Expression maxValue = translatedOperands.get(6);

      // Extract bin type literal value at compile time
      org.apache.calcite.rex.RexNode binTypeNode = call.operands.get(1);
      String binType = null;

      if (binTypeNode instanceof org.apache.calcite.rex.RexLiteral) {
        binType = ((org.apache.calcite.rex.RexLiteral) binTypeNode).getValueAs(String.class);
      } else if (binTypeNode instanceof org.apache.calcite.rex.RexCall) {
        // Handle CAST operations wrapping literals
        org.apache.calcite.rex.RexCall castCall = (org.apache.calcite.rex.RexCall) binTypeNode;
        if (!castCall.getOperands().isEmpty()
            && castCall.getOperands().get(0) instanceof org.apache.calcite.rex.RexLiteral) {
          binType =
              ((org.apache.calcite.rex.RexLiteral) castCall.getOperands().get(0))
                  .getValueAs(String.class);
        }
      } else if (binTypeNode instanceof org.apache.calcite.rex.RexLocalRef) {
        // Handle local references by dereferencing to actual literal
        try {
          org.apache.calcite.rex.RexLocalRef localRef =
              (org.apache.calcite.rex.RexLocalRef) binTypeNode;
          java.lang.reflect.Field programField = translator.getClass().getDeclaredField("program");
          programField.setAccessible(true);
          org.apache.calcite.rex.RexProgram program =
              (org.apache.calcite.rex.RexProgram) programField.get(translator);
          org.apache.calcite.rex.RexNode referencedNode =
              program.getExprList().get(localRef.getIndex());
          if (referencedNode instanceof org.apache.calcite.rex.RexLiteral) {
            binType = ((org.apache.calcite.rex.RexLiteral) referencedNode).getValueAs(String.class);
          }
        } catch (Exception e) {
          // Reflection failed, bin type remains null
        }
      }

      if (binType != null) {
        String lowerType = binType.toLowerCase();
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

      // This should never happen since PPL always uses literal bin types
      throw new IllegalArgumentException(
          "Bin type must be a literal value (span, bins, or minspan)");
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
