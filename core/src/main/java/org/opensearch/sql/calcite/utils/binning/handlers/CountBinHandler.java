/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.handlers;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.CountBin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.calcite.utils.binning.BinConstants;
import org.opensearch.sql.calcite.utils.binning.BinHandler;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** Handler for bins-based (count) binning operations. */
public class CountBinHandler implements BinHandler {

  @Override
  public RexNode createExpression(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    CountBin countBin = (CountBin) node;

    Integer requestedBins = countBin.getBins();
    if (requestedBins == null) {
      requestedBins = BinConstants.DEFAULT_BINS;
    }

    validateBinsCount(requestedBins);

    // Calculate data range using window functions
    RexNode minValue = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode maxValue = context.relBuilder.max(fieldExpr).over().toRex();
    RexNode dataRange = context.relBuilder.call(SqlStdOperatorTable.MINUS, maxValue, minValue);

    // Convert start/end parameters
    RexNode startValue = convertParameter(countBin.getStart(), context);
    RexNode endValue = convertParameter(countBin.getEnd(), context);

    // BIN_CALCULATOR(field_value, 'bins', num_bins, start, end, data_range, max_value)
    RexNode binType = context.relBuilder.literal("bins");
    RexNode numBins = context.relBuilder.literal(requestedBins);

    return context.rexBuilder.makeCall(
        PPLBuiltinOperators.BIN_CALCULATOR,
        fieldExpr,
        binType,
        numBins,
        startValue,
        endValue,
        dataRange,
        maxValue);
  }

  private void validateBinsCount(int bins) {
    if (bins < BinConstants.MIN_BINS) {
      throw new IllegalArgumentException(
          "The bins parameter must be at least " + BinConstants.MIN_BINS + ", got: " + bins);
    }
    if (bins > BinConstants.MAX_BINS) {
      throw new IllegalArgumentException(
          "The bins parameter must not exceed " + BinConstants.MAX_BINS + ", got: " + bins);
    }
  }

  private RexNode convertParameter(
      org.opensearch.sql.ast.expression.UnresolvedExpression expr, CalcitePlanContext context) {

    if (expr == null) {
      return context.relBuilder.literal(-1); // Sentinel value
    }

    if (expr instanceof Literal) {
      Literal literal = (Literal) expr;
      Object value = literal.getValue();
      if (value instanceof Number) {
        return context.relBuilder.literal(((Number) value).doubleValue());
      } else {
        return context.relBuilder.literal(value);
      }
    }

    throw new IllegalArgumentException("Expected literal expression, got: " + expr.getClass());
  }

  @Override
  public boolean usesWindowFunctions() {
    return true;
  }
}
