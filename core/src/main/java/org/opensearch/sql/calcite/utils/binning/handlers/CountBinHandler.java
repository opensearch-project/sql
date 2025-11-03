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
import org.opensearch.sql.calcite.utils.binning.BinFieldValidator;
import org.opensearch.sql.calcite.utils.binning.BinHandler;
import org.opensearch.sql.calcite.utils.binning.BinnableField;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** Handler for bins-based (count) binning operations. */
public class CountBinHandler implements BinHandler {

  @Override
  public RexNode createExpression(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    CountBin countBin = (CountBin) node;

    // Create validated binnable field (validates that field is numeric or time-based)
    // Note: bins parameter works with both numeric and time-based fields
    String fieldName = BinFieldValidator.extractFieldName(node);
    BinnableField field = new BinnableField(fieldExpr, fieldExpr.getType(), fieldName);

    Integer requestedBins = countBin.getBins();
    if (requestedBins == null) {
      requestedBins = BinConstants.DEFAULT_BINS;
    }

    // Calculate data range using window functions
    RexNode minValue = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode maxValue = context.relBuilder.max(fieldExpr).over().toRex();
    RexNode dataRange = context.relBuilder.call(SqlStdOperatorTable.MINUS, maxValue, minValue);

    // Convert start/end parameters
    RexNode startValue = convertParameter(countBin.getStart(), context);
    RexNode endValue = convertParameter(countBin.getEnd(), context);

    // WIDTH_BUCKET(field_value, num_bins, data_range, max_value)
    RexNode numBins = context.relBuilder.literal(requestedBins);

    return context.rexBuilder.makeCall(
        PPLBuiltinOperators.WIDTH_BUCKET, fieldExpr, numBins, dataRange, maxValue);
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
}
