/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.handlers;

import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.RangeBin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.calcite.utils.binning.BinFieldValidator;
import org.opensearch.sql.calcite.utils.binning.BinHandler;
import org.opensearch.sql.calcite.utils.binning.BinnableField;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** Handler for range-based binning (start/end parameters only). */
public class RangeBinHandler implements BinHandler {

  @Override
  public RexNode createExpression(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    RangeBin rangeBin = (RangeBin) node;

    // Create validated binnable field (validates that field is numeric or time-based)
    String fieldName = BinFieldValidator.extractFieldName(node);
    BinnableField field = new BinnableField(fieldExpr, fieldExpr.getType(), fieldName);

    // Range binning requires numeric fields
    if (!field.requiresNumericBinning()) {
      throw new IllegalArgumentException(
          "Range binning (start/end) is only supported for numeric fields, not time-based fields");
    }

    // Simple MIN/MAX calculation - cleaner than complex CASE expressions
    RexNode dataMin = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode dataMax = context.relBuilder.max(fieldExpr).over().toRex();

    // Convert start/end parameters
    RexNode startParam = convertParameter(rangeBin.getStart(), context, visitor);
    RexNode endParam = convertParameter(rangeBin.getEnd(), context, visitor);

    // Use RANGE_BUCKET with data bounds and user parameters
    return context.rexBuilder.makeCall(
        PPLBuiltinOperators.RANGE_BUCKET, fieldExpr, dataMin, dataMax, startParam, endParam);
  }

  private RexNode convertParameter(
      org.opensearch.sql.ast.expression.UnresolvedExpression expr,
      CalcitePlanContext context,
      CalciteRexNodeVisitor visitor) {

    if (expr == null) {
      return context.relBuilder.literal(null);
    }

    return visitor.analyze(expr, context);
  }
}
