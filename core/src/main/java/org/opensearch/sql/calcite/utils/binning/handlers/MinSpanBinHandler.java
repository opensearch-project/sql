/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.handlers;

import static org.apache.calcite.sql.SqlKind.LITERAL;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.MinSpanBin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.calcite.utils.binning.BinFieldValidator;
import org.opensearch.sql.calcite.utils.binning.BinHandler;
import org.opensearch.sql.calcite.utils.binning.BinnableField;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** Handler for minspan-based binning operations. */
public class MinSpanBinHandler implements BinHandler {

  @Override
  public RexNode createExpression(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    MinSpanBin minSpanBin = (MinSpanBin) node;

    // Create validated binnable field (validates that field is numeric or time-based)
    String fieldName = BinFieldValidator.extractFieldName(node);
    BinnableField field = new BinnableField(fieldExpr, fieldExpr.getType(), fieldName);

    // Minspan binning requires numeric fields
    if (!field.requiresNumericBinning()) {
      throw new IllegalArgumentException(
          "Minspan binning is only supported for numeric fields, not time-based fields");
    }

    RexNode minspanValue = visitor.analyze(minSpanBin.getMinspan(), context);

    if (!minspanValue.isA(LITERAL)) {
      throw new IllegalArgumentException("Minspan must be a literal value");
    }

    Number minspanNum = (Number) ((RexLiteral) minspanValue).getValue();
    double minspan = minspanNum.doubleValue();

    // Calculate data range using window functions
    RexNode minValue = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode maxValue = context.relBuilder.max(fieldExpr).over().toRex();
    RexNode dataRange = context.relBuilder.call(SqlStdOperatorTable.MINUS, maxValue, minValue);

    // Convert start/end parameters
    RexNode startValue = convertParameter(minSpanBin.getStart(), context);
    RexNode endValue = convertParameter(minSpanBin.getEnd(), context);

    // MINSPAN_BUCKET(field_value, min_span, data_range, max_value)
    RexNode minSpanParam = context.relBuilder.literal(minspan);

    return context.rexBuilder.makeCall(
        PPLBuiltinOperators.MINSPAN_BUCKET, fieldExpr, minSpanParam, dataRange, maxValue);
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
