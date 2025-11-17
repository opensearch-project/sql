/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.handlers;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.DefaultBin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.calcite.utils.BinTimeSpanUtils;
import org.opensearch.sql.calcite.utils.binning.BinFieldValidator;
import org.opensearch.sql.calcite.utils.binning.BinHandler;
import org.opensearch.sql.calcite.utils.binning.BinnableField;
import org.opensearch.sql.calcite.utils.binning.RangeFormatter;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/** Handler for default binning when no parameters are specified. */
public class DefaultBinHandler implements BinHandler {

  @Override
  public RexNode createExpression(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    DefaultBin defaultBin = (DefaultBin) node;
    String fieldName = BinFieldValidator.extractFieldName(node);

    // Create validated binnable field (validates that field is numeric or time-based)
    BinnableField field = new BinnableField(fieldExpr, fieldExpr.getType(), fieldName);

    // Use time-based binning for time fields
    if (field.isTimeBased()) {
      BinFieldValidator.validateFieldExists(fieldName, context);
      return BinTimeSpanUtils.createBinTimeSpanExpression(fieldExpr, 1, "h", 0, context);
    }

    // Use numeric binning for numeric fields
    return createNumericDefaultBinning(fieldExpr, context);
  }

  private RexNode createNumericDefaultBinning(RexNode fieldExpr, CalcitePlanContext context) {

    // Calculate data range
    RexNode minValue = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode maxValue = context.relBuilder.max(fieldExpr).over().toRex();
    RexNode dataRange =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.SUBTRACT, maxValue, minValue);

    // Calculate magnitude-based width
    RexNode log10Range = context.relBuilder.call(SqlStdOperatorTable.LOG10, dataRange);
    RexNode magnitude = context.relBuilder.call(SqlStdOperatorTable.FLOOR, log10Range);

    RexNode tenLiteral = context.relBuilder.literal(10.0);
    RexNode defaultWidth =
        context.relBuilder.call(SqlStdOperatorTable.POWER, tenLiteral, magnitude);

    RexNode widthInt = context.relBuilder.call(SqlStdOperatorTable.FLOOR, defaultWidth);

    // Calculate bin value
    RexNode binStartValue = calculateBinValue(fieldExpr, widthInt, context);
    RexNode binEndValue =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.ADD, binStartValue, widthInt);

    return RangeFormatter.createRangeString(binStartValue, binEndValue, context);
  }

  private RexNode calculateBinValue(RexNode fieldExpr, RexNode width, CalcitePlanContext context) {

    RexNode divided =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.DIVIDE, fieldExpr, width);

    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);

    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder, BuiltinFunctionName.MULTIPLY, floored, width);
  }
}
