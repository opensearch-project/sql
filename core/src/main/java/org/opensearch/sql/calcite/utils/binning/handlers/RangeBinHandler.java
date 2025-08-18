/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.handlers;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.RangeBin;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRexNodeVisitor;
import org.opensearch.sql.calcite.utils.binning.BinHandler;
import org.opensearch.sql.calcite.utils.binning.RangeFormatter;

/** Handler for range-based binning (start/end parameters only). */
public class RangeBinHandler implements BinHandler {

  @Override
  public RexNode createExpression(
      Bin node, RexNode fieldExpr, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    RangeBin rangeBin = (RangeBin) node;

    // Calculate min/max values
    RexNode minValue = context.relBuilder.min(fieldExpr).over().toRex();
    RexNode maxValue = context.relBuilder.max(fieldExpr).over().toRex();

    // Determine effective range
    RexNode effectiveMin = calculateEffectiveMin(rangeBin, minValue, context, visitor);
    RexNode effectiveMax = calculateEffectiveMax(rangeBin, maxValue, context, visitor);

    RexNode effectiveRange =
        context.relBuilder.call(SqlStdOperatorTable.MINUS, effectiveMax, effectiveMin);

    // Calculate appropriate width using log scale
    RexNode selectedWidth = calculateWidth(effectiveRange, context);

    // Calculate first bin start
    RexNode firstBinStart = calculateFirstBinStart(effectiveMin, selectedWidth, context);

    // Calculate bin value for current field
    RexNode binValue = calculateBinValue(fieldExpr, selectedWidth, firstBinStart, context);

    // Create range string
    RexNode binEnd = context.relBuilder.call(SqlStdOperatorTable.PLUS, binValue, selectedWidth);

    return RangeFormatter.createRangeString(binValue, binEnd, selectedWidth, context);
  }

  private RexNode calculateEffectiveMin(
      RangeBin node, RexNode minValue, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    if (node.getStart() == null) {
      return minValue;
    }

    RexNode startValue = visitor.analyze(node.getStart(), context);
    return context.relBuilder.call(
        SqlStdOperatorTable.CASE,
        context.relBuilder.call(SqlStdOperatorTable.LESS_THAN, startValue, minValue),
        startValue,
        minValue);
  }

  private RexNode calculateEffectiveMax(
      RangeBin node, RexNode maxValue, CalcitePlanContext context, CalciteRexNodeVisitor visitor) {

    if (node.getEnd() == null) {
      return maxValue;
    }

    RexNode endValue = visitor.analyze(node.getEnd(), context);
    return context.relBuilder.call(
        SqlStdOperatorTable.CASE,
        context.relBuilder.call(SqlStdOperatorTable.GREATER_THAN, endValue, maxValue),
        endValue,
        maxValue);
  }

  private RexNode calculateWidth(RexNode effectiveRange, CalcitePlanContext context) {
    RexNode log10Range = context.relBuilder.call(SqlStdOperatorTable.LOG10, effectiveRange);
    RexNode floorLog = context.relBuilder.call(SqlStdOperatorTable.FLOOR, log10Range);

    RexNode isExactPowerOf10 =
        context.relBuilder.call(SqlStdOperatorTable.EQUALS, log10Range, floorLog);

    RexNode adjustedMagnitude =
        context.relBuilder.call(
            SqlStdOperatorTable.CASE,
            isExactPowerOf10,
            context.relBuilder.call(
                SqlStdOperatorTable.MINUS, floorLog, context.relBuilder.literal(1.0)),
            floorLog);

    return context.relBuilder.call(
        SqlStdOperatorTable.POWER, context.relBuilder.literal(10.0), adjustedMagnitude);
  }

  private RexNode calculateFirstBinStart(
      RexNode effectiveMin, RexNode selectedWidth, CalcitePlanContext context) {

    return context.relBuilder.call(
        SqlStdOperatorTable.MULTIPLY,
        context.relBuilder.call(
            SqlStdOperatorTable.FLOOR,
            context.relBuilder.call(SqlStdOperatorTable.DIVIDE, effectiveMin, selectedWidth)),
        selectedWidth);
  }

  private RexNode calculateBinValue(
      RexNode fieldExpr, RexNode selectedWidth, RexNode firstBinStart, CalcitePlanContext context) {

    RexNode adjustedField =
        context.relBuilder.call(SqlStdOperatorTable.MINUS, fieldExpr, firstBinStart);

    RexNode divided =
        context.relBuilder.call(SqlStdOperatorTable.DIVIDE, adjustedField, selectedWidth);

    RexNode floored = context.relBuilder.call(SqlStdOperatorTable.FLOOR, divided);

    RexNode binIndex =
        context.relBuilder.call(SqlStdOperatorTable.MULTIPLY, floored, selectedWidth);

    return context.relBuilder.call(SqlStdOperatorTable.PLUS, binIndex, firstBinStart);
  }

  @Override
  public boolean usesWindowFunctions() {
    return true;
  }
}
