/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.handlers;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.binning.BinConstants;
import org.opensearch.sql.calcite.utils.binning.RangeFormatter;
import org.opensearch.sql.calcite.utils.binning.SpanInfo;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/** Helper for creating logarithmic span expressions. */
public class LogSpanHelper {

  /** Creates logarithmic span expression. */
  public RexNode createLogSpanExpression(
      RexNode fieldExpr, SpanInfo spanInfo, CalcitePlanContext context) {

    double base = spanInfo.getBase();
    double coefficient = spanInfo.getCoefficient();

    // Check if value is positive
    RexNode positiveCheck =
        context.relBuilder.call(
            SqlStdOperatorTable.GREATER_THAN, fieldExpr, context.relBuilder.literal(0.0));

    // Apply coefficient if needed
    RexNode adjustedField = fieldExpr;
    if (coefficient != 1.0) {
      adjustedField =
          PPLFuncImpTable.INSTANCE.resolve(
              context.rexBuilder,
              BuiltinFunctionName.DIVIDE,
              fieldExpr,
              context.relBuilder.literal(coefficient));
    }

    // Calculate log_base(adjusted_field)
    RexNode lnField = context.relBuilder.call(SqlStdOperatorTable.LN, adjustedField);
    RexNode lnBase = context.relBuilder.literal(Math.log(base));
    RexNode logValue =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.DIVIDE, lnField, lnBase);

    // Get bin number
    RexNode binNumber = context.relBuilder.call(SqlStdOperatorTable.FLOOR, logValue);

    // Calculate bounds
    RexNode baseNode = context.relBuilder.literal(base);
    RexNode coefficientNode = context.relBuilder.literal(coefficient);

    RexNode basePowerBin = context.relBuilder.call(SqlStdOperatorTable.POWER, baseNode, binNumber);
    RexNode lowerBound =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MULTIPLY, coefficientNode, basePowerBin);

    RexNode binPlusOne =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.ADD,
            binNumber,
            context.relBuilder.literal(1.0));
    RexNode basePowerBinPlusOne =
        context.relBuilder.call(SqlStdOperatorTable.POWER, baseNode, binPlusOne);
    RexNode upperBound =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder, BuiltinFunctionName.MULTIPLY, coefficientNode, basePowerBinPlusOne);

    // Create range string
    RexNode rangeStr = RangeFormatter.createRangeString(lowerBound, upperBound, context);

    // Return range for positive values, "Invalid" for non-positive
    return context.relBuilder.call(
        SqlStdOperatorTable.CASE,
        positiveCheck,
        rangeStr,
        context.relBuilder.literal(BinConstants.INVALID_CATEGORY));
  }
}
