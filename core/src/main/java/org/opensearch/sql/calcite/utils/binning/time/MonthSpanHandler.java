/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.time;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.binning.BinConstants;
import org.opensearch.sql.expression.function.BuiltinFunctionName;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.expression.function.PPLFuncImpTable;

/** Handler for month-based time spans using SPL Monthly Binning Algorithm. */
public class MonthSpanHandler {

  public RexNode createExpression(
      RexNode fieldExpr, int intervalMonths, CalcitePlanContext context) {

    // Extract year and month from input timestamp
    RexNode inputYear = context.rexBuilder.makeCall(PPLBuiltinOperators.YEAR, fieldExpr);
    RexNode inputMonth = context.rexBuilder.makeCall(PPLBuiltinOperators.MONTH, fieldExpr);

    // Calculate months since Unix epoch (January 1970)
    RexNode monthsSinceEpoch = calculateMonthsSinceEpoch(inputYear, inputMonth, context);

    // Find bin start using modular arithmetic
    RexNode binStartMonths = calculateBinStart(monthsSinceEpoch, intervalMonths, context);

    // Convert back to year and month
    RexNode binStartYear = calculateBinStartYear(binStartMonths, context);
    RexNode binStartMonth = calculateBinStartMonth(binStartMonths, context);

    // Format as YYYY-MM string
    RexNode tempDate =
        context.rexBuilder.makeCall(
            PPLBuiltinOperators.MAKEDATE,
            binStartYear,
            PPLFuncImpTable.INSTANCE.resolve(
                context.rexBuilder,
                BuiltinFunctionName.ADD,
                PPLFuncImpTable.INSTANCE.resolve(
                    context.rexBuilder,
                    BuiltinFunctionName.MULTIPLY,
                    PPLFuncImpTable.INSTANCE.resolve(
                        context.rexBuilder,
                        BuiltinFunctionName.SUBTRACT,
                        binStartMonth,
                        context.relBuilder.literal(1)),
                    context.relBuilder.literal(31)),
                context.relBuilder.literal(1)));

    return context.rexBuilder.makeCall(
        PPLBuiltinOperators.DATE_FORMAT, tempDate, context.relBuilder.literal("%Y-%m"));
  }

  private RexNode calculateMonthsSinceEpoch(
      RexNode inputYear, RexNode inputMonth, CalcitePlanContext context) {
    RexNode yearsSinceEpoch =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.SUBTRACT,
            inputYear,
            context.relBuilder.literal(BinConstants.UNIX_EPOCH_YEAR));
    RexNode monthsFromYears =
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.MULTIPLY,
            yearsSinceEpoch,
            context.relBuilder.literal(12));
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder,
        BuiltinFunctionName.ADD,
        monthsFromYears,
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.SUBTRACT,
            inputMonth,
            context.relBuilder.literal(1)));
  }

  private RexNode calculateBinStart(RexNode value, int interval, CalcitePlanContext context) {
    RexNode intervalLiteral = context.relBuilder.literal(interval);
    RexNode positionInCycle =
        context.relBuilder.call(SqlStdOperatorTable.MOD, value, intervalLiteral);
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder, BuiltinFunctionName.SUBTRACT, value, positionInCycle);
  }

  private RexNode calculateBinStartYear(RexNode binStartMonths, CalcitePlanContext context) {
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder,
        BuiltinFunctionName.ADD,
        context.relBuilder.literal(BinConstants.UNIX_EPOCH_YEAR),
        PPLFuncImpTable.INSTANCE.resolve(
            context.rexBuilder,
            BuiltinFunctionName.DIVIDE,
            binStartMonths,
            context.relBuilder.literal(12)));
  }

  private RexNode calculateBinStartMonth(RexNode binStartMonths, CalcitePlanContext context) {
    return PPLFuncImpTable.INSTANCE.resolve(
        context.rexBuilder,
        BuiltinFunctionName.ADD,
        context.relBuilder.call(
            SqlStdOperatorTable.MOD, binStartMonths, context.relBuilder.literal(12)),
        context.relBuilder.literal(1));
  }
}
