/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.time;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.binning.BinConstants;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

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
            context.rexBuilder.makeCall(
                SqlStdOperatorTable.PLUS,
                context.rexBuilder.makeCall(
                    SqlStdOperatorTable.MULTIPLY,
                    context.rexBuilder.makeCall(
                        SqlStdOperatorTable.MINUS, binStartMonth, context.relBuilder.literal(1)),
                    context.relBuilder.literal(31)),
                context.relBuilder.literal(1)));

    return context.rexBuilder.makeCall(
        PPLBuiltinOperators.DATE_FORMAT, tempDate, context.relBuilder.literal("%Y-%m"));
  }

  private RexNode calculateMonthsSinceEpoch(
      RexNode inputYear, RexNode inputMonth, CalcitePlanContext context) {
    RexNode yearsSinceEpoch =
        context.relBuilder.call(
            SqlStdOperatorTable.MINUS,
            inputYear,
            context.relBuilder.literal(BinConstants.UNIX_EPOCH_YEAR));
    RexNode monthsFromYears =
        context.relBuilder.call(
            SqlStdOperatorTable.MULTIPLY, yearsSinceEpoch, context.relBuilder.literal(12));
    return context.relBuilder.call(
        SqlStdOperatorTable.PLUS,
        monthsFromYears,
        context.relBuilder.call(
            SqlStdOperatorTable.MINUS, inputMonth, context.relBuilder.literal(1)));
  }

  private RexNode calculateBinStart(RexNode value, int interval, CalcitePlanContext context) {
    RexNode intervalLiteral = context.relBuilder.literal(interval);
    RexNode positionInCycle =
        context.relBuilder.call(SqlStdOperatorTable.MOD, value, intervalLiteral);
    return context.relBuilder.call(SqlStdOperatorTable.MINUS, value, positionInCycle);
  }

  private RexNode calculateBinStartYear(RexNode binStartMonths, CalcitePlanContext context) {
    return context.relBuilder.call(
        SqlStdOperatorTable.PLUS,
        context.relBuilder.literal(BinConstants.UNIX_EPOCH_YEAR),
        context.relBuilder.call(
            SqlStdOperatorTable.DIVIDE, binStartMonths, context.relBuilder.literal(12)));
  }

  private RexNode calculateBinStartMonth(RexNode binStartMonths, CalcitePlanContext context) {
    return context.relBuilder.call(
        SqlStdOperatorTable.PLUS,
        context.relBuilder.call(
            SqlStdOperatorTable.MOD, binStartMonths, context.relBuilder.literal(12)),
        context.relBuilder.literal(1));
  }
}
