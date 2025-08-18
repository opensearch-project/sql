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

/** Handler for day-based time spans. */
public class DaySpanHandler {

  public RexNode createExpression(RexNode fieldExpr, int intervalDays, CalcitePlanContext context) {

    // Extract date part (ignoring time component)
    RexNode inputDate = context.rexBuilder.makeCall(PPLBuiltinOperators.DATE, fieldExpr);

    // Calculate days since Unix epoch using DATEDIFF
    RexNode epochDate = context.relBuilder.literal(BinConstants.UNIX_EPOCH_DATE);
    RexNode daysSinceEpoch =
        context.rexBuilder.makeCall(PPLBuiltinOperators.DATEDIFF, inputDate, epochDate);

    // Find bin using modular arithmetic
    RexNode binStartDays = calculateBinStart(daysSinceEpoch, intervalDays, context);

    // Convert back to timestamp at midnight
    RexNode binStartDate =
        context.rexBuilder.makeCall(PPLBuiltinOperators.ADDDATE, epochDate, binStartDays);

    return context.rexBuilder.makeCall(PPLBuiltinOperators.TIMESTAMP, binStartDate);
  }

  private RexNode calculateBinStart(RexNode value, int interval, CalcitePlanContext context) {
    RexNode intervalLiteral = context.relBuilder.literal(interval);
    RexNode positionInCycle =
        context.relBuilder.call(SqlStdOperatorTable.MOD, value, intervalLiteral);
    return context.relBuilder.call(SqlStdOperatorTable.MINUS, value, positionInCycle);
  }
}
