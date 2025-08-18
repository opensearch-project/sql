/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils.binning.handlers;

import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;

/** Helper for creating numeric span expressions. */
public class NumericSpanHelper {

  /** Creates numeric span expression using BIN_CALCULATOR UDF. */
  public RexNode createNumericSpanExpression(
      RexNode fieldExpr, int span, CalcitePlanContext context) {

    RexNode spanValue = context.relBuilder.literal(span);
    return createExpression(fieldExpr, spanValue, context);
  }

  /** Creates numeric span expression for floating point spans. */
  public RexNode createNumericSpanExpression(
      RexNode fieldExpr, double span, CalcitePlanContext context) {

    RexNode spanValue = context.relBuilder.literal(span);
    return createExpression(fieldExpr, spanValue, context);
  }

  private RexNode createExpression(
      RexNode fieldExpr, RexNode spanValue, CalcitePlanContext context) {

    // BIN_CALCULATOR(field_value, 'span', span_value, -1, -1, -1, -1)
    RexNode binType = context.relBuilder.literal("span");
    RexNode sentinel = context.relBuilder.literal(-1);

    return context.rexBuilder.makeCall(
        PPLBuiltinOperators.BIN_CALCULATOR,
        fieldExpr,
        binType,
        spanValue,
        sentinel, // start (not used)
        sentinel, // end (not used)
        sentinel, // dataRange (not used)
        sentinel); // maxValue (not used)
  }
}
