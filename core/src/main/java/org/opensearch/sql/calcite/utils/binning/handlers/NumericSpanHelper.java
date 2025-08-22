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

    // SPAN_BUCKET(field_value, span_value)
    return context.rexBuilder.makeCall(PPLBuiltinOperators.SPAN_BUCKET, fieldExpr, spanValue);
  }
}
