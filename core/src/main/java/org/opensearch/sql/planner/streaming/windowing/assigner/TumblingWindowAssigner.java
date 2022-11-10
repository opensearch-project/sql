/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import static org.opensearch.sql.expression.DSL.add;
import static org.opensearch.sql.expression.DSL.adddate;
import static org.opensearch.sql.expression.DSL.interval;
import static org.opensearch.sql.expression.DSL.literal;

import java.util.Collections;
import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.planner.streaming.windowing.Window;

/**
 * A tumbling window assigner assigns a single window per input value without overlap.
 */
public class TumblingWindowAssigner implements WindowAssigner {

  /** Window size that maybe numeric or time interval. */
  private final SpanExpression windowingExpr;

  /**
   * Create tumbling window assigner with the given window size.
   *
   * @param windowingExpr window size in millisecond
   */
  public TumblingWindowAssigner(SpanExpression windowingExpr) {
    this.windowingExpr = windowingExpr;
  }

  @Override
  public List<Window> assign(ExprValue value) {
    Environment<Expression, ExprValue> valueEnv = value.bindingTuples();
    ExprValue lowerBound = windowingExpr.valueOf(valueEnv);
    ExprValue upperBound = getUpperBound(lowerBound);
    return Collections.singletonList(
        new Window(lowerBound, upperBound));
  }

  private ExprValue getUpperBound(ExprValue lowerBound) {
    ExprValue upperBound;
    if (lowerBound.isNumber()) {
      ExprValue windowSize = windowingExpr.getValue().valueOf();
      upperBound = add(literal(lowerBound), literal(windowSize)).valueOf();
    } else {
      FunctionExpression windowSize = interval(windowingExpr.getValue(), literal("minute"));
      upperBound = adddate(literal(lowerBound), windowSize).valueOf();
    }
    return upperBound;
  }
}
