/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.watermark;

import static org.opensearch.sql.expression.DSL.greater;
import static org.opensearch.sql.expression.DSL.literal;
import static org.opensearch.sql.expression.DSL.subdate;
import static org.opensearch.sql.expression.DSL.subtract;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;

/**
 * Watermark generator that generates watermark with bounded out-of-order delay.
 */
@RequiredArgsConstructor
public class BoundedOutOfOrderWatermarkGenerator implements WatermarkGenerator {

  /** The maximum out-of-order value allowed. */
  private final Expression maxOutOfOrderAllowed;

  /** The maximum timestamp value seen so far. */
  private ExprValue maxTimestamp;

  @Override
  public ExprValue generate(ExprValue value) {
    if (isGreaterThanMaxTimestamp(value)) {
      maxTimestamp = value;
    }
    return generateWatermark();
  }

  private boolean isGreaterThanMaxTimestamp(ExprValue value) {
    if (maxTimestamp == null) {
      return true;
    }
    return greater(literal(value), literal(maxTimestamp))
        .valueOf().booleanValue();
  }

  private ExprValue generateWatermark() {
    FunctionExpression function;
    if (maxTimestamp.isNumber()) {
      function = subtract(literal(maxTimestamp), maxOutOfOrderAllowed);
    } else {
      function = subdate(literal(maxTimestamp), maxOutOfOrderAllowed);
    }
    return function.valueOf();
  }
}
