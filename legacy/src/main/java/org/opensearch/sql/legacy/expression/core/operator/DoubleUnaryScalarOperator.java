/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.core.operator;

import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getDoubleValue;

import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.legacy.expression.model.ExprDoubleValue;
import org.opensearch.sql.legacy.expression.model.ExprValue;
import org.opensearch.sql.legacy.expression.model.ExprValueFactory;

/**
 * Unary Binary Scalar Operator take one {@link ExprValue} which have double value as arguments ans
 * return one {@link ExprDoubleValue} as result.
 */
@RequiredArgsConstructor
public class DoubleUnaryScalarOperator implements ScalarOperator {
  private final ScalarOperation op;
  private final Function<Double, Double> doubleFunc;

  @Override
  public ExprValue apply(List<ExprValue> exprValues) {
    ExprValue exprValue = exprValues.get(0);
    switch (exprValue.kind()) {
      case DOUBLE_VALUE:
      case INTEGER_VALUE:
      case LONG_VALUE:
      case FLOAT_VALUE:
        return ExprValueFactory.from(doubleFunc.apply(getDoubleValue(exprValue)));
      default:
        throw new RuntimeException(
            String.format("unexpected operation type: %s(%s)", op.name(), exprValue.kind()));
    }
  }

  @Override
  public String name() {
    return op.name();
  }
}
