/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.core.operator;

import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getDoubleValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getFloatValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getIntegerValue;
import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getLongValue;

import java.util.List;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.legacy.expression.model.ExprValue;
import org.opensearch.sql.legacy.expression.model.ExprValueFactory;

/**
 * Unary Scalar Operator take one {@link ExprValue} as arguments ans return one {@link ExprValue} as
 * result.
 */
@RequiredArgsConstructor
public class UnaryScalarOperator implements ScalarOperator {
  private final ScalarOperation op;
  private final Function<Integer, Integer> integerFunc;
  private final Function<Long, Long> longFunc;
  private final Function<Double, Double> doubleFunc;
  private final Function<Float, Float> floatFunc;

  @Override
  public ExprValue apply(List<ExprValue> exprValues) {
    ExprValue exprValue = exprValues.get(0);
    switch (exprValue.kind()) {
      case DOUBLE_VALUE:
        return ExprValueFactory.from(doubleFunc.apply(getDoubleValue(exprValue)));
      case INTEGER_VALUE:
        return ExprValueFactory.from(integerFunc.apply(getIntegerValue(exprValue)));
      case LONG_VALUE:
        return ExprValueFactory.from(longFunc.apply(getLongValue(exprValue)));
      case FLOAT_VALUE:
        return ExprValueFactory.from(floatFunc.apply(getFloatValue(exprValue)));
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
