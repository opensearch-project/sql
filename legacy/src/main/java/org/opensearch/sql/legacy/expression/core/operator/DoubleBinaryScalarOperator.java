/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.core.operator;

import static org.opensearch.sql.legacy.expression.model.ExprValueUtils.getDoubleValue;

import java.util.List;
import java.util.function.BiFunction;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.legacy.expression.model.ExprDoubleValue;
import org.opensearch.sql.legacy.expression.model.ExprValue;
import org.opensearch.sql.legacy.expression.model.ExprValueFactory;

/**
 * Double Binary Scalar Operator take two {@link ExprValue} which have double value as arguments ans
 * return one {@link ExprDoubleValue} as result.
 */
@RequiredArgsConstructor
public class DoubleBinaryScalarOperator implements ScalarOperator {
  private final ScalarOperation op;
  private final BiFunction<Double, Double, Double> doubleFunc;

  @Override
  public ExprValue apply(List<ExprValue> exprValues) {
    ExprValue exprValue1 = exprValues.get(0);
    ExprValue exprValue2 = exprValues.get(1);
    if (exprValue1.kind() != exprValue2.kind()) {
      throw new RuntimeException(
          String.format(
              "unexpected operation type: %s(%s,%s)",
              op.name(), exprValue1.kind(), exprValue2.kind()));
    }
    switch (exprValue1.kind()) {
      case DOUBLE_VALUE:
      case INTEGER_VALUE:
      case LONG_VALUE:
      case FLOAT_VALUE:
        return ExprValueFactory.from(
            doubleFunc.apply(getDoubleValue(exprValue1), getDoubleValue(exprValue2)));
      default:
        throw new RuntimeException(
            String.format(
                "unexpected operation type: %s(%s,%s)",
                op.name(), exprValue1.kind(), exprValue2.kind()));
    }
  }

  @Override
  public String name() {
    return op.name();
  }
}
