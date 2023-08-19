/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.core.builder;

import java.util.Arrays;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.legacy.expression.core.Expression;
import org.opensearch.sql.legacy.expression.core.operator.ScalarOperator;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.expression.model.ExprValue;

/** The definition of the Expression Builder which has two arguments. */
@RequiredArgsConstructor
public class BinaryExpressionBuilder implements ExpressionBuilder {
  private final ScalarOperator op;

  /**
   * Build the expression with two {@link Expression} as arguments.
   *
   * @param expressionList expression list.
   * @return expression.
   */
  @Override
  public Expression build(List<Expression> expressionList) {
    Expression e1 = expressionList.get(0);
    Expression e2 = expressionList.get(1);

    return new Expression() {
      @Override
      public ExprValue valueOf(BindingTuple tuple) {
        return op.apply(Arrays.asList(e1.valueOf(tuple), e2.valueOf(tuple)));
      }

      @Override
      public String toString() {
        return String.format("%s(%s,%s)", op.name(), e1, e2);
      }
    };
  }
}
