/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.expression.core.builder;

import static java.util.Collections.singletonList;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.legacy.expression.core.Expression;
import org.opensearch.sql.legacy.expression.core.operator.ScalarOperator;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.expression.model.ExprValue;

/** The definition of the Expression Builder which has one argument. */
@RequiredArgsConstructor
public class UnaryExpressionBuilder implements ExpressionBuilder {
  private final ScalarOperator op;

  /**
   * Build the expression with two {@link Expression} as arguments.
   *
   * @param expressionList expression list.
   * @return expression.
   */
  @Override
  public Expression build(List<Expression> expressionList) {
    Expression expression = expressionList.get(0);

    return new Expression() {
      @Override
      public ExprValue valueOf(BindingTuple tuple) {
        return op.apply(singletonList(expression.valueOf(tuple)));
      }

      @Override
      public String toString() {
        return String.format("%s(%s)", op.name(), expression);
      }
    };
  }
}
