/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage.bindingtuple;

import org.opensearch.sql.data.model.ExprMissingValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.env.Environment;

/**
 * BindingTuple represents the a relationship between bindingName and ExprValue. e.g. The operation
 * output column name is bindingName, the value is the ExprValue.
 */
public abstract class BindingTuple implements Environment<Expression, ExprValue> {
  public static final BindingTuple EMPTY =
      new BindingTuple() {
        @Override
        public ExprValue resolve(ReferenceExpression ref) {
          return ExprMissingValue.of();
        }
      };

  /** Resolve {@link Expression} in the BindingTuple environment. */
  @Override
  public ExprValue resolve(Expression var) {
    if (var instanceof ReferenceExpression) {
      return resolve(((ReferenceExpression) var));
    } else {
      throw new ExpressionEvaluationException(String.format("can resolve expression: %s", var));
    }
  }

  /** Resolve the {@link ReferenceExpression} in BindingTuple context. */
  public abstract ExprValue resolve(ReferenceExpression ref);
}
