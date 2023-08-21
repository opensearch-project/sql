/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import java.io.Serializable;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

/** The definition of the resolved expression. */
public interface Expression extends Serializable {

  /** Evaluate the value of expression that does not depend on value environment. */
  default ExprValue valueOf() {
    return valueOf(null);
  }

  /** Evaluate the value of expression in the value environment. */
  ExprValue valueOf(Environment<Expression, ExprValue> valueEnv);

  /** The type of the expression. */
  ExprType type();

  /**
   * Accept a visitor to visit current expression node.
   *
   * @param visitor visitor
   * @param context context
   * @param <T> result type
   * @param <C> context type
   * @return result accumulated by visitor when visiting
   */
  <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context);
}
