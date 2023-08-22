/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

/** Named argument expression that represents function argument with name. */
@RequiredArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class NamedArgumentExpression implements Expression {
  private final String argName;
  private final Expression value;

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    return value.valueOf(valueEnv);
  }

  @Override
  public ExprType type() {
    return value.type();
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitNamedArgument(this, context);
  }
}
