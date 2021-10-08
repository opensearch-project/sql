/*
 * SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 *
 */

package org.opensearch.sql.expression;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.env.Environment;

/**
 * Named argument expression that represents function argument with name.
 */
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
