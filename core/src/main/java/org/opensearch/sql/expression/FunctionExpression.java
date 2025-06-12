/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.expression.function.FunctionImplementation;
import org.opensearch.sql.expression.function.FunctionName;

/** Function Expression. */
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
@ToString
public abstract class FunctionExpression implements Expression, FunctionImplementation {
  @Getter private final FunctionName functionName;

  @Getter private final List<Expression> arguments;

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitFunction(this, context);
  }
}
