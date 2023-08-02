/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.conditional.cases;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ExpressionNodeVisitor;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;

/** WHEN clause that consists of a condition and a result corresponding. */
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class WhenClause extends FunctionExpression {

  /** Condition that must be a predicate. */
  private final Expression condition;

  /** Result to return if condition is evaluated to true. */
  private final Expression result;

  /** Initialize when clause. */
  public WhenClause(Expression condition, Expression result) {
    super(FunctionName.of("when"), ImmutableList.of(condition, result));
    this.condition = condition;
    this.result = result;
  }

  /**
   * Evaluate when condition.
   *
   * @param valueEnv value env
   * @return is condition satisfied
   */
  public boolean isTrue(Environment<Expression, ExprValue> valueEnv) {
    ExprValue result = condition.valueOf(valueEnv);
    if (result.isMissing() || result.isNull()) {
      return false;
    }
    return result.booleanValue();
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    return result.valueOf(valueEnv);
  }

  @Override
  public ExprType type() {
    return result.type();
  }

  @Override
  public <T, C> T accept(ExpressionNodeVisitor<T, C> visitor, C context) {
    return visitor.visitWhen(this, context);
  }
}
