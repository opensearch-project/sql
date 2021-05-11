/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

/*
 *    Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License").
 *    You may not use this file except in compliance with the License.
 *    A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    or in the "license" file accompanying this file. This file is distributed
 *    on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *    express or implied. See the License for the specific language governing
 *    permissions and limitations under the License.
 *
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

/**
 * WHEN clause that consists of a condition and a result corresponding.
 */
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class WhenClause extends FunctionExpression {

  /**
   * Condition that must be a predicate.
   */
  private final Expression condition;

  /**
   * Result to return if condition is evaluated to true.
   */
  private final Expression result;

  /**
   * Initialize when clause.
   */
  public WhenClause(Expression condition, Expression result) {
    super(FunctionName.of("when"), ImmutableList.of(condition, result));
    this.condition = condition;
    this.result = result;
  }

  /**
   * Evaluate when condition.
   * @param valueEnv  value env
   * @return          is condition satisfied
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
