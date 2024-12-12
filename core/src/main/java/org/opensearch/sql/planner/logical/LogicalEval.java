/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Logical Evaluation represent the evaluation operation. The {@link LogicalEval#expressions} is a
 * list assignment operation. e.g. velocity = distance/speed, then the Pair is (velocity,
 * distance/speed).
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalEval extends LogicalPlan {

  @Getter private final List<Pair<ReferenceExpression, Expression>> expressions;

  /** Constructor of LogicalEval. */
  public LogicalEval(LogicalPlan child, List<Pair<ReferenceExpression, Expression>> expressions) {
    super(Collections.singletonList(child));
    this.expressions = expressions;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitEval(this, context);
  }
}
