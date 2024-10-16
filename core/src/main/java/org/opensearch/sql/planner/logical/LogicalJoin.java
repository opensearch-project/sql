/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.expression.Expression;

@ToString
@EqualsAndHashCode(callSuper = true)
@Getter
public class LogicalJoin extends LogicalPlan {
  private final LogicalPlan left;
  private final LogicalPlan right;
  private final Join.JoinType type;
  private final Expression condition;

  public LogicalJoin(
      LogicalPlan left, LogicalPlan right, Join.JoinType type, Expression condition) {
    super(ImmutableList.of(left, right));
    this.left = left;
    this.right = right;
    this.type = type;
    this.condition = condition;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitJoin(this, context);
  }
}
