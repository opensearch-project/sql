/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Remove field specified by the {@link LogicalRemove#removeList}.
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalRemove extends LogicalPlan {

  @Getter
  private final Set<ReferenceExpression> removeList;

  /**
   * Constructor of LogicalRemove.
   */
  public LogicalRemove(
      LogicalPlan child,
      Set<ReferenceExpression> removeList) {
    super(Collections.singletonList(child));
    this.removeList = removeList;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRemove(this, context);
  }
}
