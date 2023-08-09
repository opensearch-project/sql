/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.List;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.planner.PlanNode;

/** The abstract base class for all the Logical Plan node. */
@EqualsAndHashCode(callSuper = false)
public abstract class LogicalPlan implements PlanNode<LogicalPlan> {

  private List<LogicalPlan> childPlans;

  public LogicalPlan(List<LogicalPlan> childPlans) {
    this.childPlans = childPlans;
  }

  /**
   * Accept the {@link LogicalPlanNodeVisitor}.
   *
   * @param visitor visitor.
   * @param context visitor context.
   * @param <R> returned object type.
   * @param <C> context type.
   * @return returned object.
   */
  public abstract <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context);

  public LogicalPlan replaceChildPlans(List<LogicalPlan> childPlans) {
    this.childPlans = childPlans;
    return this;
  }

  @Override
  public List<LogicalPlan> getChild() {
    return childPlans;
  }
}
