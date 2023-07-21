/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * LogicalPaginate represents pagination operation for underlying plan.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
public class LogicalPaginate extends LogicalPlan {
  @Getter
  private final int pageSize;

  public LogicalPaginate(int pageSize, List<LogicalPlan> childPlans) {
    super(childPlans);
    this.pageSize = pageSize;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitPaginate(this, context);
  }
}
