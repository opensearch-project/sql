/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/** Logical plan that represent the flatten command. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalFlatten extends LogicalPlan {

  // TODO #3030: Implement

  public LogicalFlatten(List<LogicalPlan> childPlans) {
    super(childPlans);
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitFlatten(this, context);
  }
}
