/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.ast.logical;

import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

import java.util.List;

public abstract class OpenSearchLogicalPlan extends LogicalPlan {

  public OpenSearchLogicalPlan(List<LogicalPlan> childPlans) {
    super(childPlans);
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    if (visitor instanceof OpenSearchLogicalPlanNodeVisitor) {
      return accept((OpenSearchLogicalPlanNodeVisitor<R, C>) visitor, context);
    }
    return null;
  }

  public abstract <R, C> R accept(OpenSearchLogicalPlanNodeVisitor<R, C> visitor, C context);
}
