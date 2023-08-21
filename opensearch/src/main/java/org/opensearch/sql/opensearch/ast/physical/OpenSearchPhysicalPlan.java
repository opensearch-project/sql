/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.ast.physical;

import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

public abstract class OpenSearchPhysicalPlan extends PhysicalPlan {
  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    if (visitor instanceof OpenSearchPhysicalPlanNodeVisitor) {
      return accept((OpenSearchPhysicalPlanNodeVisitor<R, C>) visitor, context);
    }
    throw new RuntimeException();
  }

  public abstract <R, C> R accept(OpenSearchPhysicalPlanNodeVisitor<R, C> visitor, C context);
}
