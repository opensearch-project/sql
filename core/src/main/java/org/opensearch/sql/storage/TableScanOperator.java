/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage;

import java.util.Collections;
import java.util.List;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * Abstract table scan class for different storage to implement. This is also to avoid "polluting"
 * physical plan visitor by concrete table scan implementation.
 */
public abstract class TableScanOperator extends PhysicalPlan {

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitTableScan(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return Collections.emptyList();
  }

  /**
   * Force cleanup of server-side resources (e.g. PIT, scroll) regardless of pagination state. Used
   * when the client explicitly closes a cursor mid-pagination. Default implementation delegates to
   * {@link #close()}.
   */
  public void forceClose() {
    close();
  }

  /**
   * Explain the execution plan.
   *
   * @return execution plan.
   */
  public abstract String explain();
}
