/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage.write;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

/**
 * {@link TableWriteOperator} is the abstraction for data source to implement different physical
 * write operator on a data source. This is also to avoid "polluting" physical plan visitor by
 * concrete table scan implementation.
 */
@RequiredArgsConstructor
public abstract class TableWriteOperator extends PhysicalPlan {

  /** Input physical node. */
  protected final PhysicalPlan input;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitTableWrite(this, context);
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return List.of(input);
  }

  /**
   * Explain the execution plan.
   *
   * @return explain output
   */
  public abstract String explain();
}
