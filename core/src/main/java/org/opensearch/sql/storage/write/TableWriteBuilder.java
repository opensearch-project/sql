/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.storage.write;

import static java.util.Collections.singletonList;

import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * A {@link TableWriteBuilder} represents transition state between logical planning and physical
 * planning for table write operator. The concrete implementation class gets involved in the logical
 * optimization through this abstraction and thus transform to specific {@link TableWriteOperator}
 * in a certain storage engine.
 */
public abstract class TableWriteBuilder extends LogicalPlan {

  /** Construct table write builder with child node. */
  public TableWriteBuilder(LogicalPlan child) {
    super(singletonList(child));
  }

  /**
   * Build table write operator with given child node.
   *
   * @param child child operator node
   * @return table write operator
   */
  public abstract TableWriteOperator build(PhysicalPlan child);

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitTableWriteBuilder(this, context);
  }
}
