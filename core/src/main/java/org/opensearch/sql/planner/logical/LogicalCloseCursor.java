/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * A logical plan node which wraps {@link org.opensearch.sql.planner.LogicalCursor} and represent a
 * cursor close operation.
 */
@ToString
@EqualsAndHashCode(callSuper = false)
public class LogicalCloseCursor extends LogicalPlan {

  public LogicalCloseCursor(LogicalPlan child) {
    super(List.of(child));
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitCloseCursor(this, context);
  }
}
