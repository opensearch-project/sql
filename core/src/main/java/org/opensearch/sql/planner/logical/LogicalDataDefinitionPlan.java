/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.ddl.DataDefinitionTask;

/**
 * Logical node for data definition plan.
 */
@EqualsAndHashCode(callSuper = true)
@Getter
@ToString
public class LogicalDataDefinitionPlan extends LogicalPlan {

  private final DataDefinitionTask task;

  public LogicalDataDefinitionPlan(DataDefinitionTask task) {
    super(Collections.emptyList());
    this.task = task;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitDataDefinitionPlan(this, context);
  }
}
