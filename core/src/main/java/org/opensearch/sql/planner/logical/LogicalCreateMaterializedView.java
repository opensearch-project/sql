/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Logical operator for create materialized view.
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = true)
public class LogicalCreateMaterializedView extends LogicalPlan {

  //private final Expression viewName; // TODO: table/view name type is missing?
  private final String viewName;

  public LogicalCreateMaterializedView(String viewName, LogicalPlan child) {
    super(Collections.singletonList(child));
    this.viewName = viewName;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitCreateMaterializedView(this, context);
  }
}
