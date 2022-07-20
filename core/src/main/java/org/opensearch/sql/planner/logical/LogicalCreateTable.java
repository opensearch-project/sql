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
 * Logical operator for create table.
 */
@ToString
@Getter
@EqualsAndHashCode(callSuper = true)
public class LogicalCreateTable extends LogicalPlan {

  private final String tableName;

  public LogicalCreateTable(String tableName, LogicalPlan child) {
    super(Collections.singletonList(child));
    this.tableName = tableName;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitCreateTable(this, context);
  }
}
