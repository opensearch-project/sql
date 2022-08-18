/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;

/**
 * Logical operator for insert statement.
 */
public class LogicalInsert extends LogicalPlan {

  private final String tableName;

  private final List<String> columnNames;

  public LogicalInsert(String tableName, List<String> columnNames, LogicalPlan child) {
    super(Collections.singletonList(child));
    this.tableName = tableName;
    this.columnNames = columnNames;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return null;
  }
}
