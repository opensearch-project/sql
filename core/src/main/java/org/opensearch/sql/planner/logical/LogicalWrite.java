/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.opensearch.sql.storage.Table;

/**
 * Logical operator for insert statement.
 */
@Getter
public class LogicalWrite extends LogicalPlan {

  private final String tableName;

  private final List<String> columnNames;

  private final Table table;

  public LogicalWrite(LogicalPlan child, String tableName, List<String> columnNames, Table table) {
    super(Collections.singletonList(child));
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.table = table;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitWrite(this, context);
  }
}
