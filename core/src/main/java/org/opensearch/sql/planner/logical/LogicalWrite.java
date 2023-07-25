/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.storage.Table;

/**
 * Logical operator for insert statement.
 */
@EqualsAndHashCode(callSuper = true)
@Getter
@ToString
public class LogicalWrite extends LogicalPlan {

  /** Table that handles the write operation. */
  private final Table table;

  /** Optional column name list specified in insert statement. */
  private final List<String> columns;

  /**
   * Construct a logical write with given child node, table and column name list.
   */
  public LogicalWrite(LogicalPlan child, Table table, List<String> columns) {
    super(Collections.singletonList(child));
    this.table = table;
    this.columns = columns;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitWrite(this, context);
  }
}
