/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.storage.Table;

/**
 * Logical operator for insert statement.
 */
@EqualsAndHashCode(callSuper = true)
@ToString
public class LogicalWrite extends LogicalPlan {

  /** Table that handles the write operation. */
  @Getter
  private final Table table;

  public LogicalWrite(LogicalPlan child, Table table) {
    super(Collections.singletonList(child));
    this.table = table;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitWrite(this, context);
  }
}
