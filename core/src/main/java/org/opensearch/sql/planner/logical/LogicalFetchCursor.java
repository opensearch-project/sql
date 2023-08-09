/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.storage.StorageEngine;

/** A plan node which represents operation of fetching a next page from the cursor. */
@EqualsAndHashCode(callSuper = false)
@ToString
public class LogicalFetchCursor extends LogicalPlan {
  @Getter private final String cursor;

  @Getter private final StorageEngine engine;

  /** LogicalCursor constructor. Does not have child plans. */
  public LogicalFetchCursor(String cursor, StorageEngine engine) {
    super(List.of());
    this.cursor = cursor;
    this.engine = engine;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitFetchCursor(this, context);
  }
}
