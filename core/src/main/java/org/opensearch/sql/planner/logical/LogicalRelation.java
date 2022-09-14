/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.storage.Table;

/**
 * Logical Relation represent the data source.
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalRelation extends LogicalPlan {

  @Getter
  private final String relationName;

  @Getter
  private final Table table;

  /**
   * Constructor of LogicalRelation.
   */
  public LogicalRelation(String relationName, Table table) {
    super(ImmutableList.of());
    this.relationName = relationName;
    this.table = table;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRelation(this, context);
  }
}
