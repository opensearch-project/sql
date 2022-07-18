/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Logical Relation represent the data source.
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalRelation extends LogicalPlan {

  @Getter
  private final String relationName;

  @Getter
  private String catalogName;

  public LogicalRelation(String relationName) {
    super(ImmutableList.of());
    this.relationName = relationName;
  }

  /**
   * Constructor of LogicalRelation.
   */
  public LogicalRelation(String relationName, String catalogName) {
    super(ImmutableList.of());
    this.relationName = relationName;
    this.catalogName = catalogName;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRelation(this, context);
  }
}
