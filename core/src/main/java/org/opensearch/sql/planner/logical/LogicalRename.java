/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.ReferenceExpression;

/**
 * Rename Operator.
 * renameList is list of mapping of source and target.
 */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalRename extends LogicalPlan {

  @Getter
  private final Map<ReferenceExpression, ReferenceExpression> renameMap;

  /**
   * Constructor of LogicalRename.
   */
  public LogicalRename(
      LogicalPlan child,
      Map<ReferenceExpression, ReferenceExpression> renameMap) {
    super(Collections.singletonList(child));
    this.renameMap = renameMap;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitRename(this, context);
  }
}
