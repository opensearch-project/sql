/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;

/** Logical Filter represent the filter relation. */
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalFilter extends LogicalPlan {

  @Getter private final Expression condition;

  /** Constructor of LogicalFilter. */
  public LogicalFilter(LogicalPlan child, Expression condition) {
    super(Collections.singletonList(child));
    this.condition = condition;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitFilter(this, context);
  }
}
