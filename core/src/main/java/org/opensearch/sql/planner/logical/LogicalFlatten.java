/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.ReferenceExpression;

/** Logical plan that represent the flatten command. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalFlatten extends LogicalPlan {
  private final ReferenceExpression field;

  public LogicalFlatten(LogicalPlan child, ReferenceExpression field) {
    super(Collections.singletonList(child));
    this.field = field;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitFlatten(this, context);
  }
}
