/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;

@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalExpand extends LogicalPlan {

  @Getter private final Expression field;

  public LogicalExpand(LogicalPlan child, Expression field) {
    super(Collections.singletonList(child));
    this.field = field;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitExpand(this, context);
  }
}
