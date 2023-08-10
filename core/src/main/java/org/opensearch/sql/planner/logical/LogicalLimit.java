/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class LogicalLimit extends LogicalPlan {
  private final Integer limit;
  private final Integer offset;

  /** Constructor of LogicalLimit. */
  public LogicalLimit(LogicalPlan input, Integer limit, Integer offset) {
    super(Collections.singletonList(input));
    this.limit = limit;
    this.offset = offset;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitLimit(this, context);
  }
}
