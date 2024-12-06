/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import static java.util.Collections.singletonList;

import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;

/** Logical Dedupe Plan. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalDedupe extends LogicalPlan {

  private final List<Expression> dedupeList;
  private final Integer allowedDuplication;
  private final Boolean keepEmpty;
  private final Boolean consecutive;

  /** Constructor of LogicalDedupe. */
  public LogicalDedupe(
      LogicalPlan child,
      List<Expression> dedupeList,
      Integer allowedDuplication,
      Boolean keepEmpty,
      Boolean consecutive) {
    super(singletonList(child));
    this.dedupeList = dedupeList;
    this.allowedDuplication = allowedDuplication;
    this.keepEmpty = keepEmpty;
    this.consecutive = consecutive;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitDedupe(this, context);
  }
}
