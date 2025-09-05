/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;

/** Logical Aggregation. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalAggregation extends LogicalPlan {

  private final List<NamedAggregator> aggregatorList;

  private final List<NamedExpression> groupByList;

  private final boolean bucketNullable;

  /** Constructor of LogicalAggregation. */
  public LogicalAggregation(
      LogicalPlan child,
      List<NamedAggregator> aggregatorList,
      List<NamedExpression> groupByList,
      boolean bucketNullable) {
    super(Collections.singletonList(child));
    this.aggregatorList = aggregatorList;
    this.groupByList = groupByList;
    this.bucketNullable = bucketNullable;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAggregation(this, context);
  }
}
