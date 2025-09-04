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
@ToString
@EqualsAndHashCode(callSuper = true)
public class LogicalAggregation extends LogicalPlan {

  @Getter private final List<NamedAggregator> aggregatorList;

  @Getter private final List<NamedExpression> groupByList;

  @Getter private final boolean nullableBucket;

  /** Constructor of LogicalAggregation. */
  public LogicalAggregation(
      LogicalPlan child,
      List<NamedAggregator> aggregatorList,
      List<NamedExpression> groupByList,
      boolean nullableBucket) {
    super(Collections.singletonList(child));
    this.aggregatorList = aggregatorList;
    this.groupByList = groupByList;
    this.nullableBucket = nullableBucket;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitAggregation(this, context);
  }
}
