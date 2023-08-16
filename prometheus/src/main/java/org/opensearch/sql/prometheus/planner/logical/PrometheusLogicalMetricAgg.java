/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.planner.logical;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

/** Logical Metric Scan along with aggregation Operation. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class PrometheusLogicalMetricAgg extends LogicalPlan {

  private final String metricName;

  /** Filter Condition. */
  @Setter private Expression filter;

  /** Aggregation List. */
  @Setter private List<NamedAggregator> aggregatorList;

  /** Group List. */
  @Setter private List<NamedExpression> groupByList;

  /**
   * Constructor for LogicalMetricAgg Logical Plan.
   *
   * @param metricName metricName
   * @param filter filter
   * @param aggregatorList aggregatorList
   * @param groupByList groupByList.
   */
  @Builder
  public PrometheusLogicalMetricAgg(
      String metricName,
      Expression filter,
      List<NamedAggregator> aggregatorList,
      List<NamedExpression> groupByList) {
    super(ImmutableList.of());
    this.metricName = metricName;
    this.filter = filter;
    this.aggregatorList = aggregatorList;
    this.groupByList = groupByList;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }
}
