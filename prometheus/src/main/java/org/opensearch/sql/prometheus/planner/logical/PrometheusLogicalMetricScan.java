/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

/**
 * Prometheus Logical Metric Scan Operation.
 * In an optimized plan this node represents both Relation and Filter Operation.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class PrometheusLogicalMetricScan extends LogicalPlan {

  private final String metricName;

  /**
   * Filter Condition.
   */
  private final Expression filter;

  /**
   * PrometheusLogicalMetricScan constructor.
   *
   * @param metricName metricName.
   * @param filter filter.
   */
  @Builder
  public PrometheusLogicalMetricScan(String metricName,
      Expression filter) {
    super(ImmutableList.of());
    this.metricName = metricName;
    this.filter = filter;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

}
