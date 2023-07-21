/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.utils;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.aggregation.NamedAggregator;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalMetricAgg;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalMetricScan;

public class LogicalPlanUtils {

  /**
   * Build PrometheusLogicalMetricScan.
   */
  public static LogicalPlan indexScan(String metricName, Expression filter) {
    return PrometheusLogicalMetricScan.builder().metricName(metricName)
        .filter(filter)
        .build();
  }

  /**
   * Build PrometheusLogicalMetricAgg.
   */
  public static LogicalPlan indexScanAgg(String metricName, Expression filter,
                                         List<NamedAggregator> aggregators,
                                         List<NamedExpression> groupByList) {
    return PrometheusLogicalMetricAgg.builder().metricName(metricName)
        .filter(filter)
        .aggregatorList(aggregators)
        .groupByList(groupByList)
        .build();
  }

  /**
   * Build PrometheusLogicalMetricAgg.
   */
  public static LogicalPlan indexScanAgg(String metricName,
                                         List<NamedAggregator> aggregators,
                                         List<NamedExpression> groupByList) {
    return PrometheusLogicalMetricAgg.builder().metricName(metricName)
        .aggregatorList(aggregators)
        .groupByList(groupByList)
        .build();
  }

  /**
   * Build PrometheusLogicalMetricAgg.
   */
  public static LogicalPlan testLogicalPlanNode() {
    return new TestLogicalPlan();
  }

  static class TestLogicalPlan extends LogicalPlan {

    public TestLogicalPlan() {
      super(ImmutableList.of());
    }

    @Override
    public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
      return visitor.visitNode(this, null);
    }
  }



}
