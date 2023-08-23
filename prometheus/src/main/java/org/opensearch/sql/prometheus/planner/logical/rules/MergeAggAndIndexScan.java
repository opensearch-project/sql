/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.planner.logical.rules;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.Rule;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalMetricAgg;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalMetricScan;

/** Merge Aggregation -- Relation to MetricScanAggregation. */
public class MergeAggAndIndexScan implements Rule<LogicalAggregation> {

  private final Capture<PrometheusLogicalMetricScan> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalAggregation> pattern;

  /** Constructor of MergeAggAndIndexScan. */
  public MergeAggAndIndexScan() {
    this.capture = Capture.newCapture();
    this.pattern =
        typeOf(LogicalAggregation.class)
            .with(source().matching(typeOf(PrometheusLogicalMetricScan.class).capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalAggregation aggregation, Captures captures) {
    PrometheusLogicalMetricScan indexScan = captures.get(capture);
    return PrometheusLogicalMetricAgg.builder()
        .metricName(indexScan.getMetricName())
        .filter(indexScan.getFilter())
        .aggregatorList(aggregation.getAggregatorList())
        .groupByList(aggregation.getGroupByList())
        .build();
  }
}
