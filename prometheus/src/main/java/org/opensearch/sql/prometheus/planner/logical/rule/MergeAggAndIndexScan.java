/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.planner.logical.rule;

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
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalIndexAgg;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalIndexScan;

/**
 * Merge Aggregation -- Relation to IndexScanAggregation.
 */
public class MergeAggAndIndexScan implements Rule<LogicalAggregation> {

  private final Capture<PrometheusLogicalIndexScan> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalAggregation> pattern;

  /**
   * Constructor of MergeAggAndIndexScan.
   */
  public MergeAggAndIndexScan() {
    this.capture = Capture.newCapture();
    this.pattern = typeOf(LogicalAggregation.class)
        .with(source().matching(typeOf(PrometheusLogicalIndexScan.class)
            .matching(indexScan -> !indexScan.hasLimit())
            .capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalAggregation aggregation,
                           Captures captures) {
    PrometheusLogicalIndexScan indexScan = captures.get(capture);
    return PrometheusLogicalIndexAgg
        .builder()
        .relationName(indexScan.getRelationName())
        .filter(indexScan.getFilter())
        .aggregatorList(aggregation.getAggregatorList())
        .groupByList(aggregation.getGroupByList())
        .build();
  }
}
