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
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.Rule;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalMetricAgg;

/** Merge Aggregation -- Relation to IndexScanAggregation. */
public class MergeAggAndRelation implements Rule<LogicalAggregation> {

  private final Capture<LogicalRelation> relationCapture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalAggregation> pattern;

  /** Constructor of MergeAggAndRelation. */
  public MergeAggAndRelation() {
    this.relationCapture = Capture.newCapture();
    this.pattern =
        typeOf(LogicalAggregation.class)
            .with(source().matching(typeOf(LogicalRelation.class).capturedAs(relationCapture)));
  }

  @Override
  public LogicalPlan apply(LogicalAggregation aggregation, Captures captures) {
    LogicalRelation relation = captures.get(relationCapture);
    return PrometheusLogicalMetricAgg.builder()
        .metricName(relation.getRelationName())
        .aggregatorList(aggregation.getAggregatorList())
        .groupByList(aggregation.getGroupByList())
        .build();
  }
}
