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
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.Rule;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalIndexScan;

/**
 * Merge Filter -- Relation to LogicalIndexScan.
 */
public class MergeFilterAndRelation implements Rule<LogicalFilter> {

  private final Capture<LogicalRelation> relationCapture;
  private final Pattern<LogicalFilter> pattern;

  /**
   * Constructor of MergeFilterAndRelation.
   */
  public MergeFilterAndRelation() {
    this.relationCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalFilter.class)
        .with(source().matching(typeOf(LogicalRelation.class).capturedAs(relationCapture)));
  }

  @Override
  public Pattern<LogicalFilter> pattern() {
    return pattern;
  }

  @Override
  public LogicalPlan apply(LogicalFilter filter,
                           Captures captures) {
    LogicalRelation relation = captures.get(relationCapture);
    return PrometheusLogicalIndexScan
        .builder()
        .relationName(relation.getRelationName())
        .filter(filter.getCondition())
        .build();
  }
}
