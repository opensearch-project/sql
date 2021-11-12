/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.planner.logical.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.opensearch.planner.logical.OpenSearchLogicalIndexScan;
import org.opensearch.sql.planner.logical.LogicalLimit;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.Rule;

@Getter
public class MergeLimitAndRelation implements Rule<LogicalLimit> {

  private final Capture<LogicalRelation> relationCapture;

  @Accessors(fluent = true)
  private final Pattern<LogicalLimit> pattern;

  /**
   * Constructor of MergeLimitAndRelation.
   */
  public MergeLimitAndRelation() {
    this.relationCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalLimit.class)
        .with(source().matching(typeOf(LogicalRelation.class).capturedAs(relationCapture)));
  }

  @Override
  public LogicalPlan apply(LogicalLimit plan, Captures captures) {
    LogicalRelation relation = captures.get(relationCapture);
    return OpenSearchLogicalIndexScan.builder()
        .relationName(relation.getRelationName())
        .offset(plan.getOffset())
        .limit(plan.getLimit())
        .build();
  }
}
