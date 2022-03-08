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
import org.opensearch.sql.opensearch.planner.logical.OpenSearchLogicalIndexScan;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Merge Sort with Relation only when Sort by fields.
 */
public class MergeSortAndRelation implements Rule<LogicalSort> {

  private final Capture<LogicalRelation> relationCapture;
  private final Pattern<LogicalSort> pattern;

  /**
   * Constructor of MergeSortAndRelation.
   */
  public MergeSortAndRelation() {
    this.relationCapture = Capture.newCapture();
    this.pattern = typeOf(LogicalSort.class).matching(OptimizationRuleUtils::sortByFieldsOnly)
        .with(source().matching(typeOf(LogicalRelation.class).capturedAs(relationCapture)));
  }

  @Override
  public Pattern<LogicalSort> pattern() {
    return pattern;
  }

  @Override
  public LogicalPlan apply(LogicalSort sort,
                           Captures captures) {
    LogicalRelation relation = captures.get(relationCapture);
    return OpenSearchLogicalIndexScan
        .builder()
        .relationName(relation.getRelationName())
        .sortList(sort.getSortList())
        .build();
  }
}
