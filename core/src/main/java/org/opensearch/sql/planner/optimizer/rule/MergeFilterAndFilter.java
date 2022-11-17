/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.optimizer.rule;

import static com.facebook.presto.matching.Pattern.typeOf;
import static org.opensearch.sql.planner.optimizer.pattern.Patterns.source;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Merge Filter --> Filter to the single Filter condition.
 */
public class MergeFilterAndFilter implements Rule<LogicalFilter> {

  private final Capture<LogicalFilter> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalFilter> pattern;

  /**
   * Constructor of MergeFilterAndFilter.
   */
  public MergeFilterAndFilter() {
    this.capture = Capture.newCapture();
    this.pattern = typeOf(LogicalFilter.class)
        .with(source().matching(typeOf(LogicalFilter.class).capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalFilter filter,
                           Captures captures) {
    LogicalFilter childFilter = captures.get(capture);
    return new LogicalFilter(
        childFilter.getChild().get(0),
        DSL.and(filter.getCondition(), childFilter.getCondition())
    );
  }
}
