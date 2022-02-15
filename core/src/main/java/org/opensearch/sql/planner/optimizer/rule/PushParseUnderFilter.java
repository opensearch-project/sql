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
import org.opensearch.sql.planner.logical.LogicalFilter;
import org.opensearch.sql.planner.logical.LogicalParse;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Push Filter under Sort.
 * Filter - Sort - Child --> Sort - Filter - Child
 */
public class PushParseUnderFilter implements Rule<LogicalParse> {

  private final Capture<LogicalFilter> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalParse> pattern;

  /**
   * Constructor of PushFilterUnderSort.
   */
  public PushParseUnderFilter() {
    this.capture = Capture.newCapture();
    this.pattern = typeOf(LogicalParse.class)
        .with(source().matching(typeOf(LogicalFilter.class).capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalParse parse,
                           Captures captures) {
    LogicalFilter filter = captures.get(capture);
    LogicalPlan newFilter = new LogicalFilter(
        parse.replaceChildPlans(filter.getChild()),
        filter.getCondition()
    );
    return newFilter;
  }
}
