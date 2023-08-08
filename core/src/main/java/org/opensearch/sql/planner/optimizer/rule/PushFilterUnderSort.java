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
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalSort;
import org.opensearch.sql.planner.optimizer.Rule;

/** Push Filter under Sort. Filter - Sort - Child --> Sort - Filter - Child */
public class PushFilterUnderSort implements Rule<LogicalFilter> {

  private final Capture<LogicalSort> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalFilter> pattern;

  /** Constructor of PushFilterUnderSort. */
  public PushFilterUnderSort() {
    this.capture = Capture.newCapture();
    this.pattern =
        typeOf(LogicalFilter.class)
            .with(source().matching(typeOf(LogicalSort.class).capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalFilter filter, Captures captures) {
    LogicalSort sort = captures.get(capture);
    return new LogicalSort(filter.replaceChildPlans(sort.getChild()), sort.getSortList());
  }
}
