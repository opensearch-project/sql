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
import org.opensearch.sql.planner.logical.LogicalAggregation;
import org.opensearch.sql.planner.logical.LogicalNested;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Eliminate LogicalNested if its child is LogicalAggregation.<br>
 * LogicalNested - LogicalAggregation - Child --> LogicalAggregation - Child<br>
 * E.g. count(nested(foo.bar, foo))
 */
public class EliminateNested implements Rule<LogicalNested> {

  private final Capture<LogicalAggregation> capture;

  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalNested> pattern;

  public EliminateNested() {
    this.capture = Capture.newCapture();
    this.pattern =
        typeOf(LogicalNested.class)
            .with(source().matching(typeOf(LogicalAggregation.class).capturedAs(capture)));
  }

  @Override
  public LogicalPlan apply(LogicalNested plan, Captures captures) {
    return captures.get(capture);
  }
}
