/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule;

import static org.opensearch.sql.planner.optimizer.pattern.Patterns.pagination;

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.Rule;

public class PushPageSize
    implements Rule<LogicalPaginate> {
  /** Capture the table inside matched logical paginate operator. */
  private final Capture<Integer> capture;

  /** Pattern that matches logical paginate operator. */
  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalPaginate> pattern;

  /**
   * Constructor.
   */
  public PushPageSize() {
    this.capture = Capture.newCapture();
    this.pattern = Pattern.typeOf(LogicalPaginate.class)
        .with(pagination().capturedAs(capture));
  }

  private LogicalRelation findLogicalRelation(LogicalPlan plan) { //TODO TBD multiple relations?
    for (var subplan : plan.getChild()) {
      if (subplan instanceof LogicalRelation) {
        return (LogicalRelation) subplan;
      }
      var found = findLogicalRelation(subplan);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  @Override
  public LogicalPlan apply(LogicalPaginate plan, Captures captures) {
    var relation = findLogicalRelation(plan);
    if (relation != null) {
      relation.setPageSize(captures.get(capture));
    }
    return plan;
  }
}
