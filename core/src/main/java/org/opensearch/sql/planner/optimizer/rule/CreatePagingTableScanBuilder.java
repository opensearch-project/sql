/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import lombok.Getter;
import lombok.experimental.Accessors;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.optimizer.Rule;

/**
 * Rule to create a paged TableScanBuilder in pagination request.
 */
public class CreatePagingTableScanBuilder implements Rule<LogicalPaginate> {
  /** Capture the table inside matched logical paginate operator. */
  private LogicalPlan relationParent = null;
  /** Pattern that matches logical relation operator. */
  @Accessors(fluent = true)
  @Getter
  private final Pattern<LogicalPaginate> pattern;

  /**
   * Constructor.
   */
  public CreatePagingTableScanBuilder() {
    this.pattern = Pattern.typeOf(LogicalPaginate.class).matching(this::findLogicalRelation);
  }

  /**
   * Finds an instance of LogicalRelation and saves a reference in relationParent variable.
   * @param logicalPaginate An instance of LogicalPaginate
   * @return true if {@link LogicalRelation} node was found among the descendents of
   *     {@link this.logicalPaginate}, false otherwise.
   */
  private boolean findLogicalRelation(LogicalPaginate logicalPaginate) {
    Deque<LogicalPlan> plans = new ArrayDeque<>();
    plans.push(logicalPaginate);
    do {
      var plan = plans.pop();
      if (plan.getChild().stream().anyMatch(LogicalRelation.class::isInstance)) {
        if (plan.getChild().size() > 1) {
          throw new UnsupportedOperationException();
        }
        relationParent = plan;
        return true;
      }
      plan.getChild().forEach(plans::push);
    } while (!plans.isEmpty());
    return false;
  }


  @Override
  public LogicalPlan apply(LogicalPaginate plan, Captures captures) {
    var logicalRelation = (LogicalRelation) relationParent.getChild().get(0);
    var scan = logicalRelation.getTable().createPagedScanBuilder(plan.getPageSize());
    relationParent.replaceChildPlans(List.of(scan));

    return plan;
  }
}
