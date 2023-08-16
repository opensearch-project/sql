/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Optional;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * A {@link LogicalPlanOptimizer} rule that pushes down page size
 * to table scan builder.
 */
public class PushDownPageSize implements Rule<LogicalPaginate> {
  @Override
  public Pattern<LogicalPaginate> pattern() {
    return Pattern.typeOf(LogicalPaginate.class)
      .matching(lp -> findTableScanBuilder(lp).isPresent());
  }

  @Override
  public LogicalPlan apply(LogicalPaginate plan, Captures captures) {

    var builder = findTableScanBuilder(plan).orElseThrow();
    if (!builder.pushDownPageSize(plan)) {
      throw new IllegalStateException("Failed to push down LogicalPaginate");
    }
    return plan.getChild().get(0);
  }

  private Optional<TableScanBuilder> findTableScanBuilder(LogicalPaginate logicalPaginate) {
    Deque<LogicalPlan> plans = new ArrayDeque<>();
    plans.add(logicalPaginate);
    do {
      var plan = plans.removeFirst();
      var children = plan.getChild();
      if (children.stream().anyMatch(TableScanBuilder.class::isInstance)) {
        if (children.size() > 1) {
          throw new UnsupportedOperationException(
            "Unsupported plan: relation operator cannot have siblings");
        }
        return Optional.of((TableScanBuilder) children.get(0));
      }
      plans.addAll(children);
    } while (!plans.isEmpty());
    return Optional.empty();
  }
}
