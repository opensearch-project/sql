/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.optimizer;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import java.util.ArrayDeque;
import java.util.Deque;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.storage.read.TableScanBuilder;

public class PushDownPageSize implements Rule<LogicalPaginate> {
  TableScanBuilder builder;
  @Override
  public Pattern<LogicalPaginate> pattern() {
    return Pattern.typeOf(LogicalPaginate.class).matching(this::findTableScanBuilder);
  }

  @Override
  public LogicalPlan apply(LogicalPaginate plan, Captures captures) {

    if (!builder.pushDownPageSize(plan)) {
      throw new IllegalStateException("Failed to push down LogicalPaginate");
    }
    return plan.getChild().get(0);
  }

  private boolean findTableScanBuilder(LogicalPaginate logicalPaginate) {
    Deque<LogicalPlan> plans = new ArrayDeque<>();
    plans.add(logicalPaginate);
    do {
      final var plan = plans.removeFirst();
      final var children = plan.getChild();
      if (children.stream().anyMatch(TableScanBuilder.class::isInstance)) {
        if (children.size() > 1) {
          throw new UnsupportedOperationException(
              "Unsupported plan: relation operator cannot have siblings");
        }
        builder = (TableScanBuilder) children.get(0);
      }
      plans.addAll(children);
    } while (!plans.isEmpty());
    return builder != null;
  }
}
