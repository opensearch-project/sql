/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.logical.rule;

import org.opensearch.sql.legacy.query.planner.logical.LogicalPlanVisitor;
import org.opensearch.sql.legacy.query.planner.logical.node.Filter;
import org.opensearch.sql.legacy.query.planner.logical.node.Group;

/** Push down selection (filter) */
public class SelectionPushDown implements LogicalPlanVisitor {

  /**
   * Store the filter found in visit and reused to push down. It's not necessary to create a new one
   * because no need to collect filter condition elsewhere
   */
  private Filter filter;

  @Override
  public boolean visit(Filter filter) {
    this.filter = filter;
    return true;
  }

  @Override
  public boolean visit(Group group) {
    if (filter != null && !filter.isNoOp()) {
      group.pushDown(filter);
    }
    return false; // avoid iterating operators in virtual Group
  }
}
