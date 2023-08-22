/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.logical.node;

import java.util.List;
import java.util.Map;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.node.sort.QuickSort;

/** Logical operator for Sort. */
public class Sort implements LogicalOperator {

  private final LogicalOperator next;

  /** Column name list in ORDER BY */
  private final List<String> orderByColNames;

  /** Order by type, ex. ASC, DESC */
  private final String orderByType;

  public Sort(LogicalOperator next, List<String> orderByColNames, String orderByType) {
    this.next = next;
    this.orderByColNames = orderByColNames;
    this.orderByType = orderByType.toUpperCase();
  }

  @Override
  public PlanNode[] children() {
    return new PlanNode[] {next};
  }

  @Override
  public <T> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<T>> optimalOps) {
    return new PhysicalOperator[] {
      new QuickSort<>(optimalOps.get(next), orderByColNames, orderByType)
    };
  }

  @Override
  public String toString() {
    return "Sort [ columns=" + orderByColNames + " order=" + orderByType + " ]";
  }
}
