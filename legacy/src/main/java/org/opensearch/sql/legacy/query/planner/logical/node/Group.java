/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.logical.node;

import java.util.Map;
import org.opensearch.sql.legacy.query.planner.core.PlanNode;
import org.opensearch.sql.legacy.query.planner.logical.LogicalOperator;
import org.opensearch.sql.legacy.query.planner.physical.PhysicalOperator;

/** Project-Filter-TableScan group for push down optimization convenience. */
public class Group implements LogicalOperator {

  /** Optional pushed down projection */
  private final Project<?> project;

  /** Optional pushed down filter (selection) */
  private final Filter filter;

  /** Required table scan operator */
  private final TableScan tableScan;

  public Group(TableScan tableScan) {
    this.tableScan = tableScan;
    this.filter = new Filter(tableScan);
    this.project = new Project<>(filter);
  }

  @Override
  public boolean isNoOp() {
    return true;
  }

  @Override
  public <T> PhysicalOperator[] toPhysical(Map<LogicalOperator, PhysicalOperator<T>> optimalOps) {
    return tableScan.toPhysical(optimalOps);
  }

  @Override
  public PlanNode[] children() {
    return new PlanNode[] {topNonNullNode()};
  }

  private PlanNode topNonNullNode() {
    return project != null ? project : (filter != null ? filter : tableScan);
  }

  public String id() {
    return tableScan.getTableAlias();
  }

  public void pushDown(Project<?> project) {
    this.project.pushDown(id(), project);
  }

  public void pushDown(Filter filter) {
    this.filter.pushDown(id(), filter);
  }

  @Override
  public String toString() {
    return "Group";
  }
}
