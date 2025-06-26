/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

public interface OpenSearchIndexScanRule {
  /**
   * CalciteOpenSearchIndexScan doesn't allow push-down anymore (except Sort under some strict
   * condition) after Aggregate push-down.
   */
  static boolean noAggregatePushed(CalciteLogicalIndexScan scan) {
    if (scan.getPushDownContext().isAggregatePushed()) return false;
    final RelOptTable table = scan.getTable();
    return table.unwrap(OpenSearchIndex.class) != null;
  }

  static boolean isLimitPushed(CalciteLogicalIndexScan scan) {
    return scan.getPushDownContext().isLimitPushed();
  }

  static boolean distinctProjectList(LogicalProject project) {
    Set<RexNode> rexSet = new HashSet<>();
    return project.getProjects().stream().allMatch(rexSet::add);
  }

  /**
   * The LogicalSort is a LIMIT that should be pushed down when its fetch field is not null and its
   * collation is empty. For example: <code>sort name | head 5</code> should not be pushed down
   * because it has a field collation.
   *
   * @param sort The LogicalSort to check.
   * @return True if the LogicalSort is a LIMIT, false otherwise.
   */
  static boolean isLogicalSortLimit(LogicalSort sort) {
    return sort.fetch != null && sort.getCollation().getFieldCollations().isEmpty();
  }
}
