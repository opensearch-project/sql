/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.planner.physical;

import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.util.Pair;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

public interface OpenSearchIndexScanRule {
  /**
   * CalciteOpenSearchIndexScan doesn't allow push-down anymore (except Sort under some strict
   * condition) after Aggregate push-down.
   */
  static boolean noAggregatePushed(AbstractCalciteIndexScan scan) {
    if (scan.getPushDownContext().isAggregatePushed()) return false;
    final RelOptTable table = scan.getTable();
    return table.unwrap(OpenSearchIndex.class) != null;
  }

  static boolean isLimitPushed(AbstractCalciteIndexScan scan) {
    return scan.getPushDownContext().isLimitPushed();
  }

  // `RelDecorrelator` may generate a Project with duplicated fields, e.g. Project($0,$0).
  // There will be problem if pushing down the pattern like `Aggregate(AGG($0),{1})-Project($0,$0)`,
  // as it will lead to field-name conflict.
  // We should wait and rely on `AggregateProjectMergeRule` to mitigate it by having this constraint
  // Nevertheless, that rule cannot handle all cases if there is RexCall in the Project,
  // e.g. Project($0, $0, +($0,1)). We cannot push down the Aggregate for this corner case.
  // TODO: Simplify the Project where there is RexCall by adding a new rule.
  static boolean distinctProjectList(LogicalProject project) {
    // Change to Set<Pair<RexNode, String>> to resolve
    // https://github.com/opensearch-project/sql/issues/4347
    Set<Pair<RexNode, String>> rexSet = new HashSet<>();
    return project.getNamedProjects().stream().allMatch(rexSet::add);
  }

  static boolean containsRexOver(LogicalProject project) {
    return project.getProjects().stream().anyMatch(RexOver::containsOver);
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
    return sort.fetch != null;
  }

  static boolean projectContainsExpr(Project project) {
    return project.getProjects().stream().anyMatch(p -> p instanceof RexCall);
  }

  static boolean sortByFieldsOnly(Sort sort) {
    return !sort.getCollation().getFieldCollations().isEmpty() && sort.fetch == null;
  }
}
