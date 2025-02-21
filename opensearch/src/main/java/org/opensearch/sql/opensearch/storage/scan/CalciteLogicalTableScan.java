/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.opensearch.sql.opensearch.planner.physical.EnumerableIndexScanRule;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

@Getter
public class CalciteLogicalTableScan extends TableScan {
  private final OpenSearchIndex osIndex;

  protected CalciteLogicalTableScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchIndex osIndex) {
    super(cluster, traitSet, hints, table);
    this.osIndex = osIndex;
  }

  public CalciteLogicalTableScan(
      RelOptCluster cluster, RelOptTable table, OpenSearchIndex osIndex) {
    this(cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(), table, osIndex);
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    planner.addRule(EnumerableIndexScanRule.DEFAULT_CONFIG.toRule());
  }
}
