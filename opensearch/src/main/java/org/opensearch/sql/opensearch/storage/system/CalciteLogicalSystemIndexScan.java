/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.opensearch.planner.physical.EnumerableSystemIndexScanRule;

public class CalciteLogicalSystemIndexScan extends CalciteSystemIndexScan {

  public CalciteLogicalSystemIndexScan(
      RelOptCluster cluster, RelOptTable table, OpenSearchSystemIndex sysIndex) {
    this(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        ImmutableList.of(),
        table,
        sysIndex,
        table.getRowType());
  }

  protected CalciteLogicalSystemIndexScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchSystemIndex sysIndex,
      RelDataType schema) {
    super(cluster, traitSet, hints, table, sysIndex, schema);
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    planner.addRule(EnumerableSystemIndexScanRule.DEFAULT_CONFIG.toRule());
  }
}
