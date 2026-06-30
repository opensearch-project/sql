/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.rest;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.opensearch.planner.rules.EnumerableRestScanRule;

/** The logical relational operator representing a scan of a {@link RestSourceTable}. */
public class CalciteLogicalRestScan extends AbstractCalciteRestScan {

  public CalciteLogicalRestScan(
      RelOptCluster cluster, RelOptTable table, RestSourceTable restTable) {
    this(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        ImmutableList.of(),
        table,
        restTable,
        table.getRowType());
  }

  protected CalciteLogicalRestScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      RestSourceTable restTable,
      RelDataType schema) {
    super(cluster, traitSet, hints, table, restTable, schema);
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    planner.addRule(EnumerableRestScanRule.DEFAULT_CONFIG.toRule());
  }
}
