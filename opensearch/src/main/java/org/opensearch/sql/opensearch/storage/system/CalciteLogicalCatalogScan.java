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
import org.opensearch.sql.opensearch.planner.rules.EnumerableCatalogScanRule;

/** The logical relational operator representing a scan of an {@link OpenSearchCatalogTable}. */
public class CalciteLogicalCatalogScan extends AbstractCalciteCatalogScan {

  public CalciteLogicalCatalogScan(
      RelOptCluster cluster, RelOptTable table, OpenSearchCatalogTable catalogTable) {
    this(
        cluster,
        cluster.traitSetOf(Convention.NONE),
        ImmutableList.of(),
        table,
        catalogTable,
        table.getRowType());
  }

  protected CalciteLogicalCatalogScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table,
      OpenSearchCatalogTable catalogTable,
      RelDataType schema) {
    super(cluster, traitSet, hints, table, catalogTable, schema);
  }

  @Override
  public void register(RelOptPlanner planner) {
    super.register(planner);
    planner.addRule(EnumerableCatalogScanRule.DEFAULT_CONFIG.toRule());
  }
}
