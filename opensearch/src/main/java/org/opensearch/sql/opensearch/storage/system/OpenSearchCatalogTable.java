/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.system;

import java.util.Map;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.calcite.plan.AbstractOpenSearchTable;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.monitor.OpenSearchMemoryHealthy;
import org.opensearch.sql.opensearch.monitor.OpenSearchResourceMonitor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * A single generic Calcite and V2 table over a read-only OpenSearch catalog endpoint. Per-endpoint
 * behavior (schema, row source, V2 support, and the {@code Scannable} marker) is supplied by a
 * pluggable {@link CatalogSource}.
 */
@Getter
public class OpenSearchCatalogTable extends AbstractOpenSearchTable {

  private final CatalogSource source;
  private final Settings settings;

  public OpenSearchCatalogTable(CatalogSource source, Settings settings) {
    this.source = source;
    this.settings = settings;
  }

  @Override
  public boolean exists() {
    return true;
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException(
        "OpenSearch catalog table is predefined and cannot be created");
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return source.getFieldTypes();
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CalciteLogicalCatalogScan(cluster, relOptTable, this);
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return source.implementV2(plan);
  }

  public OpenSearchResourceMonitor createOpenSearchResourceMonitor() {
    return new OpenSearchResourceMonitor(settings, new OpenSearchMemoryHealthy(settings));
  }
}
