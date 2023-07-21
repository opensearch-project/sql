/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage.system;


import static org.opensearch.sql.utils.SystemIndexUtils.systemTable;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.system.PrometheusDescribeMetricRequest;
import org.opensearch.sql.prometheus.request.system.PrometheusListMetricsRequest;
import org.opensearch.sql.prometheus.request.system.PrometheusSystemRequest;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils;

/**
 * Prometheus System Table Implementation.
 */
public class PrometheusSystemTable implements Table {
  /**
   * System Index Name.
   */
  private final Pair<PrometheusSystemTableSchema, PrometheusSystemRequest> systemIndexBundle;

  private final DataSourceSchemaName dataSourceSchemaName;

  public PrometheusSystemTable(
      PrometheusClient client, DataSourceSchemaName dataSourceSchemaName, String indexName) {
    this.dataSourceSchemaName = dataSourceSchemaName;
    this.systemIndexBundle = buildIndexBundle(client, indexName);
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return systemIndexBundle.getLeft().getMapping();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new PrometheusSystemTableDefaultImplementor(), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public class PrometheusSystemTableDefaultImplementor
      extends DefaultImplementor<Object> {

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Object context) {
      return new PrometheusSystemTableScan(systemIndexBundle.getRight());
    }
  }

  private Pair<PrometheusSystemTableSchema, PrometheusSystemRequest> buildIndexBundle(
      PrometheusClient client, String indexName) {
    SystemIndexUtils.SystemTable systemTable = systemTable(indexName);
    if (systemTable.isSystemInfoTable()) {
      return Pair.of(PrometheusSystemTableSchema.SYS_TABLE_TABLES,
          new PrometheusListMetricsRequest(client, dataSourceSchemaName));
    } else {
      return Pair.of(PrometheusSystemTableSchema.SYS_TABLE_MAPPINGS,
          new PrometheusDescribeMetricRequest(client,
              dataSourceSchemaName, systemTable.getTableName()));
    }
  }
}
