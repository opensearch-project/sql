/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.storage;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.functions.scan.QueryRangeFunctionTableScanBuilder;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalPlanOptimizerFactory;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.request.system.PrometheusDescribeMetricRequest;
import org.opensearch.sql.prometheus.storage.implementor.PrometheusDefaultImplementor;
import org.opensearch.sql.prometheus.storage.scan.CalciteLogicalPrometheusScan;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Prometheus table (metric) implementation. This can be constructed from a metric Name or from
 * PrometheusQueryRequest In case of query_range table function.
 *
 * <p>Implements both the V2 engine's {@link Table} interface and Calcite's {@link
 * TranslatableTable} interface, enabling this table to participate in Calcite query plans with
 * pushdown of time range filters and label matchers to PromQL, while retaining V2 engine
 * compatibility.
 */
public class PrometheusMetricTable extends AbstractTable implements TranslatableTable, Table {

  @Getter private final PrometheusClient prometheusClient;

  @Getter private final String metricName;

  @Getter private final PrometheusQueryRequest prometheusQueryRequest;

  /** The cached mapping of field and type in index. */
  private Map<String, ExprType> cachedFieldTypes = null;

  /** Constructor only with metric name. */
  public PrometheusMetricTable(PrometheusClient prometheusService, @Nonnull String metricName) {
    this.prometheusClient = prometheusService;
    this.metricName = metricName;
    this.prometheusQueryRequest = null;
  }

  /** Constructor for entire promQl Request. */
  public PrometheusMetricTable(
      PrometheusClient prometheusService, @Nonnull PrometheusQueryRequest prometheusQueryRequest) {
    this.prometheusClient = prometheusService;
    this.metricName = null;
    this.prometheusQueryRequest = prometheusQueryRequest;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException("Prometheus metric exists operation is not supported");
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException("Prometheus metric create operation is not supported");
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      if (metricName != null) {
        cachedFieldTypes =
            new PrometheusDescribeMetricRequest(prometheusClient, null, metricName).getFieldTypes();
      } else {
        cachedFieldTypes =
            new HashMap<>(PrometheusMetricDefaultSchema.DEFAULT_MAPPING.getMapping());
        cachedFieldTypes.put(LABELS, ExprCoreType.STRING);
      }
    }
    return cachedFieldTypes;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    PrometheusMetricScan metricScan = new PrometheusMetricScan(prometheusClient);
    return plan.accept(new PrometheusDefaultImplementor(), metricScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return PrometheusLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  // Only handling query_range function for now.
  // we need to move PPL implementations to ScanBuilder in future.
  @Override
  public TableScanBuilder createScanBuilder() {
    if (metricName == null) {
      return new QueryRangeFunctionTableScanBuilder(prometheusClient, prometheusQueryRequest);
    } else {
      return null;
    }
  }

  // ---- Calcite TranslatableTable implementation ----

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return OpenSearchTypeFactory.convertSchema(this);
  }

  /**
   * Creates a logical scan node for this Prometheus metric that supports pushdown of time range
   * filters and label matchers into the PromQL query.
   */
  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CalciteLogicalPrometheusScan(cluster, relOptTable, this);
  }
}
