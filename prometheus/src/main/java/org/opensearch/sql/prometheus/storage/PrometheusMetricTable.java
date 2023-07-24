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
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Prometheus table (metric) implementation.
 * This can be constructed from  a metric Name
 * or from PrometheusQueryRequest In case of query_range table function.
 */
public class PrometheusMetricTable implements Table {

  private final PrometheusClient prometheusClient;

  @Getter
  private final String metricName;

  @Getter
  private final PrometheusQueryRequest prometheusQueryRequest;


  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * Constructor only with metric name.
   */
  public PrometheusMetricTable(PrometheusClient prometheusService, @Nonnull String metricName) {
    this.prometheusClient = prometheusService;
    this.metricName = metricName;
    this.prometheusQueryRequest = null;
  }

  /**
   * Constructor for entire promQl Request.
   */
  public PrometheusMetricTable(PrometheusClient prometheusService,
                               @Nonnull PrometheusQueryRequest prometheusQueryRequest) {
    this.prometheusClient = prometheusService;
    this.metricName = null;
    this.prometheusQueryRequest = prometheusQueryRequest;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException(
        "Prometheus metric exists operation is not supported");
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException(
        "Prometheus metric create operation is not supported");
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      if (metricName != null) {
        cachedFieldTypes =
            new PrometheusDescribeMetricRequest(prometheusClient, null,
                metricName).getFieldTypes();
      } else {
        cachedFieldTypes = new HashMap<>(PrometheusMetricDefaultSchema.DEFAULT_MAPPING
            .getMapping());
        cachedFieldTypes.put(LABELS, ExprCoreType.STRING);
      }
    }
    return cachedFieldTypes;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    PrometheusMetricScan metricScan =
        new PrometheusMetricScan(prometheusClient);
    if (prometheusQueryRequest != null) {
      metricScan.setRequest(prometheusQueryRequest);
      metricScan.setIsQueryRangeFunctionScan(Boolean.TRUE);
    }
    return plan.accept(new PrometheusDefaultImplementor(), metricScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return PrometheusLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  //Only handling query_range function for now.
  //we need to move PPL implementations to ScanBuilder in future.
  @Override
  public TableScanBuilder createScanBuilder() {
    if (metricName == null) {
      return new QueryRangeFunctionTableScanBuilder(prometheusClient, prometheusQueryRequest);
    } else {
      return null;
    }
  }
}
