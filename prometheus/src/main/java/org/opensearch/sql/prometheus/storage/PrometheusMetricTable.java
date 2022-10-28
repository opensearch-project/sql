/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.request.system.PrometheusDescribeMetricRequest;
import org.opensearch.sql.prometheus.storage.implementor.PrometheusDefaultImplementor;
import org.opensearch.sql.storage.Table;

/**
 * Prometheus table (metric) implementation.
 * This can be constructed from  a metric Name
 * or from PrometheusQueryRequest In case of query_range table function.
 */
public class PrometheusMetricTable implements Table {

  private final PrometheusClient prometheusClient;

  @Getter
  private final Optional<String> metricName;

  @Getter
  private final Optional<PrometheusQueryRequest> prometheusQueryRequest;


  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * Constructor only with metric name.
   */
  public PrometheusMetricTable(PrometheusClient prometheusService, @Nonnull String metricName) {
    this.prometheusClient = prometheusService;
    this.metricName = Optional.of(metricName);
    this.prometheusQueryRequest = Optional.empty();
  }

  /**
   * Constructor for entire promQl Request.
   */
  public PrometheusMetricTable(PrometheusClient prometheusService,
                               @Nonnull PrometheusQueryRequest prometheusQueryRequest) {
    this.prometheusClient = prometheusService;
    this.metricName = Optional.empty();
    this.prometheusQueryRequest = Optional.of(prometheusQueryRequest);
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      if (metricName.isPresent()) {
        cachedFieldTypes =
            new PrometheusDescribeMetricRequest(prometheusClient, null,
                metricName.orElse(null)).getFieldTypes();
      } else {
        cachedFieldTypes = PrometheusMetricDefaultSchema.DEFAULT_MAPPING.getMapping();
      }
    }
    return cachedFieldTypes;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    PrometheusMetricScan metricScan =
        new PrometheusMetricScan(prometheusClient);
    prometheusQueryRequest.ifPresent(metricScan::setRequest);
    return plan.accept(new PrometheusDefaultImplementor(), metricScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return plan;
  }

}