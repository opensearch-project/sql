/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalPlanOptimizerFactory;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalPlanValidator;
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
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      if (metricName != null) {
        cachedFieldTypes =
            new PrometheusDescribeMetricRequest(prometheusClient, null,
                metricName).getFieldTypes();
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
    if (prometheusQueryRequest != null) {
      metricScan.setRequest(prometheusQueryRequest);
    }
    return plan.accept(new PrometheusDefaultImplementor(), metricScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return PrometheusLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  @Override
  public void validate(LogicalPlan plan) {
    PrometheusLogicalPlanValidator.ValidatorContext context
        = new PrometheusLogicalPlanValidator.ValidatorContext(null, new ArrayList<>());
    plan.accept(new PrometheusLogicalPlanValidator(), context);
    if (context.getErrorMessages().size() > 0) {
      throw new UnsupportedOperationException(
          String.format("Error list: %s", context.getErrorMessages()));
    }
  }

}