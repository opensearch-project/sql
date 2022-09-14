/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.METRIC;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalTableFunction;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.config.PrometheusConfig;
import org.opensearch.sql.prometheus.data.value.PrometheusExprValueFactory;
import org.opensearch.sql.prometheus.request.PrometheusDescribeMetricRequest;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.storage.querybuilder.PromQLQueryBuilder;
import org.opensearch.sql.storage.Table;

/**
 * OpenSearch table (index) implementation.
 */
public class PrometheusIndex implements Table {

  private final PrometheusClient prometheusService;

  private final String metricName;

  private final PrometheusConfig prometheusConfig;

  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * Constructor.
   */
  public PrometheusIndex(PrometheusClient prometheusService, String metricName,
                         PrometheusConfig prometheusConfig) {
    this.prometheusService = prometheusService;
    this.metricName = metricName;
    this.prometheusConfig = prometheusConfig;
  }

  /*
   * TODO: Assume indexName doesn't have wildcard.
   *  Need to either handle field name conflicts
   *   or lazy evaluate when query engine pulls field type.
   */
  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      cachedFieldTypes =
          new PrometheusDescribeMetricRequest(prometheusService, metricName).getFieldTypes();
    }
    return cachedFieldTypes;
  }

  /**
   * TODO: Push down operations to index scan operator as much as possible in future.
   */
  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    PrometheusMetricScan indexScan =
        new PrometheusMetricScan(prometheusService, prometheusConfig, metricName,
            new PrometheusExprValueFactory(getFieldTypes()));

    /*
     * Visit logical plan with index scan as context so logical operators visited, such as
     * aggregation, filter, will accumulate (push down) OpenSearch query and aggregation DSL on
     * index scan.
     */
    return plan.accept(new PrometheusDefaultImplementor(indexScan, prometheusConfig), indexScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return plan;
  }

  /**
   * Default Implementor of Logical plan for prometheus.
   */
  @VisibleForTesting
  @RequiredArgsConstructor
  public static class PrometheusDefaultImplementor
      extends DefaultImplementor<PrometheusMetricScan> {
    private final PrometheusMetricScan metricScan;
    private final PrometheusConfig prometheusConfig;

    @Override
    public PhysicalPlan visitTableFunction(LogicalTableFunction node,
                                           PrometheusMetricScan context) {
      PromQLQueryBuilder queryBuilder = new PromQLQueryBuilder();
      queryBuilder.build(node.getTableFunction(), context);
      return metricScan;
    }

    @Override
    public PhysicalPlan visitProject(LogicalProject node, PrometheusMetricScan context) {
      List<NamedExpression> finalProjectList = new ArrayList<>();
      finalProjectList.add(
          new NamedExpression(METRIC, new ReferenceExpression(METRIC, ExprCoreType.STRING)));
      finalProjectList.add(
          new NamedExpression(TIMESTAMP,
              new ReferenceExpression(TIMESTAMP, ExprCoreType.TIMESTAMP)));
      finalProjectList.add(
          new NamedExpression(VALUE, new ReferenceExpression(VALUE, ExprCoreType.DOUBLE)));
      return new ProjectOperator(visitChild(node, context), finalProjectList,
          node.getNamedParseExpressions());
    }

    /**
     * Carried logic to define range
     * and resolution in case the parameters are not provided by user.
     * Heuristic used here is from prometheus UI i.e. divide by 250.
     *
     * @param prometheusQueryRequest prometheusQueryRequest.
     * @param startTime              startTime.
     * @param endTime                endTime.
     * @param step                   step.
     */
    private void setRangeAndResolution(PrometheusQueryRequest prometheusQueryRequest,
                                       Long startTime,
                                       Long endTime,
                                       String step) {
      if (endTime == null) {
        endTime = new Date().getTime() / 1000;
        startTime = endTime - prometheusConfig.getDefaultTimeRange();
      } else if (startTime == null) {
        startTime = endTime - prometheusConfig.getDefaultTimeRange();
      }
      if (step == null) {
        long range = endTime - startTime;
        step = Math.max(range / 250, 1) + "s";
      }

      prometheusQueryRequest.setStartTime(startTime);
      prometheusQueryRequest.setEndTime(endTime);
      prometheusQueryRequest.setStep(step);
    }

  }


}
