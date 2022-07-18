/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalNativeQuery;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.prometheus.client.PrometheusClient;
import org.opensearch.sql.prometheus.config.PrometheusConfig;
import org.opensearch.sql.prometheus.data.value.PrometheusExprValueFactory;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalIndexAgg;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalIndexScan;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalPlanOptimizerFactory;
import org.opensearch.sql.prometheus.request.PrometheusDescribeMetricRequest;
import org.opensearch.sql.prometheus.request.PrometheusQueryRequest;
import org.opensearch.sql.prometheus.storage.script.filter.PromFilterQuery;
import org.opensearch.sql.prometheus.storage.script.filter.PromQlFilterQueryBuilder;
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
    return PrometheusLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  /**
   * Default Implementor of Logical plan for prometheus.
   */
  @VisibleForTesting
  @RequiredArgsConstructor
  public static class PrometheusDefaultImplementor
      extends DefaultImplementor<PrometheusMetricScan> {
    private final PrometheusMetricScan indexScan;
    private final PrometheusConfig prometheusConfig;

    @Override
    public PhysicalPlan visitNode(LogicalPlan plan, PrometheusMetricScan context) {
      if (plan instanceof PrometheusLogicalIndexScan) {
        return visitIndexScan((PrometheusLogicalIndexScan) plan, context);
      } else if (plan instanceof PrometheusLogicalIndexAgg) {
        return visitIndexAggregation((PrometheusLogicalIndexAgg) plan, context);
      } else {
        throw new IllegalStateException(StringUtils.format("unexpected plan node type %s",
            plan.getClass()));
      }
    }

    /**
     * Implement Prometheus metric Scan.
     */
    public PhysicalPlan visitIndexScan(PrometheusLogicalIndexScan node,
                                       PrometheusMetricScan context) {
      PrometheusQueryRequest request = context.getRequest();
      StringBuilder promQlBuilder = context.getRequest().getPrometheusQueryBuilder();
      promQlBuilder.append(node.getRelationName());
      if (null != node.getFilter()) {
        PromQlFilterQueryBuilder promQlFilterQueryBuilder = new PromQlFilterQueryBuilder();
        PromFilterQuery filterQuery = promQlFilterQueryBuilder.build(node.getFilter());
        promQlBuilder.append(filterQuery.getPromQl());
        setRangeAndResolution(request, filterQuery.getStartTime(), filterQuery.getEndTime(), null);
      } else {
        setRangeAndResolution(request, null, null, null);
      }

      promQlBuilder.insert(0, "(");
      promQlBuilder.append(")");
      return indexScan;
    }

    /**
     * Implement ElasticsearchLogicalIndexAgg.
     */
    public PhysicalPlan visitIndexAggregation(PrometheusLogicalIndexAgg node,
                                              PrometheusMetricScan context) {
      PrometheusQueryRequest request = context.getRequest();
      StringBuilder promQlBuilder = context.getRequest().getPrometheusQueryBuilder();
      promQlBuilder.append(node.getRelationName());
      String step = getSpanExpression(node).map(
          expression -> expression.getValue().toString() + expression.getUnit().getName())
          .orElse(null);
      if (node.getFilter() != null) {
        PromQlFilterQueryBuilder promQlFilterQueryBuilder = new PromQlFilterQueryBuilder();
        PromFilterQuery filterQuery = promQlFilterQueryBuilder.build(node.getFilter());
        promQlBuilder.append(filterQuery.getPromQl());
        setRangeAndResolution(request, filterQuery.getStartTime(), filterQuery.getEndTime(), step);
      } else {
        setRangeAndResolution(request, null, null, step);
      }
      promQlBuilder.insert(0, "(");
      promQlBuilder.append(")");
      if (!node.getAggregatorList().isEmpty()) {
        StringBuilder aggregateQuery = getAggregateQuery(node, request);
        promQlBuilder.insert(0, aggregateQuery);
      }


      return indexScan;
    }

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, PrometheusMetricScan context) {
      StringBuilder promQlBuilder = context.getRequest().getPrometheusQueryBuilder();
      Pattern p = Pattern.compile("`([^`]+)`");
      Matcher m = p.matcher(node.getRelationName());
      if (m.find()) {
        promQlBuilder.append(m.group(1));
      }
      return indexScan;
    }

    @Override
    public PhysicalPlan visitNativeQuery(LogicalNativeQuery node,
                                         PrometheusMetricScan context) {
      PrometheusQueryRequest prometheusQueryRequest = context.getRequest();
      StringBuilder promQlBuilder = prometheusQueryRequest.getPrometheusQueryBuilder();
      promQlBuilder.append(node.getQueryParams().get("query").toString());

      Long startTime = null;
      Long endTime = null;
      String step = null;
      if (node.getQueryParams().containsKey("starttime")) {
        startTime = Long.parseLong(node.getQueryParams().get("starttime").toString());
      }
      if (node.getQueryParams().containsKey("endtime")) {
        endTime = Long.parseLong(node.getQueryParams().get("endtime").toString());
      }
      if (node.getQueryParams().containsKey("step")) {
        step = node.getQueryParams().get("step").toString();
      }

      setRangeAndResolution(prometheusQueryRequest, startTime, endTime, step);
      return indexScan;
    }

    @Override
    public PhysicalPlan visitProject(LogicalProject node, PrometheusMetricScan context) {
      List<NamedExpression> finalProjectList = new ArrayList<>();
      finalProjectList.add(
          new NamedExpression("metric", new ReferenceExpression("metric", ExprCoreType.STRING)));
      finalProjectList.add(
          new NamedExpression("@timestamp",
              new ReferenceExpression("@timestamp", ExprCoreType.TIMESTAMP)));
      finalProjectList.add(
          new NamedExpression("@value", new ReferenceExpression("@value", ExprCoreType.TIMESTAMP)));
      return new ProjectOperator(visitChild(node, context), finalProjectList,
          node.getNamedParseExpressions());
    }

    private StringBuilder getAggregateQuery(PrometheusLogicalIndexAgg node,
                                            PrometheusQueryRequest request) {
      StringBuilder aggregateQuery = new StringBuilder();
      aggregateQuery.insert(0,
          node.getAggregatorList().get(0).getFunctionName().getFunctionName() + " ");
      if (!node.getGroupByList().isEmpty()) {
        long groupByCount = node.getGroupByList().stream().map(NamedExpression::getDelegated)
            .filter(delegated -> !(delegated instanceof SpanExpression)).count();
        if (groupByCount > 0) {
          aggregateQuery.append("by (");
          for (int i = 0; i < node.getGroupByList().size(); i++) {
            NamedExpression expression = node.getGroupByList().get(i);
            if (expression.getDelegated() instanceof SpanExpression) {
              continue;
            }
            if (i == node.getGroupByList().size() - 1) {
              aggregateQuery.append(expression.getName());
            } else {
              aggregateQuery.append(expression.getName()).append(", ");
            }
          }
          aggregateQuery.append(")");
        }
      }
      return aggregateQuery;
    }

    private Optional<SpanExpression> getSpanExpression(PrometheusLogicalIndexAgg node) {
      return node.getGroupByList().stream().map(NamedExpression::getDelegated)
          .filter(delegated -> delegated instanceof SpanExpression)
          .map(delegated -> (SpanExpression) delegated)
          .findFirst();
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
