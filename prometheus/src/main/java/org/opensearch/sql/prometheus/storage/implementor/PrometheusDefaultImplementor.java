/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.implementor;

import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.util.Pair;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalMetricAgg;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalMetricScan;
import org.opensearch.sql.prometheus.storage.PrometheusMetricScan;
import org.opensearch.sql.prometheus.storage.PrometheusMetricTable;
import org.opensearch.sql.prometheus.storage.model.PrometheusResponseFieldNames;
import org.opensearch.sql.prometheus.storage.querybuilder.AggregationQueryBuilder;
import org.opensearch.sql.prometheus.storage.querybuilder.SeriesSelectionQueryBuilder;
import org.opensearch.sql.prometheus.storage.querybuilder.StepParameterResolver;
import org.opensearch.sql.prometheus.storage.querybuilder.TimeRangeParametersResolver;

/**
 * Default Implementor of Logical plan for prometheus.
 */
@RequiredArgsConstructor
public class PrometheusDefaultImplementor
    extends DefaultImplementor<PrometheusMetricScan> {


  @Override
  public PhysicalPlan visitNode(LogicalPlan plan, PrometheusMetricScan context) {
    if (plan instanceof PrometheusLogicalMetricScan) {
      return visitIndexScan((PrometheusLogicalMetricScan) plan, context);
    } else if (plan instanceof PrometheusLogicalMetricAgg) {
      return visitIndexAggregation((PrometheusLogicalMetricAgg) plan, context);
    } else {
      throw new IllegalStateException(StringUtils.format("unexpected plan node type %s",
          plan.getClass()));
    }
  }

  /**
   * Implement PrometheusLogicalMetricScan.
   */
  public PhysicalPlan visitIndexScan(PrometheusLogicalMetricScan node,
                                     PrometheusMetricScan context) {
    String query = SeriesSelectionQueryBuilder.build(node.getMetricName(), node.getFilter());

    context.getRequest().setPromQl(query);
    setTimeRangeParameters(node.getFilter(), context);
    context.getRequest()
        .setStep(StepParameterResolver.resolve(context.getRequest().getStartTime(),
            context.getRequest().getEndTime(), null));
    return context;
  }

  /**
   * Implement PrometheusLogicalMetricAgg.
   */
  public PhysicalPlan visitIndexAggregation(PrometheusLogicalMetricAgg node,
                                            PrometheusMetricScan context) {
    setTimeRangeParameters(node.getFilter(), context);
    context.getRequest()
        .setStep(StepParameterResolver.resolve(context.getRequest().getStartTime(),
            context.getRequest().getEndTime(), node.getGroupByList()));
    String step = context.getRequest().getStep();
    String seriesSelectionQuery
        = SeriesSelectionQueryBuilder.build(node.getMetricName(), node.getFilter());

    String aggregateQuery
        = AggregationQueryBuilder.build(node.getAggregatorList(),
        node.getGroupByList());

    String finalQuery = String.format(aggregateQuery, seriesSelectionQuery + "[" + step + "]");
    context.getRequest().setPromQl(finalQuery);

    //Since prometheus response doesn't have any fieldNames in its output.
    //the field names are sent to PrometheusResponse constructor via context.
    setPrometheusResponseFieldNames(node, context);
    return context;
  }

  @Override
  public PhysicalPlan visitRelation(LogicalRelation node,
                                    PrometheusMetricScan context) {
    PrometheusMetricTable prometheusMetricTable = (PrometheusMetricTable) node.getTable();
    String query = SeriesSelectionQueryBuilder.build(node.getRelationName(), null);
    context.getRequest().setPromQl(query);
    setTimeRangeParameters(null, context);
    context.getRequest()
        .setStep(StepParameterResolver.resolve(context.getRequest().getStartTime(),
            context.getRequest().getEndTime(), null));
    return context;
  }

  private void setTimeRangeParameters(Expression filter, PrometheusMetricScan context) {
    TimeRangeParametersResolver timeRangeParametersResolver = new TimeRangeParametersResolver();
    Pair<Long, Long> timeRange = timeRangeParametersResolver.resolve(filter);
    context.getRequest().setStartTime(timeRange.getFirst());
    context.getRequest().setEndTime(timeRange.getSecond());
  }

  private void setPrometheusResponseFieldNames(PrometheusLogicalMetricAgg node,
                                               PrometheusMetricScan context) {
    Optional<NamedExpression> spanExpression = getSpanExpression(node.getGroupByList());
    if (spanExpression.isEmpty()) {
      throw new RuntimeException(
          "Prometheus Catalog doesn't support aggregations without span expression");
    }
    PrometheusResponseFieldNames prometheusResponseFieldNames = new PrometheusResponseFieldNames();
    prometheusResponseFieldNames.setValueFieldName(node.getAggregatorList().get(0).getName());
    prometheusResponseFieldNames.setValueType(node.getAggregatorList().get(0).type());
    prometheusResponseFieldNames.setTimestampFieldName(spanExpression.get().getNameOrAlias());
    prometheusResponseFieldNames.setGroupByList(node.getGroupByList());
    context.setPrometheusResponseFieldNames(prometheusResponseFieldNames);
  }

  private Optional<NamedExpression> getSpanExpression(List<NamedExpression> namedExpressionList) {
    if (namedExpressionList == null) {
      return Optional.empty();
    }
    return namedExpressionList.stream()
        .filter(expression -> expression.getDelegated() instanceof SpanExpression)
        .findFirst();
  }


}
