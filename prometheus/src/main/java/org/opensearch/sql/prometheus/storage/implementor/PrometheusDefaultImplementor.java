/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.storage.implementor;

import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.LABELS;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.TIMESTAMP;
import static org.opensearch.sql.prometheus.data.constants.PrometheusFieldConstants.VALUE;

import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.util.Pair;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalProject;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.ProjectOperator;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalMetricAgg;
import org.opensearch.sql.prometheus.planner.logical.PrometheusLogicalMetricScan;
import org.opensearch.sql.prometheus.storage.PrometheusMetricScan;
import org.opensearch.sql.prometheus.storage.PrometheusMetricTable;
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
    SeriesSelectionQueryBuilder seriesSelectionQueryBuilder = new SeriesSelectionQueryBuilder();
    String query = seriesSelectionQueryBuilder.build(node.getMetricName(), node.getFilter());

    context.getRequest().setPromQl(query);
    setTimeRangeParameters(node.getFilter(), context);
    context.getRequest()
        .setStep(new StepParameterResolver().resolve(context.getRequest().getStartTime(),
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
        .setStep(new StepParameterResolver().resolve(context.getRequest().getStartTime(),
            context.getRequest().getEndTime(), node.getGroupByList()));
    String step = context.getRequest().getStep();
    SeriesSelectionQueryBuilder seriesSelectionQueryBuilder = new SeriesSelectionQueryBuilder();
    String seriesSelectionQuery
        = seriesSelectionQueryBuilder.build(node.getMetricName(), node.getFilter());

    AggregationQueryBuilder aggregationQueryBuilder = new AggregationQueryBuilder();
    String aggregateQuery
        = aggregationQueryBuilder.build(node.getAggregatorList(),
        node.getGroupByList());

    String finalQuery = String.format(aggregateQuery, seriesSelectionQuery + "[" + step + "]");
    context.getRequest().setPromQl(finalQuery);
    return context;
  }

  @Override
  public PhysicalPlan visitRelation(LogicalRelation node,
                                    PrometheusMetricScan context) {
    PrometheusMetricTable prometheusMetricTable = (PrometheusMetricTable) node.getTable();
    if (prometheusMetricTable.getMetricName() != null) {
      SeriesSelectionQueryBuilder seriesSelectionQueryBuilder = new SeriesSelectionQueryBuilder();
      String query = seriesSelectionQueryBuilder.build(node.getRelationName(), null);
      context.getRequest().setPromQl(query);
      setTimeRangeParameters(null, context);
      context.getRequest()
          .setStep(new StepParameterResolver().resolve(context.getRequest().getStartTime(),
              context.getRequest().getEndTime(), null));
    }
    return context;
  }

  // Since getFieldTypes include labels
  // we are explicitly specifying the output column names;
  @Override
  public PhysicalPlan visitProject(LogicalProject node, PrometheusMetricScan context) {
    for (NamedExpression namedExpression : node.getProjectList()) {
      if (namedExpression.getDelegated().type().equals(ExprCoreType.DOUBLE)
          || namedExpression.getDelegated().type().equals(ExprCoreType.INTEGER)) {
        context.setValueFieldName(namedExpression.getName());
      } else if (namedExpression.getDelegated().type().equals(ExprCoreType.TIMESTAMP)) {
        context.setTimestampFieldName(namedExpression.getName());
      }
    }
    return new ProjectOperator(visitChild(node, context), node.getProjectList(),
        node.getNamedParseExpressions());
  }

  private void setTimeRangeParameters(Expression filter, PrometheusMetricScan context) {
    TimeRangeParametersResolver timeRangeParametersResolver = new TimeRangeParametersResolver();
    Pair<Long, Long> timeRange = timeRangeParametersResolver.resolve(filter);
    context.getRequest().setStartTime(timeRange.getFirst());
    context.getRequest().setEndTime(timeRange.getSecond());
  }


}