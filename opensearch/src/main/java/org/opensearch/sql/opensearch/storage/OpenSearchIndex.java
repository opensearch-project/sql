/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.storage;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.planner.logical.OpenSearchLogicalIndexAgg;
import org.opensearch.sql.opensearch.planner.logical.OpenSearchLogicalIndexScan;
import org.opensearch.sql.opensearch.planner.logical.OpenSearchLogicalPlanOptimizerFactory;
import org.opensearch.sql.opensearch.planner.physical.ADOperator;
import org.opensearch.sql.opensearch.planner.physical.MLCommonsOperator;
import org.opensearch.sql.opensearch.client.IPrometheusService;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.request.system.OpenSearchDescribeIndexRequest;
import org.opensearch.sql.opensearch.response.agg.OpenSearchAggregationResponseParser;
import org.opensearch.sql.opensearch.storage.script.aggregation.AggregationQueryBuilder;
import org.opensearch.sql.opensearch.storage.script.filter.FilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.script.filter.PromFilterQuery;
import org.opensearch.sql.opensearch.storage.script.filter.PromQLFilterQueryBuilder;
import org.opensearch.sql.opensearch.storage.script.sort.SortQueryBuilder;
import org.opensearch.sql.opensearch.storage.serialization.DefaultExpressionSerializer;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalAD;
import org.opensearch.sql.planner.logical.LogicalMLCommons;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/** OpenSearch table (index) implementation. */
public class OpenSearchIndex implements Table {

  /** OpenSearch client connection. */
  private final OpenSearchClient client;
  private final IPrometheusService prometheusService;

  private final Settings settings;

  /**
   * {@link OpenSearchRequest.IndexName}.
   */
  private final OpenSearchRequest.IndexName indexName;

  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * Constructor.
   */
  public OpenSearchIndex(OpenSearchClient client, IPrometheusService prometheusService, Settings settings, String indexName) {
    this.client = client;
    this.prometheusService = prometheusService;
    this.settings = settings;
    this.indexName = new OpenSearchRequest.IndexName(indexName);
  }

  /*
   * TODO: Assume indexName doesn't have wildcard.
   *  Need to either handle field name conflicts
   *   or lazy evaluate when query engine pulls field type.
   */
  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      cachedFieldTypes = new OpenSearchDescribeIndexRequest(client, prometheusService, indexName).getFieldTypes();
    }
    return cachedFieldTypes;
  }

  /**
   * TODO: Push down operations to index scan operator as much as possible in future.
   */
  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    OpenSearchIndexScan indexScan = new OpenSearchIndexScan(client, prometheusService, settings, indexName,
        new OpenSearchExprValueFactory(getFieldTypes()));

    /*
     * Visit logical plan with index scan as context so logical operators visited, such as
     * aggregation, filter, will accumulate (push down) OpenSearch query and aggregation DSL on
     * index scan.
     */
    return plan.accept(new OpenSearchDefaultImplementor(indexScan, client), indexScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return OpenSearchLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public static class OpenSearchDefaultImplementor
      extends DefaultImplementor<OpenSearchIndexScan> {
    private final OpenSearchIndexScan indexScan;

    private final OpenSearchClient client;

    @Override
    public PhysicalPlan visitNode(LogicalPlan plan, OpenSearchIndexScan context) {
      if (plan instanceof OpenSearchLogicalIndexScan) {
        return visitIndexScan((OpenSearchLogicalIndexScan) plan, context);
      } else if (plan instanceof OpenSearchLogicalIndexAgg) {
        return visitIndexAggregation((OpenSearchLogicalIndexAgg) plan, context);
      } else {
        throw new IllegalStateException(StringUtils.format("unexpected plan node type %s",
            plan.getClass()));
      }
    }

    /**
     * Implement ElasticsearchLogicalIndexScan.
     */
    public PhysicalPlan visitIndexScan(OpenSearchLogicalIndexScan node,
                                       OpenSearchIndexScan context) {
      if (null != node.getSortList()) {
        final SortQueryBuilder builder = new SortQueryBuilder();
        context.pushDownSort(node.getSortList().stream()
            .map(sort -> builder.build(sort.getValue(), sort.getKey()))
            .collect(Collectors.toList()));
      }
      OpenSearchRequest request = context.getRequest();
      StringBuilder promQlBuilder = context.getRequest().getPrometheusQueryBuilder();
      promQlBuilder.append(node.getRelationName().split("\\.")[1]);
      if (null != node.getFilter()) {
        FilterQueryBuilder queryBuilder = new FilterQueryBuilder(new DefaultExpressionSerializer());
        QueryBuilder query = queryBuilder.build(node.getFilter());
        context.pushDown(query);
        PromQLFilterQueryBuilder promQLFilterQueryBuilder = new PromQLFilterQueryBuilder();
        PromFilterQuery filterQuery = promQLFilterQueryBuilder.build(node.getFilter());
        if(filterQuery.getPromQl()!=null && filterQuery.getPromQl().length() > 0) {
          filterQuery.setPromQl(new StringBuilder().append("{").append(filterQuery.getPromQl()).append("}"));
          promQlBuilder.append(filterQuery.getPromQl());
        }

        if(filterQuery.getStartTime()!= null) {
          request.setStartTime(filterQuery.getStartTime());
        }
        if(filterQuery.getEndTime() != null) {
          request.setEndTime(filterQuery.getEndTime());
        }
      }
      if (node.getLimit() != null) {
        context.pushDownLimit(node.getLimit(), node.getOffset());
      }

      if (node.hasProjects()) {
        context.pushDownProjects(node.getProjectList());
      }

      if (node.getFilter() != null) {

      }
      promQlBuilder.insert(0, "(");
      promQlBuilder.append(")");

      return indexScan;
    }

    /**
     * Implement ElasticsearchLogicalIndexAgg.
     */
    public PhysicalPlan visitIndexAggregation(OpenSearchLogicalIndexAgg node,
                                              OpenSearchIndexScan context) {
      OpenSearchRequest request = context.getRequest();
      StringBuilder promQlBuilder = context.getRequest().getPrometheusQueryBuilder();
      promQlBuilder.append(node.getRelationName().split("\\.")[1]);
      if (node.getFilter() != null) {
        FilterQueryBuilder queryBuilder = new FilterQueryBuilder(
            new DefaultExpressionSerializer());
        PromQLFilterQueryBuilder promQLFilterQueryBuilder = new PromQLFilterQueryBuilder();
        PromFilterQuery filterQuery = promQLFilterQueryBuilder.build(node.getFilter());
        QueryBuilder query = queryBuilder.build(node.getFilter());
        context.pushDown(query);
        if(filterQuery.getPromQl()!=null && filterQuery.getPromQl().length() > 0) {
          filterQuery.setPromQl(new StringBuilder().append("{").append(filterQuery.getPromQl()).append("}"));
          promQlBuilder.append(filterQuery.getPromQl());
        }


        if(filterQuery.getStartTime()!= null) {
          request.setStartTime(filterQuery.getStartTime());
        }
        if(filterQuery.getEndTime() != null) {
          request.setEndTime(filterQuery.getEndTime());
        }
      }
      promQlBuilder.insert(0, "(");
      promQlBuilder.append(")");
      AggregationQueryBuilder builder =
          new AggregationQueryBuilder(new DefaultExpressionSerializer());
      Pair<List<AggregationBuilder>, OpenSearchAggregationResponseParser> aggregationBuilder =
          builder.buildAggregationBuilder(node.getAggregatorList(),
              node.getGroupByList(), node.getSortList());
      context.pushDownAggregation(aggregationBuilder);
      context.pushTypeMapping(
              builder.buildTypeMapping(node.getAggregatorList(),
                      node.getGroupByList()));
      StringBuilder aggregateQuery = new StringBuilder();
      if(!node.getAggregatorList().isEmpty()) {
        aggregateQuery.insert(0, node.getAggregatorList().get(0).getFunctionName().getFunctionName() + " ");
        if(!node.getGroupByList().isEmpty()){
          Optional<SpanExpression> spanExpression = node.getGroupByList().stream().map(NamedExpression::getDelegated)
                .filter(delegated -> delegated instanceof SpanExpression)
                .map(delegated -> (SpanExpression) delegated)
                .findFirst();
          spanExpression.ifPresent(expression -> request.setStep(expression.getValue().toString() + expression.getUnit().getName()));
        long groupByCount = node.getGroupByList().stream().map(NamedExpression::getDelegated)
                .filter(delegated -> !(delegated instanceof SpanExpression)).count();
        if (groupByCount > 0) {
          aggregateQuery.append("by (");
          for (int i = 0; i < node.getGroupByList().size(); i++) {
            NamedExpression expression = node.getGroupByList().get(i);
            if(expression.getDelegated() instanceof SpanExpression)
              continue;
            if (i == node.getGroupByList().size() - 1) {
              aggregateQuery.append(expression.getName());
            } else {
              aggregateQuery.append(expression.getName()).append(", ");
            }
          }
          aggregateQuery.append(")");
        }
      }
      }
      promQlBuilder.insert(0, aggregateQuery);

      return indexScan;
    }

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, OpenSearchIndexScan context) {
      return indexScan;
    }

    @Override
    public PhysicalPlan visitMLCommons(LogicalMLCommons node, OpenSearchIndexScan context) {
      return new MLCommonsOperator(visitChild(node, context), node.getAlgorithm(),
              node.getArguments(), client.getNodeClient());
    }

    @Override
    public PhysicalPlan visitAD(LogicalAD node, OpenSearchIndexScan context) {
      return new ADOperator(visitChild(node, context),
              node.getArguments(), client.getNodeClient());
    }
  }
}
