/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import static org.opensearch.sql.common.setting.Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER;

import java.io.IOException;
import java.util.List;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.adapter.QueryPlanQueryAction;
import org.opensearch.sql.legacy.executor.adapter.QueryPlanRequestBuilder;
import org.opensearch.sql.legacy.executor.join.ElasticJoinExecutor;
import org.opensearch.sql.legacy.executor.multi.MultiRequestExecutorFactory;
import org.opensearch.sql.legacy.expression.domain.BindingTuple;
import org.opensearch.sql.legacy.query.AggregationQueryAction;
import org.opensearch.sql.legacy.query.DefaultQueryAction;
import org.opensearch.sql.legacy.query.DeleteQueryAction;
import org.opensearch.sql.legacy.query.DescribeQueryAction;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.ShowQueryAction;
import org.opensearch.sql.legacy.query.SqlElasticRequestBuilder;
import org.opensearch.sql.legacy.query.SqlOpenSearchRequestBuilder;
import org.opensearch.sql.legacy.query.join.JoinRequestBuilder;
import org.opensearch.sql.legacy.query.join.OpenSearchJoinQueryAction;
import org.opensearch.sql.legacy.query.multi.MultiQueryAction;
import org.opensearch.sql.legacy.query.multi.MultiQueryRequestBuilder;

/** Created by Eliran on 3/10/2015. */
public class QueryActionElasticExecutor {
  public static SearchHits executeSearchAction(DefaultQueryAction searchQueryAction)
      throws SqlParseException {
    SqlOpenSearchRequestBuilder builder = searchQueryAction.explain();
    return ((SearchResponse) builder.get()).getHits();
  }

  public static SearchHits executeJoinSearchAction(
      Client client, OpenSearchJoinQueryAction joinQueryAction)
      throws IOException, SqlParseException {
    SqlElasticRequestBuilder joinRequestBuilder = joinQueryAction.explain();

    boolean isSearchAfter =
        LocalClusterState.state().getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER);
    if (isSearchAfter) {
      JoinRequestBuilder requestBuilder = (JoinRequestBuilder) joinRequestBuilder;
      requestBuilder.updateRequestWithPit(client);
    }
    ElasticJoinExecutor executor =
        ElasticJoinExecutor.createJoinExecutor(client, joinRequestBuilder);
    executor.run();

    if (isSearchAfter) {
      JoinRequestBuilder requestBuilder = (JoinRequestBuilder) joinRequestBuilder;
      requestBuilder.getPit().deletePointInTime(client);
    }
    return executor.getHits();
  }

  public static Aggregations executeAggregationAction(AggregationQueryAction aggregationQueryAction)
      throws SqlParseException {
    SqlOpenSearchRequestBuilder select = aggregationQueryAction.explain();
    return ((SearchResponse) select.get()).getAggregations();
  }

  public static List<BindingTuple> executeQueryPlanQueryAction(
      QueryPlanQueryAction queryPlanQueryAction) {
    QueryPlanRequestBuilder select = (QueryPlanRequestBuilder) queryPlanQueryAction.explain();
    return select.execute();
  }

  public static ActionResponse executeShowQueryAction(ShowQueryAction showQueryAction) {
    return showQueryAction.explain().get();
  }

  public static ActionResponse executeDescribeQueryAction(DescribeQueryAction describeQueryAction) {
    return describeQueryAction.explain().get();
  }

  public static ActionResponse executeDeleteAction(DeleteQueryAction deleteQueryAction)
      throws SqlParseException {
    return deleteQueryAction.explain().get();
  }

  public static SearchHits executeMultiQueryAction(Client client, MultiQueryAction queryAction)
      throws SqlParseException, IOException {
    SqlElasticRequestBuilder multiRequestBuilder = queryAction.explain();

    boolean isSearchAfter =
        LocalClusterState.state().getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER);
    if (isSearchAfter) {
      ((MultiQueryRequestBuilder) multiRequestBuilder).updateRequestWithPit(client);
    }

    ElasticHitsExecutor executor =
        MultiRequestExecutorFactory.createExecutor(
            client, (MultiQueryRequestBuilder) multiRequestBuilder);
    executor.run();

    if (isSearchAfter) {
      ((MultiQueryRequestBuilder) multiRequestBuilder).getPit().deletePointInTime(client);
    }
    return executor.getHits();
  }

  public static Object executeAnyAction(Client client, QueryAction queryAction)
      throws SqlParseException, IOException {
    if (queryAction instanceof DefaultQueryAction) {
      return executeSearchAction((DefaultQueryAction) queryAction);
    }
    if (queryAction instanceof AggregationQueryAction) {
      return executeAggregationAction((AggregationQueryAction) queryAction);
    }
    if (queryAction instanceof QueryPlanQueryAction) {
      return executeQueryPlanQueryAction((QueryPlanQueryAction) queryAction);
    }
    if (queryAction instanceof ShowQueryAction) {
      return executeShowQueryAction((ShowQueryAction) queryAction);
    }
    if (queryAction instanceof DescribeQueryAction) {
      return executeDescribeQueryAction((DescribeQueryAction) queryAction);
    }
    if (queryAction instanceof OpenSearchJoinQueryAction) {
      return executeJoinSearchAction(client, (OpenSearchJoinQueryAction) queryAction);
    }
    if (queryAction instanceof MultiQueryAction) {
      return executeMultiQueryAction(client, (MultiQueryAction) queryAction);
    }
    if (queryAction instanceof DeleteQueryAction) {
      return executeDeleteAction((DeleteQueryAction) queryAction);
    }
    return null;
  }
}
