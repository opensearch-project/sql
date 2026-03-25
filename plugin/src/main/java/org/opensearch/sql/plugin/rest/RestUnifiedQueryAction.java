/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.datasources.exceptions.DataSourceClientException;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.analytics.AnalyticsExecutionEngine;
import org.opensearch.sql.executor.analytics.QueryPlanExecutor;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.plugin.rest.analytics.stub.StubIndexDetector;
import org.opensearch.sql.plugin.rest.analytics.stub.StubSchemaProvider;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.JdbcResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Handles queries routed to the Analytics engine via the unified query pipeline. Parses PPL queries
 * using {@link UnifiedQueryPlanner} to generate a Calcite {@link RelNode}, then delegates to {@link
 * AnalyticsExecutionEngine} for execution.
 */
public class RestUnifiedQueryAction {

  private static final Logger LOG = LogManager.getLogger(RestUnifiedQueryAction.class);
  private static final String SCHEMA_NAME = "opensearch";

  private final AnalyticsExecutionEngine analyticsEngine;
  private final NodeClient client;

  public RestUnifiedQueryAction(NodeClient client, QueryPlanExecutor planExecutor) {
    this.client = client;
    this.analyticsEngine = new AnalyticsExecutionEngine(planExecutor);
  }

  /**
   * Check if the query targets an analytics engine index. Delegates to {@link StubIndexDetector}
   * which will be replaced by UnifiedQueryParser and index settings when available.
   */
  public static boolean isAnalyticsIndex(String query) {
    return StubIndexDetector.isAnalyticsIndex(query);
  }

  /**
   * Execute a query through the unified query pipeline on the sql-worker thread pool.
   *
   * @param query the PPL query string
   * @param queryType SQL or PPL
   * @param channel the REST channel for sending the response
   */
  public void execute(String query, QueryType queryType, RestChannel channel) {
    client
        .threadPool()
        .schedule(
            () -> doExecute(query, queryType, channel),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  private void doExecute(String query, QueryType queryType, RestChannel channel) {
    try {
      long startTime = System.nanoTime();

      // TODO: Replace with EngineContext.getSchema() when analytics engine is ready
      AbstractSchema schema = StubSchemaProvider.buildSchema();

      try (UnifiedQueryContext context =
          UnifiedQueryContext.builder()
              .language(queryType)
              .catalog(SCHEMA_NAME, schema)
              .defaultNamespace(SCHEMA_NAME)
              .build()) {

        UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
        RelNode plan = planner.plan(query);
        long planTime = System.nanoTime();
        LOG.info(
            "[unified] Planning completed in {}ms for {} query",
            (planTime - startTime) / 1_000_000,
            queryType);

        CalcitePlanContext planContext = context.getPlanContext();
        analyticsEngine.execute(plan, planContext, createQueryListener(channel, planTime));
      }
    } catch (Exception e) {
      recordFailureMetric(e);
      reportError(channel, e);
    }
  }

  private ResponseListener<QueryResponse> createQueryListener(
      RestChannel channel, long planEndTime) {
    ResponseFormatter<QueryResult> formatter = new JdbcResponseFormatter(PRETTY);
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        long execTime = System.nanoTime();
        LOG.info(
            "[unified] Execution completed in {}ms, {} rows returned",
            (execTime - planEndTime) / 1_000_000,
            response.getResults().size());
        Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
        Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();
        String result =
            formatter.format(
                new QueryResult(
                    response.getSchema(), response.getResults(), response.getCursor(), PPL_SPEC));
        channel.sendResponse(new BytesRestResponse(OK, formatter.contentType(), result));
      }

      @Override
      public void onFailure(Exception e) {
        recordFailureMetric(e);
        reportError(channel, e);
      }
    };
  }

  /**
   * Classify whether the exception is a client error (bad query) or server error (engine bug).
   * Matches the classification in {@link RestPPLQueryAction#isClientError}.
   */
  private static boolean isClientError(Exception e) {
    return e instanceof NullPointerException
        || e instanceof IllegalArgumentException
        || e instanceof IndexNotFoundException
        || e instanceof SemanticCheckException
        || e instanceof ExpressionEvaluationException
        || e instanceof QueryEngineException
        || e instanceof SyntaxCheckException
        || e instanceof DataSourceClientException
        || e instanceof IllegalAccessException;
  }

  private static void recordFailureMetric(Exception e) {
    if (isClientError(e)) {
      LOG.warn("[unified] Client error in query execution", e);
      Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_CUS).increment();
    } else {
      LOG.error("[unified] Server error in query execution", e);
      Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_SYS).increment();
    }
  }

  private static void reportError(RestChannel channel, Exception e) {
    RestStatus status =
        isClientError(e) ? RestStatus.BAD_REQUEST : RestStatus.INTERNAL_SERVER_ERROR;
    channel.sendResponse(
        new BytesRestResponse(
            status, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
  }
}
