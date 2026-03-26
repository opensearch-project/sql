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

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.datasources.exceptions.DataSourceClientException;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.analytics.AnalyticsExecutionEngine;
import org.opensearch.sql.executor.analytics.QueryPlanExecutor;
import org.opensearch.sql.lang.LangSpec;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.plugin.rest.analytics.stub.StubIndexDetector;
import org.opensearch.sql.plugin.rest.analytics.stub.StubSchemaProvider;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
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
            withCurrentContext(() -> doExecute(query, queryType, channel)),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  /**
   * Execute a query through the unified query pipeline, returning the result via transport action
   * listener. Called from {@code TransportPPLQueryAction} for proper PPL enabled check, metrics,
   * and request ID handling.
   *
   * @param query the PPL query string
   * @param queryType SQL or PPL
   * @param pplRequest the original PPL request (for format selection)
   * @param listener the transport action listener
   */
  public void executeViaTransport(
      String query,
      QueryType queryType,
      PPLQueryRequest pplRequest,
      ActionListener<TransportPPLQueryResponse> listener) {
    client
        .threadPool()
        .schedule(
            withCurrentContext(() -> doExecuteViaTransport(query, queryType, pplRequest, listener)),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  private void doExecuteViaTransport(
      String query,
      QueryType queryType,
      PPLQueryRequest pplRequest,
      ActionListener<TransportPPLQueryResponse> listener) {
    try {
      long startTime = System.nanoTime();
      AbstractSchema schema = StubSchemaProvider.buildSchema();

      try (UnifiedQueryContext context =
          UnifiedQueryContext.builder()
              .language(queryType)
              .catalog(SCHEMA_NAME, schema)
              .defaultNamespace(SCHEMA_NAME)
              .build()) {

        UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
        RelNode plan = planner.plan(query);

        // Add query size limit to the plan so the analytics engine can enforce it
        // during execution, consistent with PPL V3 (see QueryService.convertToCalcitePlan)
        CalcitePlanContext planContext = context.getPlanContext();
        plan = addQuerySizeLimit(plan, planContext);

        long planTime = System.nanoTime();
        LOG.info(
            "[unified] Planning completed in {}ms for {} query",
            (planTime - startTime) / 1_000_000,
            queryType);

        analyticsEngine.execute(
            plan, planContext, createTransportQueryListener(queryType, planTime, listener));
      }
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Add a system-level query size limit to the plan. This ensures the analytics engine enforces the
   * limit during execution rather than returning all rows for post-processing truncation.
   */
  private static RelNode addQuerySizeLimit(RelNode plan, CalcitePlanContext context) {
    return LogicalSystemLimit.create(
        LogicalSystemLimit.SystemLimitType.QUERY_SIZE_LIMIT,
        plan,
        context.relBuilder.literal(context.sysLimit.querySizeLimit()));
  }

  private ResponseListener<QueryResponse> createTransportQueryListener(
      QueryType queryType,
      long planEndTime,
      ActionListener<TransportPPLQueryResponse> transportListener) {
    ResponseFormatter<QueryResult> formatter = new JdbcResponseFormatter(PRETTY);
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        long execTime = System.nanoTime();
        LOG.info(
            "[unified] Execution completed in {}ms, {} rows returned",
            (execTime - planEndTime) / 1_000_000,
            response.getResults().size());
        LangSpec langSpec = queryType == QueryType.PPL ? PPL_SPEC : LangSpec.SQL_SPEC;
        String result =
            formatter.format(
                new QueryResult(
                    response.getSchema(), response.getResults(), response.getCursor(), langSpec));
        transportListener.onResponse(new TransportPPLQueryResponse(result));
      }

      @Override
      public void onFailure(Exception e) {
        transportListener.onFailure(e);
      }
    };
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

        CalcitePlanContext planContext = context.getPlanContext();
        plan = addQuerySizeLimit(plan, planContext);

        long planTime = System.nanoTime();
        LOG.info(
            "[unified] Planning completed in {}ms for {} query",
            (planTime - startTime) / 1_000_000,
            queryType);

        analyticsEngine.execute(
            plan, planContext, createQueryListener(queryType, channel, planTime));
      }
    } catch (Exception e) {
      recordFailureMetric(queryType, e);
      reportError(channel, e);
    }
  }

  private ResponseListener<QueryResponse> createQueryListener(
      QueryType queryType, RestChannel channel, long planEndTime) {
    ResponseFormatter<QueryResult> formatter = new JdbcResponseFormatter(PRETTY);
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        long execTime = System.nanoTime();
        LOG.info(
            "[unified] Execution completed in {}ms, {} rows returned",
            (execTime - planEndTime) / 1_000_000,
            response.getResults().size());
        recordSuccessMetric(queryType);
        LangSpec langSpec = queryType == QueryType.PPL ? PPL_SPEC : LangSpec.SQL_SPEC;
        String result =
            formatter.format(
                new QueryResult(
                    response.getSchema(), response.getResults(), response.getCursor(), langSpec));
        channel.sendResponse(new BytesRestResponse(OK, formatter.contentType(), result));
      }

      @Override
      public void onFailure(Exception e) {
        recordFailureMetric(queryType, e);
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

  private static void recordSuccessMetric(QueryType queryType) {
    if (queryType == QueryType.PPL) {
      Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
      Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();
    } else {
      Metrics.getInstance().getNumericalMetric(MetricName.REQ_TOTAL).increment();
      Metrics.getInstance().getNumericalMetric(MetricName.REQ_COUNT_TOTAL).increment();
    }
  }

  private static void recordFailureMetric(QueryType queryType, Exception e) {
    if (isClientError(e)) {
      LOG.warn("[unified] Client error in query execution", e);
      Metrics.getInstance()
          .getNumericalMetric(
              queryType == QueryType.PPL
                  ? MetricName.PPL_FAILED_REQ_COUNT_CUS
                  : MetricName.FAILED_REQ_COUNT_CUS)
          .increment();
    } else {
      LOG.error("[unified] Server error in query execution", e);
      Metrics.getInstance()
          .getNumericalMetric(
              queryType == QueryType.PPL
                  ? MetricName.PPL_FAILED_REQ_COUNT_SYS
                  : MetricName.FAILED_REQ_COUNT_SYS)
          .increment();
    }
  }

  /**
   * Capture current thread context and restore it on the worker thread. Ensures security context
   * (user identity, permissions) is propagated. Same pattern as {@link
   * org.opensearch.sql.opensearch.executor.OpenSearchQueryManager#withCurrentContext}.
   */
  private static Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }

  private static void reportError(RestChannel channel, Exception e) {
    RestStatus status =
        isClientError(e) ? RestStatus.BAD_REQUEST : RestStatus.INTERNAL_SERVER_ERROR;
    channel.sendResponse(
        new BytesRestResponse(
            status, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
  }
}
