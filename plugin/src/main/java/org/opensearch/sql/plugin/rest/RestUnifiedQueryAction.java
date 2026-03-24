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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.analytics.AnalyticsExecutionEngine;
import org.opensearch.sql.executor.analytics.QueryPlanExecutor;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
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

  /**
   * Pattern to extract index name from PPL source clause. Matches: source = index, source=index,
   * source = `index`, source = catalog.index
   */
  private static final Pattern SOURCE_PATTERN =
      Pattern.compile(
          "source\\s*=\\s*`?([a-zA-Z0-9_.*]+(?:\\.[a-zA-Z0-9_.*]+)*)`?", Pattern.CASE_INSENSITIVE);

  private final AnalyticsExecutionEngine analyticsEngine;
  private final NodeClient client;

  public RestUnifiedQueryAction(NodeClient client, QueryPlanExecutor planExecutor) {
    this.client = client;
    this.analyticsEngine = new AnalyticsExecutionEngine(planExecutor);
  }

  /**
   * Check if the query targets an analytics engine index (e.g., Parquet-backed). Currently uses a
   * prefix convention ("parquet_"). In production, this will check index settings such as
   * index.storage_type.
   */
  public static boolean isAnalyticsIndex(String query) {
    if (query == null) {
      return false;
    }
    String indexName = extractIndexName(query);
    if (indexName == null) {
      return false;
    }
    // Handle qualified names like "catalog.parquet_logs" — check the last segment
    int lastDot = indexName.lastIndexOf('.');
    String tableName = lastDot >= 0 ? indexName.substring(lastDot + 1) : indexName;
    return tableName.startsWith("parquet_");
  }

  /**
   * Extract the source index name from a PPL query string.
   *
   * @param query the PPL query string
   * @return the index name, or null if not found
   */
  static String extractIndexName(String query) {
    Matcher matcher = SOURCE_PATTERN.matcher(query);
    if (matcher.find()) {
      return matcher.group(1);
    }
    return null;
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

      // TODO: Replace stub schema with EngineContext.getSchema() when analytics engine is ready
      AbstractSchema schema = buildStubSchema();

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
   * Stub schema for development and testing. Returns a hardcoded table definition for any
   * "parquet_*" table. Will be replaced by EngineContext.getSchema() when the analytics engine is
   * ready.
   */
  private static AbstractSchema buildStubSchema() {
    return new AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return Map.of(
            "parquet_logs", buildStubTable(),
            "parquet_metrics", buildStubMetricsTable());
      }
    };
  }

  private static Table buildStubTable() {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory
            .builder()
            .add("ts", SqlTypeName.TIMESTAMP)
            .add("status", SqlTypeName.INTEGER)
            .add("message", SqlTypeName.VARCHAR)
            .add("ip_addr", SqlTypeName.VARCHAR)
            .build();
      }
    };
  }

  private static Table buildStubMetricsTable() {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory
            .builder()
            .add("ts", SqlTypeName.TIMESTAMP)
            .add("cpu", SqlTypeName.DOUBLE)
            .add("memory", SqlTypeName.DOUBLE)
            .add("host", SqlTypeName.VARCHAR)
            .build();
      }
    };
  }

  /** Classify whether the exception is a client error (bad query) or server error (engine bug). */
  private static boolean isClientError(Exception e) {
    return e instanceof SyntaxCheckException
        || e instanceof SemanticCheckException
        || e instanceof IllegalArgumentException
        || e instanceof NullPointerException;
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
    String reason = e.getMessage() != null ? e.getMessage() : "Unknown error";
    // Escape characters that would break JSON
    reason =
        reason.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "");
    channel.sendResponse(
        new BytesRestResponse(
            status,
            "application/json; charset=UTF-8",
            "{\"error\":{\"type\":\""
                + e.getClass().getSimpleName()
                + "\",\"reason\":\""
                + reason
                + "\"},\"status\":"
                + status.getStatus()
                + "}"));
  }
}
