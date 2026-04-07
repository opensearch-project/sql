/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.analytics.AnalyticsExecutionEngine;
import org.opensearch.sql.lang.LangSpec;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Handles queries routed to the Analytics engine via the unified query pipeline. Parses PPL/SQL
 * queries using {@link UnifiedQueryPlanner} to generate a Calcite {@link RelNode}, then delegates
 * to {@link AnalyticsExecutionEngine} for execution.
 */
public class RestUnifiedQueryAction {

  private static final Logger LOG = LogManager.getLogger(RestUnifiedQueryAction.class);
  private static final String SCHEMA_NAME = "opensearch";

  private final AnalyticsExecutionEngine analyticsEngine;
  private final NodeClient client;
  private final ClusterService clusterService;

  public RestUnifiedQueryAction(
      NodeClient client,
      ClusterService clusterService,
      QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
    this.client = client;
    this.clusterService = clusterService;
    this.analyticsEngine = new AnalyticsExecutionEngine(planExecutor);
  }

  /**
   * Check if the query targets an analytics engine index (e.g., Parquet-backed). Uses the context's
   * parser for index name extraction, supporting both PPL and SQL.
   *
   * <p>Note: This creates a separate UnifiedQueryContext for parsing. The context cannot be shared
   * with doExecute/doExplain because UnifiedQueryContext holds a Calcite JDBC connection that fails
   * when used across threads (transport thread -> sql-worker thread). When real catalog metadata
   * makes this expensive, consider moving the routing check to the sql-worker thread.
   */
  public boolean isAnalyticsIndex(String query, QueryType queryType) {
    if (query == null || query.isEmpty()) {
      return false;
    }
    try (UnifiedQueryContext context = buildParsingContext(queryType)) {
      String indexName = extractIndexName(query, queryType, context);
      if (indexName == null) {
        return false;
      }
      int lastDot = indexName.lastIndexOf('.');
      String tableName = lastDot >= 0 ? indexName.substring(lastDot + 1) : indexName;
      return tableName.startsWith("parquet_");
    } catch (Exception e) {
      return false;
    }
  }

  /** Execute a query through the unified query pipeline on the sql-worker thread pool. */
  public void execute(
      String query,
      QueryType queryType,
      PPLQueryRequest pplRequest,
      ActionListener<TransportPPLQueryResponse> listener) {
    client
        .threadPool()
        .schedule(
            withCurrentContext(() -> doExecute(query, queryType, pplRequest, listener)),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  /**
   * Explain a query through the unified query pipeline on the sql-worker thread pool. Returns
   * ExplainResponse via ResponseListener so the caller (TransportPPLQueryAction) can format it
   * using its own createExplainResponseListener.
   */
  public void explain(
      String query,
      QueryType queryType,
      PPLQueryRequest pplRequest,
      ResponseListener<ExplainResponse> listener) {
    client
        .threadPool()
        .schedule(
            withCurrentContext(() -> doExplain(query, queryType, pplRequest, listener)),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  private void doExecute(
      String query,
      QueryType queryType,
      PPLQueryRequest pplRequest,
      ActionListener<TransportPPLQueryResponse> listener) {
    try (UnifiedQueryContext context = buildContext(queryType, pplRequest.profile())) {
      UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
      RelNode plan = planner.plan(query);

      CalcitePlanContext planContext = context.getPlanContext();
      plan = addQuerySizeLimit(plan, planContext);

      analyticsEngine.execute(plan, planContext, createQueryListener(queryType, listener));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private void doExplain(
      String query,
      QueryType queryType,
      PPLQueryRequest pplRequest,
      ResponseListener<ExplainResponse> listener) {
    try (UnifiedQueryContext context = buildContext(queryType, pplRequest.profile())) {
      UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
      RelNode plan = planner.plan(query);

      CalcitePlanContext planContext = context.getPlanContext();
      plan = addQuerySizeLimit(plan, planContext);

      analyticsEngine.explain(plan, pplRequest.mode(), planContext, listener);
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  /**
   * Build a lightweight context for parsing only (index name extraction). Does not require cluster
   * state or catalog schema.
   */
  private static UnifiedQueryContext buildParsingContext(QueryType queryType) {
    return UnifiedQueryContext.builder().language(queryType).build();
  }

  private UnifiedQueryContext buildContext(QueryType queryType, boolean profiling) {
    return UnifiedQueryContext.builder()
        .language(queryType)
        .catalog(SCHEMA_NAME, OpenSearchSchemaBuilder.buildSchema(clusterService.state()))
        .defaultNamespace(SCHEMA_NAME)
        .profiling(profiling)
        .build();
  }

  /**
   * Extract the source index name by parsing the query and visiting the AST to find the Relation
   * node. Uses the context's parser which supports both PPL and SQL.
   */
  private static String extractIndexName(
      String query, QueryType queryType, UnifiedQueryContext context) {
    if (queryType == QueryType.PPL) {
      UnresolvedPlan unresolvedPlan = (UnresolvedPlan) context.getParser().parse(query);
      return unresolvedPlan.accept(new IndexNameExtractor(), null);
    }
    SqlNode sqlNode = (SqlNode) context.getParser().parse(query);
    return extractTableNameFromSqlNode(sqlNode);
  }

  /**
   * Execute a SQL query through the unified query pipeline. Uses {@link
   * org.opensearch.sql.plugin.transport.TransportPPLQueryResponse} as the transport response type
   * since both PPL and SQL share the same JSON response format.
   */
  public void executeSql(
      String query, QueryType queryType, ActionListener<TransportPPLQueryResponse> listener) {
    client
        .threadPool()
        .schedule(
            withCurrentContext(
                () -> {
                  try (UnifiedQueryContext context = buildContext(queryType, false)) {
                    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
                    RelNode plan = planner.plan(query);
                    CalcitePlanContext planContext = context.getPlanContext();
                    plan = addQuerySizeLimit(plan, planContext);
                    analyticsEngine.execute(
                        plan, planContext, createQueryListener(queryType, listener));
                  } catch (Exception e) {
                    listener.onFailure(e);
                  }
                }),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  /** Explain a SQL query through the unified query pipeline. */
  public void explainSql(
      String query, QueryType queryType, ResponseListener<ExplainResponse> listener) {
    client
        .threadPool()
        .schedule(
            withCurrentContext(
                () -> {
                  try (UnifiedQueryContext context = buildContext(queryType, false)) {
                    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
                    RelNode plan = planner.plan(query);
                    CalcitePlanContext planContext = context.getPlanContext();
                    plan = addQuerySizeLimit(plan, planContext);
                    analyticsEngine.explain(plan, ExplainMode.STANDARD, planContext, listener);
                  } catch (Exception e) {
                    listener.onFailure(e);
                  }
                }),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  /** Extracts the table name from a Calcite SqlNode parse tree. */
  private static String extractTableNameFromSqlNode(SqlNode sqlNode) {
    if (sqlNode instanceof SqlSelect select) {
      SqlNode from = select.getFrom();
      if (from instanceof SqlIdentifier id) {
        return id.toString();
      }
      if (from instanceof SqlJoin join) {
        // For joins, extract from the left table
        if (join.getLeft() instanceof SqlIdentifier leftId) {
          return leftId.toString();
        }
      }
    }
    return null;
  }

  /** AST visitor that extracts the source index name from a Relation node. */
  private static class IndexNameExtractor extends AbstractNodeVisitor<String, Void> {
    @Override
    public String visitRelation(Relation node, Void context) {
      return node.getTableQualifiedName().toString();
    }
  }

  private static RelNode addQuerySizeLimit(RelNode plan, CalcitePlanContext context) {
    return LogicalSystemLimit.create(
        LogicalSystemLimit.SystemLimitType.QUERY_SIZE_LIMIT,
        plan,
        context.relBuilder.literal(context.sysLimit.querySizeLimit()));
  }

  private ResponseListener<QueryResponse> createQueryListener(
      QueryType queryType, ActionListener<TransportPPLQueryResponse> transportListener) {
    ResponseFormatter<QueryResult> formatter = new SimpleJsonResponseFormatter(PRETTY);
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
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

  private static Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }
}
