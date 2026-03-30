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
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.parser.PPLQueryParser;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.analytics.AnalyticsExecutionEngine;
import org.opensearch.sql.executor.analytics.QueryPlanExecutor;
import org.opensearch.sql.lang.LangSpec;
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
  private final PPLQueryParser pplParser;

  public RestUnifiedQueryAction(
      NodeClient client, QueryPlanExecutor planExecutor, Settings settings) {
    this.client = client;
    this.analyticsEngine = new AnalyticsExecutionEngine(planExecutor);
    this.pplParser = new PPLQueryParser(settings);
  }

  /**
   * Check if the query targets an analytics engine index (e.g., Parquet-backed). Uses {@link
   * PPLQueryParser} to parse the query and extract the index name from the AST.
   */
  public boolean isAnalyticsIndex(String query) {
    if (query == null || query.isEmpty()) {
      return false;
    }
    try {
      String indexName = extractIndexName(query);
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

  /** Extract the source index name by parsing the PPL AST and finding the Relation node. */
  private String extractIndexName(String query) {
    UnresolvedPlan plan = pplParser.parse(query);
    Relation relation = findRelation(plan);
    return relation != null ? relation.getTableQualifiedName().toString() : null;
  }

  /** Walk the AST to find the Relation (table scan) node. */
  private static Relation findRelation(UnresolvedPlan plan) {
    if (plan instanceof Relation) {
      return (Relation) plan;
    }
    for (var child : plan.getChild()) {
      if (child instanceof UnresolvedPlan unresolvedChild) {
        Relation found = findRelation(unresolvedChild);
        if (found != null) {
          return found;
        }
      }
    }
    return null;
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
    try {
      AbstractSchema schema = StubSchemaProvider.buildSchema();

      try (UnifiedQueryContext context =
          UnifiedQueryContext.builder()
              .language(queryType)
              .catalog(SCHEMA_NAME, schema)
              .defaultNamespace(SCHEMA_NAME)
              .profiling(pplRequest.profile())
              .build()) {

        UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
        RelNode plan = planner.plan(query);

        CalcitePlanContext planContext = context.getPlanContext();
        plan = addQuerySizeLimit(plan, planContext);

        analyticsEngine.execute(plan, planContext, createQueryListener(queryType, listener));
      }
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private void doExplain(
      String query,
      QueryType queryType,
      PPLQueryRequest pplRequest,
      ResponseListener<ExplainResponse> listener) {
    try {
      AbstractSchema schema = StubSchemaProvider.buildSchema();

      try (UnifiedQueryContext context =
          UnifiedQueryContext.builder()
              .language(queryType)
              .catalog(SCHEMA_NAME, schema)
              .defaultNamespace(SCHEMA_NAME)
              .profiling(pplRequest.profile())
              .build()) {

        UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
        RelNode plan = planner.plan(query);

        CalcitePlanContext planContext = context.getPlanContext();
        plan = addQuerySizeLimit(plan, planContext);

        analyticsEngine.explain(plan, pplRequest.mode(), planContext, listener);
      }
    } catch (Exception e) {
      listener.onFailure(e);
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
    ResponseFormatter<QueryResult> formatter = new JdbcResponseFormatter(PRETTY);
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
