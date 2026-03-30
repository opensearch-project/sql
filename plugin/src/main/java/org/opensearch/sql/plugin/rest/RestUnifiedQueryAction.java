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
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.executor.analytics.AnalyticsExecutionEngine;
import org.opensearch.sql.executor.analytics.QueryPlanExecutor;
import org.opensearch.sql.lang.LangSpec;
import org.opensearch.sql.monitor.profile.MetricName;
import org.opensearch.sql.plugin.rest.analytics.stub.StubSchemaProvider;
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

  public RestUnifiedQueryAction(NodeClient client, QueryPlanExecutor planExecutor) {
    this.client = client;
    this.analyticsEngine = new AnalyticsExecutionEngine(planExecutor);
  }

  /**
   * Check if the query targets an analytics engine index (e.g., Parquet-backed). Creates a {@link
   * UnifiedQueryContext} to use its parser for index name extraction, supporting both PPL and SQL.
   */
  public boolean isAnalyticsIndex(String query, QueryType queryType) {
    if (query == null || query.isEmpty()) {
      return false;
    }
    try (UnifiedQueryContext context = buildContext(queryType, false)) {
      String indexName = extractIndexName(query, context);
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

      RelNode finalPlan = plan;
      context.measure(
          MetricName.EXECUTE,
          () -> {
            analyticsEngine.execute(
                finalPlan, planContext, createQueryListener(queryType, listener));
            return null;
          });
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

      RelNode finalPlan = plan;
      context.measure(
          MetricName.EXECUTE,
          () -> {
            analyticsEngine.explain(finalPlan, pplRequest.mode(), planContext, listener);
            return null;
          });
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  private UnifiedQueryContext buildContext(QueryType queryType, boolean profiling) {
    AbstractSchema schema = StubSchemaProvider.buildSchema();
    return UnifiedQueryContext.builder()
        .language(queryType)
        .catalog(SCHEMA_NAME, schema)
        .defaultNamespace(SCHEMA_NAME)
        .profiling(profiling)
        .build();
  }

  /**
   * Extract the source index name by parsing the query and visiting the AST to find the Relation
   * node. Uses the context's parser which supports both PPL and SQL.
   */
  private static String extractIndexName(String query, UnifiedQueryContext context) {
    Object parseResult = context.getParser().parse(query);
    if (parseResult instanceof UnresolvedPlan unresolvedPlan) {
      return unresolvedPlan.accept(new IndexNameExtractor(), null);
    }
    // TODO: handle SQL SqlNode for table extraction when unified SQL is enabled
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
