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
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexSettings;
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
  private final org.opensearch.sql.common.setting.Settings pluginSettings;

  public RestUnifiedQueryAction(
      NodeClient client,
      ClusterService clusterService,
      QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor,
      org.opensearch.sql.common.setting.Settings pluginSettings) {
    this.client = client;
    this.clusterService = clusterService;
    this.analyticsEngine = new AnalyticsExecutionEngine(planExecutor);
    this.pluginSettings = pluginSettings;
  }

  /**
   * Returns true iff the target index has {@link
   * IndexSettings#PLUGGABLE_DATAFORMAT_ENABLED_SETTING} set and {@link
   * IndexSettings#PLUGGABLE_DATAFORMAT_VALUE_SETTING} is {@code "parquet"}, routing it to
   * DataFusion instead of the Calcite→DSL path.
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
      return extractIndexName(query, queryType, context)
          .map(this::stripSchemaPrefix)
          .map(this::isPluggableDataformatIndex)
          .orElse(false);
    } catch (Exception e) {
      return false;
    }
  }

  private String stripSchemaPrefix(String indexName) {
    int lastDot = indexName.lastIndexOf('.');
    return lastDot >= 0 ? indexName.substring(lastDot + 1) : indexName;
  }

  private boolean isPluggableDataformatIndex(String indexName) {
    var indexMetadata = clusterService.state().metadata().index(indexName);
    if (indexMetadata == null) {
      return false;
    }
    var settings = indexMetadata.getSettings();
    return IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(settings)
        && "parquet".equals(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.get(settings));
  }

  /** Execute a query through the unified query pipeline on the sql-worker thread pool. */
  public void execute(
      String query,
      QueryType queryType,
      boolean profiling,
      ActionListener<TransportPPLQueryResponse> listener) {
    client
        .threadPool()
        .schedule(
            withCurrentContext(
                () -> {
                  try (UnifiedQueryContext context = buildContext(queryType, profiling)) {
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

  /**
   * Explain a query through the unified query pipeline on the sql-worker thread pool. Returns
   * ExplainResponse via ResponseListener so the caller can format it.
   */
  public void explain(
      String query,
      QueryType queryType,
      ExplainMode mode,
      ResponseListener<ExplainResponse> listener) {
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
                    analyticsEngine.explain(plan, mode, planContext, listener);
                  } catch (Exception e) {
                    listener.onFailure(e);
                  }
                }),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  /**
   * Build a lightweight context for parsing only (index name extraction). Does not require cluster
   * state or catalog schema.
   */
  private UnifiedQueryContext buildParsingContext(QueryType queryType) {
    return applyClusterOverrides(UnifiedQueryContext.builder().language(queryType)).build();
  }

  private UnifiedQueryContext buildContext(QueryType queryType, boolean profiling) {
    return applyClusterOverrides(
            UnifiedQueryContext.builder()
                .language(queryType)
                .catalog(SCHEMA_NAME, OpenSearchSchemaBuilder.buildSchema(clusterService.state()))
                .defaultNamespace(SCHEMA_NAME)
                .profiling(profiling))
        .build();
  }

  /**
   * Routes operator-configured cluster overrides into the builder via the existing {@code
   * setting(String, Object)} API, keeping {@link UnifiedQueryContext} decoupled from any specific
   * {@link org.opensearch.sql.common.setting.Settings} implementation.
   *
   * <p>Currently scoped to {@code plugins.ppl.rex.max_match.limit} — required so the unified path
   * honors {@code _cluster/settings} updates for {@code rex max_match} (CalciteRexCommandIT's
   * testRexMaxMatchConfigurableLimit). Add keys here if a future PR / IT depends on cluster-side
   * fidelity for one of the other planning settings.
   */
  private UnifiedQueryContext.Builder applyClusterOverrides(UnifiedQueryContext.Builder builder) {
    Object rexLimit =
        pluginSettings.getSettingValue(
            org.opensearch.sql.common.setting.Settings.Key.PPL_REX_MAX_MATCH_LIMIT);
    if (rexLimit != null) {
      builder.setting(
          org.opensearch.sql.common.setting.Settings.Key.PPL_REX_MAX_MATCH_LIMIT.getKeyValue(),
          rexLimit);
    }
    return builder;
  }

  /**
   * Extract the source index name by parsing the query and visiting the AST to find the Relation
   * node. Uses the context's parser which supports both PPL and SQL.
   */
  private static Optional<String> extractIndexName(
      String query, QueryType queryType, UnifiedQueryContext context) {
    if (queryType == QueryType.PPL) {
      UnresolvedPlan unresolvedPlan = (UnresolvedPlan) context.getParser().parse(query);
      return Optional.ofNullable(unresolvedPlan.accept(new IndexNameExtractor(), null));
    }
    SqlNode sqlNode = (SqlNode) context.getParser().parse(query);
    return Optional.ofNullable(extractTableNameFromSqlNode(sqlNode));
  }

  /** AST visitor that extracts the source index name from a Relation node (PPL path). */
  private static class IndexNameExtractor extends AbstractNodeVisitor<String, Void> {
    @Override
    public String visitRelation(Relation node, Void context) {
      return node.getTableQualifiedName().toString();
    }
  }

  /** SqlNode visitor that extracts the source table name from a SQL parse tree. */
  private static class SqlTableNameExtractor extends SqlBasicVisitor<String> {
    @Override
    public String visit(SqlCall call) {
      if (call instanceof SqlSelect select) {
        return select.getFrom().accept(this);
      }
      if (call instanceof SqlJoin join) {
        return join.getLeft().accept(this);
      }
      return null;
    }

    @Override
    public String visit(SqlIdentifier id) {
      return id.toString();
    }
  }

  private static String extractTableNameFromSqlNode(SqlNode sqlNode) {
    return sqlNode.accept(new SqlTableNameExtractor());
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
