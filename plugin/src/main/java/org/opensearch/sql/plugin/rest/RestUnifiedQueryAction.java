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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.IndicesService;
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
  private final org.opensearch.analytics.EngineContextProvider contextProvider;
  private final org.opensearch.sql.common.setting.Settings pluginSettings;

  public RestUnifiedQueryAction(
      NodeClient client,
      ClusterService clusterService,
      QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor,
      org.opensearch.analytics.EngineContextProvider contextProvider,
      org.opensearch.sql.common.setting.Settings pluginSettings) {
    this.client = client;
    this.clusterService = clusterService;
    this.analyticsEngine = new AnalyticsExecutionEngine(planExecutor);
    this.contextProvider = contextProvider;
    this.pluginSettings = pluginSettings;
  }

  /**
   * Returns true iff the target index has {@link
   * IndexSettings#PLUGGABLE_DATAFORMAT_ENABLED_SETTING} set and {@link
   * IndexSettings#PLUGGABLE_DATAFORMAT_VALUE_SETTING} is {@code "composite"}, routing it to
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
    // Cluster-level opt-in: when `cluster.pluggable.dataformat="composite"`, new indices
    // inherit `index.pluggable.dataformat="composite"` at creation (see
    // MetadataCreateIndexService), so every queryable target is analytics-eligible. Skip
    // the per-index lookup — it doesn't work for aliases, wildcards, comma-lists, or data
    // streams (Metadata#index() only resolves concrete names).
    if ("composite"
        .equals(
            IndicesService.CLUSTER_PLUGGABLE_DATAFORMAT_VALUE_SETTING.get(
                clusterService.getSettings()))) {
      return true;
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
        && "composite".equals(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.get(settings));
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
                  // Ask the engine for a per-query context — it binds the snapshot
                  // (cluster state + schema built from it) and returns the pair, so the
                  // schema we plan against and the state the executor uses are the same view.
                  org.opensearch.analytics.QueryRequestContext queryCtx =
                      contextProvider.getContext();
                  // Disable SQL-layer phase profiling when analytics engine profiling is active.
                  // Our QueryProfile (stages, tasks, timing) is strictly more detailed and replaces
                  // it.
                  UnifiedQueryContext context = buildContext(queryType, false, queryCtx);
                  ActionListener<TransportPPLQueryResponse> closingListener =
                      wrapWithContextClose(context, listener);
                  try {
                    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
                    RelNode plan = planner.plan(query);
                    CalcitePlanContext planContext = context.getPlanContext();
                    plan = addQuerySizeLimit(plan, planContext);
                    if (profiling) {
                      analyticsEngine.executeWithProfile(
                          plan,
                          planContext,
                          queryCtx,
                          createQueryListener(queryType, closingListener));
                    } else {
                      analyticsEngine.execute(
                          plan,
                          planContext,
                          queryCtx,
                          createQueryListener(queryType, closingListener));
                    }
                  } catch (Exception e) {
                    closingListener.onFailure(e);
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
                  org.opensearch.analytics.QueryRequestContext queryCtx =
                      contextProvider.getContext();
                  try (UnifiedQueryContext context = buildContext(queryType, false, queryCtx)) {
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

  private UnifiedQueryContext buildContext(
      QueryType queryType,
      boolean profiling,
      org.opensearch.analytics.QueryRequestContext queryCtx) {
    return applyClusterOverrides(
            UnifiedQueryContext.builder()
                .language(queryType)
                // Schema captured by queryCtx — same cluster state the executor will use.
                .catalog(SCHEMA_NAME, queryCtx.schema())
                .defaultNamespace(SCHEMA_NAME)
                .profiling(profiling))
        .build();
  }

  /**
   * Routes operator-configured cluster overrides into the builder via the existing {@code
   * setting(String, Object)} API, keeping {@link UnifiedQueryContext} decoupled from any specific
   * {@link org.opensearch.sql.common.setting.Settings} implementation.
   *
   * <p>Add keys here if a future PR / IT depends on cluster-side fidelity for one of the other
   * planning settings.
   */
  private UnifiedQueryContext.Builder applyClusterOverrides(UnifiedQueryContext.Builder builder) {
    forwardClusterSetting(
        builder, org.opensearch.sql.common.setting.Settings.Key.PPL_REX_MAX_MATCH_LIMIT);
    forwardClusterSetting(
        builder, org.opensearch.sql.common.setting.Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED);
    return builder;
  }

  private void forwardClusterSetting(
      UnifiedQueryContext.Builder builder, org.opensearch.sql.common.setting.Settings.Key key) {
    Object value = pluginSettings.getSettingValue(key);
    if (value != null) {
      builder.setting(key.getKeyValue(), value);
    }
  }

  /**
   * Extract the source index name by parsing the query and visiting the AST to find the Relation
   * node. Uses the context's parser which supports both PPL and SQL.
   */
  private static Optional<String> extractIndexName(
      String query, QueryType queryType, UnifiedQueryContext context) {
    UnresolvedPlan unresolvedPlan = (UnresolvedPlan) context.getParser().parse(query);
    return Optional.ofNullable(unresolvedPlan.accept(new IndexNameExtractor(), null));
  }

  /** AST visitor that extracts the source index name from a Relation node (PPL path). */
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
        if (response.getProfile() != null) {
          // Append profile and error (if any) to the JSON response
          result = appendProfileToJson(result, response.getProfile(), response.getError());
        }
        transportListener.onResponse(new TransportPPLQueryResponse(result));
      }

      @Override
      public void onFailure(Exception e) {
        transportListener.onFailure(e);
      }
    };
  }

  private static String appendProfileToJson(String json, QueryProfile profile, Throwable error) {
    try {
      StringBuilder extra = new StringBuilder();
      // Append profile
      org.opensearch.core.xcontent.XContentBuilder builder =
          org.opensearch.common.xcontent.XContentFactory.jsonBuilder();
      profile.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
      extra.append(",\"profile\":").append(builder.toString());
      // Append error if query partially failed
      if (error != null) {
        extra
            .append(",\"error\":{\"type\":\"")
            .append(error.getClass().getSimpleName())
            .append("\",\"reason\":\"")
            .append(error.getMessage() != null ? error.getMessage().replace("\"", "\\\"") : "")
            .append("\"}");
      }
      if (json.endsWith("}")) {
        return json.substring(0, json.length() - 1) + extra + "}";
      }
      return json;
    } catch (Exception e) {
      return json;
    }
  }

  private static Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }

  private static ActionListener<TransportPPLQueryResponse> wrapWithContextClose(
      UnifiedQueryContext context, ActionListener<TransportPPLQueryResponse> delegate) {
    return ActionListener.runAfter(
        delegate,
        () -> {
          try {
            context.close();
          } catch (Exception e) {
            LOG.warn("Failed to close query context", e);
          }
        });
  }
}
