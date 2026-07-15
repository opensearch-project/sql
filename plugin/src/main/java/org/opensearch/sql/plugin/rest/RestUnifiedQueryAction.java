/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.exec.profile.QueryProfile;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
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
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.utils.SystemIndexUtils;
import org.opensearch.tasks.Task;
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
      // Analytics engine can't serve system catalog; SHOW/DESCRIBE fall back to default pipeline
      try (UnifiedQueryContext context = buildParsingContext(queryType)) {
        boolean systemCatalog =
            extractIndexName(query, queryType, context)
                .map(RestUnifiedQueryAction::isSystemCatalog)
                .orElse(false);
        return !systemCatalog;
      } catch (Exception e) {
        // Check legacy-syntax SHOW/DESCRIBE; otherwise let AE handle and surface the error.
        return !isLegacySystemCatalogQuery(query);
      }
    }
    try (UnifiedQueryContext context = buildParsingContext(queryType)) {
      return extractIndexName(query, queryType, context)
          .map(this::stripSchemaPrefix)
          .map(this::allIndicesArePluggableDataformat)
          .orElse(false);
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * Checks if all indices in a (possibly comma-separated) index expression are pluggable
   * dataformat. For multi-index queries (source=idx1,idx2), each index is checked independently.
   * Returns true only if every index is composite — mixed or all-lucene returns false.
   */
  private boolean allIndicesArePluggableDataformat(String indexExpression) {
    String[] indices = indexExpression.split(",");
    if (indices.length == 0) {
      return false;
    }
    for (String idx : indices) {
      String trimmed = idx.trim();
      if (trimmed.isEmpty()) {
        continue;
      }
      if (!isPluggableDataformatIndex(trimmed)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isSystemCatalog(String name) {
    return SystemIndexUtils.isSystemIndex(name)
        || SystemIndexUtils.DATASOURCES_TABLE_NAME.equals(name);
  }

  private static boolean isLegacySystemCatalogQuery(String query) {
    String trimmed = query.trim();
    return Strings.CI.startsWith(trimmed, "SHOW ") || Strings.CI.startsWith(trimmed, "DESCRIBE ");
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

  /** Execute with no parent task (SQL path): the analytics query runs detached, not cancellable. */
  public void execute(
      String query,
      QueryType queryType,
      boolean profiling,
      ActionListener<TransportPPLQueryResponse> listener) {
    doExecute(query, queryType, profiling, 0, null, listener);
  }

  /** Execute linked to {@code parentTask} so a front-end cancel propagates into the engine. */
  public void execute(
      String query,
      QueryType queryType,
      boolean profiling,
      int fetchSize,
      Task parentTask,
      ActionListener<TransportPPLQueryResponse> listener) {
    assert parentTask != null : "parentTask required for cancellation propagation";
    doExecute(query, queryType, profiling, fetchSize, parentTask, listener);
  }

  private void doExecute(
      String query,
      QueryType queryType,
      boolean profiling,
      int fetchSize,
      Task parentTask,
      ActionListener<TransportPPLQueryResponse> listener) {
    client
        .threadPool()
        .schedule(
            withCurrentContext(
                () -> {
                  // Ask the engine for a per-query context — it binds the snapshot
                  // (cluster state + schema built from it) and returns the pair, so the
                  // schema we plan against and the state the executor uses are the same view.
                  // Carry the front-end task so cancellation propagates into the engine.
                  QueryRequestContext queryCtx =
                      withParentTask(contextProvider.getContext(), parentTask);

                  UnifiedQueryContext context = buildContext(queryType, profiling, queryCtx);
                  ProfileContext profileCtx = QueryProfiling.current();
                  ActionListener<TransportPPLQueryResponse> closingListener =
                      wrapWithContextClose(context, listener);
                  try {
                    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
                    RelNode plan = planner.plan(query);
                    CalcitePlanContext planContext = context.getPlanContext();
                    // PPL fetch_size caps the response to N rows (no cursor) — the V2 path attaches
                    // a `head N` in AstStatementBuilder; the unified path parses only the query
                    // string, so apply the equivalent top-level limit here before the system cap.
                    plan = addFetchSizeLimit(plan, planContext, fetchSize);
                    plan = addQuerySizeLimit(plan, planContext);
                    if (profiling) {
                      analyticsEngine.executeWithProfile(
                          plan,
                          planContext,
                          queryCtx,
                          createQueryListener(queryType, profileCtx, closingListener));
                    } else {
                      analyticsEngine.execute(
                          plan,
                          planContext,
                          queryCtx,
                          createQueryListener(queryType, profileCtx, closingListener));
                    }
                  } catch (Exception e) {
                    closingListener.onFailure(e);
                  } finally {
                    // visitTimewrap (run inside planner.plan) sets timewrap thread-locals on this
                    // worker thread. execute()/executeWithProfile() capture-and-clear them on the
                    // happy path, but a planning exception bypasses that — clear here so the
                    // signals never leak onto the next query reusing this pooled thread.
                    CalcitePlanContext.clearTimewrapSignals();
                  }
                }),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  /** Explain with no parent task (SQL path). */
  public void explain(
      String query,
      QueryType queryType,
      ExplainMode mode,
      ResponseListener<ExplainResponse> listener) {
    doExplain(query, queryType, mode, null, listener);
  }

  /** Explain linked to {@code parentTask} so a front-end cancel propagates into the engine. */
  public void explain(
      String query,
      QueryType queryType,
      ExplainMode mode,
      Task parentTask,
      ResponseListener<ExplainResponse> listener) {
    assert parentTask != null : "parentTask required for cancellation propagation";
    doExplain(query, queryType, mode, parentTask, listener);
  }

  private void doExplain(
      String query,
      QueryType queryType,
      ExplainMode mode,
      Task parentTask,
      ResponseListener<ExplainResponse> listener) {
    client
        .threadPool()
        .schedule(
            withCurrentContext(
                () -> {
                  QueryRequestContext queryCtx =
                      withParentTask(contextProvider.getContext(), parentTask);
                  try (UnifiedQueryContext context = buildContext(queryType, false, queryCtx)) {
                    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
                    RelNode plan = planner.plan(query);
                    CalcitePlanContext planContext = context.getPlanContext();
                    plan = addQuerySizeLimit(plan, planContext);
                    analyticsEngine.explain(plan, mode, planContext, listener);
                  } catch (Exception e) {
                    listener.onFailure(e);
                  } finally {
                    // explain plans a timewrap query (visitTimewrap sets thread-locals) but never
                    // executes, so nothing captures-and-clears them — clear here to avoid leaking
                    // onto the next query on this pooled thread.
                    CalcitePlanContext.clearTimewrapSignals();
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
      QueryType queryType, boolean profiling, QueryRequestContext queryCtx) {
    return applyClusterOverrides(
            UnifiedQueryContext.builder()
                .language(queryType)
                // Schema captured by queryCtx — same cluster state the executor will use.
                .catalog(SCHEMA_NAME, queryCtx.schema())
                .defaultNamespace(SCHEMA_NAME)
                .profiling(profiling))
        .build();
  }

  /** Returns {@code ctx} carrying {@code parentTask}, or unchanged when there is none. */
  private static QueryRequestContext withParentTask(QueryRequestContext ctx, Task parentTask) {
    if (parentTask == null) {
      return ctx;
    }
    return new QueryRequestContext(ctx.clusterState(), ctx.schema(), ctx.querySource(), parentTask);
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
    forwardClusterSetting(
        builder, org.opensearch.sql.common.setting.Settings.Key.MAX_EXPRESSION_DEPTH);
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

  /**
   * Cap the result to {@code fetchSize} rows when {@code fetchSize > 0}, mirroring PPL's {@code
   * fetch_size} (which the V2 path lowers to a top-level {@code head N} in AstStatementBuilder).
   * {@code fetchSize <= 0} means "use system default", so no limit is added. Uses the same {@code
   * relBuilder.limit} primitive that {@code head} lowers to, so the analytics backend sees an
   * ordinary fetch limit.
   */
  private static RelNode addFetchSizeLimit(
      RelNode plan, CalcitePlanContext context, int fetchSize) {
    if (fetchSize <= 0) {
      return plan;
    }
    return context.relBuilder.push(plan).limit(0, fetchSize).build();
  }

  private ResponseListener<QueryResponse> createQueryListener(
      QueryType queryType,
      ProfileContext profileCtx,
      ActionListener<TransportPPLQueryResponse> transportListener) {
    ResponseFormatter<QueryResult> formatter = new SimpleJsonResponseFormatter(PRETTY);
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        LangSpec langSpec = queryType == QueryType.PPL ? PPL_SPEC : LangSpec.SQL_SPEC;

        // Set the engine profile as the plan so the formatter serializes it in one pass.
        if (response.getProfile() != null) {
          profileCtx.setEnginePlan(toJsonElement(response.getProfile()));
        }

        String result =
            QueryProfiling.withCurrentContext(
                profileCtx,
                () ->
                    formatter.format(
                        new QueryResult(
                            response.getSchema(),
                            response.getResults(),
                            response.getCursor(),
                            langSpec)));
        if (response.getError() != null) {
          result = appendError(result, response.getError());
        }
        transportListener.onResponse(new TransportPPLQueryResponse(result));
      }

      @Override
      public void onFailure(Exception e) {
        transportListener.onFailure(e);
      }
    };
  }

  private static JsonElement toJsonElement(QueryProfile profile) {
    try {
      XContentBuilder builder = XContentFactory.jsonBuilder();
      profile.toXContent(builder, ToXContent.EMPTY_PARAMS);
      return JsonParser.parseString(builder.toString());
    } catch (Exception e) {
      return null;
    }
  }

  private static String appendError(String json, Throwable error) {
    if (!json.endsWith("}")) {
      return json;
    }
    JsonObject err = new JsonObject();
    err.addProperty("type", error.getClass().getSimpleName());
    err.addProperty("reason", error.getMessage() != null ? error.getMessage() : "");
    return json.substring(0, json.length() - 1) + ",\"error\":" + err + "}";
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
