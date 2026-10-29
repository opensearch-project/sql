/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.rest.BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX;
import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse.normalizeLf;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.time.Clock;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.ContextPreservingActionListener;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Guice;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.NotifyOnceListener;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.AnalyzeResponse;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.job.OpenSearchQueryJobService;
import org.opensearch.sql.job.OpenSearchSecurityAdapter;
import org.opensearch.sql.job.QueryJob;
import org.opensearch.sql.job.QueryJobService;
import org.opensearch.sql.job.QueryRunner;
import org.opensearch.sql.job.SecurityAdapter;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.monitor.profile.ProfileScope;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.opensearch.client.RemoteClusterServiceProvider;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.tracing.TracingPhaseListener;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.plugin.config.EngineExtensionsHolder;
import org.opensearch.sql.plugin.config.OpenSearchPluginModule;
import org.opensearch.sql.plugin.rest.AnalyticsEngineFormatSupport;
import org.opensearch.sql.plugin.rest.AnalyticsExecutorHolder;
import org.opensearch.sql.plugin.rest.RestUnifiedQueryAction;
import org.opensearch.sql.ppl.PPLQueryRunner;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.QueryInsightsMetadata;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.ExplainResponseJsonFormatter;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.VisualizationResponseFormatter;
import org.opensearch.sql.protocol.response.format.YamlResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.telemetry.tracing.listener.TraceableActionListener;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/** Send PPL query transport action. */
public class TransportPPLQueryAction
    extends HandledTransportAction<ActionRequest, TransportPPLQueryResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportPPLQueryAction.class);

  private final Injector injector;

  private final Tracer tracer;

  private final Supplier<Boolean> pplEnabled;

  /** Null when analytics-engine plugin is absent; set via {@link #setQueryPlanExecutor}. */
  private volatile RestUnifiedQueryAction unifiedQueryHandler;

  private final NodeClient clientRef;
  private final ClusterService clusterServiceRef;
  private final org.opensearch.sql.common.setting.Settings pluginSettingsRef;
  private final TransportService transportServiceRef;
  private final QueryJobService queryJobService;
  private final SecurityAdapter securityAdapter;
  private final TaskResourceTrackingService taskResourceTrackingService;

  @Inject
  public TransportPPLQueryAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      DataSourceServiceImpl dataSourceService,
      org.opensearch.common.settings.Settings clusterSettings,
      EngineExtensionsHolder extensionsHolder,
      Tracer tracer,
      OpenSearchQueryJobService queryJobService,
      OpenSearchSecurityAdapter securityAdapter,
      RemoteClusterServiceProvider remoteClusters,
      TaskResourceTrackingService taskResourceTrackingService) {
    super(PPLQueryAction.NAME, transportService, actionFilters, TransportPPLQueryRequest::new);
    this.clientRef = client;
    this.clusterServiceRef = clusterService;
    this.transportServiceRef = transportService;
    this.queryJobService = queryJobService;
    this.securityAdapter = securityAdapter;
    this.taskResourceTrackingService = taskResourceTrackingService;
    // The plugin's components are created before any TransportService exists; fill it here.
    remoteClusters.set(transportService.getRemoteClusterService());

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(new OpenSearchPluginModule(extensionsHolder.engines(), tracer));
    org.opensearch.sql.common.setting.Settings pluginSettings =
        new OpenSearchSettings(clusterService.getClusterSettings());
    this.pluginSettingsRef = pluginSettings;
    modules.add(
        b -> {
          b.bind(NodeClient.class).toInstance(client);
          b.bind(RemoteClusterServiceProvider.class).toInstance(remoteClusters);
          b.bind(org.opensearch.sql.common.setting.Settings.class).toInstance(pluginSettings);
          b.bind(DataSourceService.class).toInstance(dataSourceService);
          b.bind(ClusterService.class).toInstance(clusterService);
          b.bind(org.opensearch.common.util.concurrent.ThreadContext.class)
              .toInstance(client.threadPool().getThreadContext());
        });
    this.injector = Guice.createInjector(modules);
    this.tracer = tracer;
    ProfileScope.installListener(new TracingPhaseListener(tracer));
    this.pplEnabled =
        () ->
            MULTI_ALLOW_EXPLICIT_INDEX.get(clusterSettings)
                && (Boolean)
                    injector
                        .getInstance(org.opensearch.sql.common.setting.Settings.class)
                        .getSettingValue(Settings.Key.PPL_ENABLED);
  }

  /** Invoked by Guice iff analytics-engine bound {@code QueryPlanExecutor}. */
  @Inject(optional = true)
  public void setQueryPlanExecutor(
      QueryPlanExecutor<RelNode, Iterable<Object[]>> queryPlanExecutor) {
    AnalyticsExecutorHolder.set(queryPlanExecutor);
    // Build the SQL router once both bridges are populated (engine context might arrive
    // first or last depending on Guice ordering). buildUnifiedQueryHandler is idempotent.
    buildUnifiedQueryHandlerIfReady();
  }

  /** Invoked by Guice iff analytics-engine bound {@code EngineContextProvider}. */
  @Inject(optional = true)
  public void setEngineContext(org.opensearch.analytics.EngineContextProvider contextProvider) {
    org.opensearch.sql.plugin.rest.EngineContextProviderHolder.set(contextProvider);
    buildUnifiedQueryHandlerIfReady();
  }

  private void buildUnifiedQueryHandlerIfReady() {
    QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = AnalyticsExecutorHolder.get();
    org.opensearch.analytics.EngineContextProvider contextProvider =
        org.opensearch.sql.plugin.rest.EngineContextProviderHolder.get();
    if (executor != null && contextProvider != null) {
      this.unifiedQueryHandler =
          new RestUnifiedQueryAction(
              clientRef,
              clusterServiceRef,
              executor,
              contextProvider,
              pluginSettingsRef,
              new org.opensearch.sql.opensearch.executor.ThreadPoolExecutionDispatcher(
                  clientRef.threadPool(), pluginSettingsRef));
    }
  }

  /**
   * Stamp the parent marker so child DSL searches carry it back to this PPL query. The marker is
   * stored on the task only once it is actually in the thread context, and the record later sends
   * whatever was stored: if stamping fails the record goes out with no marker rather than one that
   * no child search carries.
   */
  private void stampQueryInsightsParentHeader(PPLQueryTask pplQueryTask) {
    try {
      ThreadContext threadContext = clientRef.threadPool().getThreadContext();
      String value =
          QueryInsightsMarker.value(
              "PPL", clusterServiceRef.localNode().getId(), pplQueryTask.getId());
      // An outer marker is kept rather than replaced: putHeader rejects a duplicate key, and
      // clearing the context first would drop the authenticated-user headers the child searches
      // need. The child searches then belong to that outer query, so this one does not report a
      // record -- see shouldReportToQueryInsights.
      if (threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER) == null) {
        threadContext.putHeader(QueryInsightsMarker.PARENT_HEADER, value);
        pplQueryTask.setQueryInsightsParentMarker(value);
      } else {
        pplQueryTask.setQueryInsightsNested(true);
      }
      // Capture the user here; the report listener runs on a thread without the transient.
      pplQueryTask.setQueryInsightsUserInfo(
          threadContext.getTransient(SECURITY_USER_INFO_TRANSIENT));
    } catch (Exception e) {
      LOG.warn("Failed to stamp Query Insights parent header for query association", e);
    }
  }

  /** Security plugin's authenticated-user transient ({@code name|backendroles|roles|...}). */
  private static final String SECURITY_USER_INFO_TRANSIENT = "_opendistro_security_user_info";

  /**
   * Toggle for recording PPL queries into Query Insights. Registered by the Query Insights plugin
   * ({@code QueryInsightsSettings.TOP_N_PPL_QUERIES_ENABLED}, default false).
   */
  static final String QUERY_INSIGHTS_PPL_ENABLED_KEY = "search.insights.top_queries.ppl.enabled";

  /** True when Query Insights is installed and the toggle is on; disabled on any failure. */
  private boolean isQueryInsightsRecordingEnabled() {
    try {
      Setting<?> setting =
          clusterServiceRef.getClusterSettings().get(QUERY_INSIGHTS_PPL_ENABLED_KEY);
      if (setting == null) {
        return false;
      }
      Object value = clusterServiceRef.getClusterSettings().get(setting);
      return Boolean.TRUE.equals(value);
    } catch (Exception e) {
      LOG.debug("Failed to evaluate Query Insights PPL recording gate; defaulting to disabled", e);
      return false;
    }
  }

  /**
   * Has the engine's thread scopes track this query's threads through core, so its CPU and memory
   * show in {@code _tasks} (and Live Queries) while it runs and in its Top N record after. Core
   * records nothing unless it registered the task, which it does only when {@code
   * task_resource_tracking.enabled} is on, the same switch that governs the child searches.
   */
  private void trackEngineThreads(PPLQueryTask pplQueryTask) {
    if (pplQueryTask != null) {
      pplQueryTask.setResourceTracking(taskResourceTrackingService);
    }
  }

  /**
   * Report the finished PPL query once the task is unregistered and every accounting scope has
   * closed, so the measured usage is final.
   */
  private void registerQueryInsightsReport(PPLQueryTask pplQueryTask) {
    try {
      boolean registered =
          pplQueryTask.addResourceTrackingCompletionListener(
              new NotifyOnceListener<>() {
                @Override
                protected void innerOnResponse(Task task) {
                  writeQueryInsightsRecord((PPLQueryTask) task);
                }

                @Override
                protected void innerOnFailure(Exception e) {}
              });
      if (registered == false) {
        LOG.debug(
            "Query Insights report listener not registered; resource tracking already complete");
      }
    } catch (Exception e) {
      LOG.warn("Failed to register Query Insights report listener", e);
    }
  }

  /**
   * Whether a finished task should produce a Top N record. Decided from what the metadata sink
   * captured rather than from the route, because the route cannot see either case: {@code explain}
   * is valid inside the query text (not just on {@code /_explain}), and a query that fails to parse
   * never reaches the sink at all, so there is no parsed query to report.
   *
   * <p>A nested query is also skipped: its child searches carry the outer query's marker, so a
   * record built from this task's own id would advertise a marker no child references, and the
   * children's cost would be counted against both records.
   */
  static boolean shouldReportToQueryInsights(PPLQueryTask pplQueryTask) {
    return pplQueryTask.getQueryInsightsQueryText() != null
        && !pplQueryTask.isQueryInsightsExplain()
        && !pplQueryTask.isQueryInsightsNested();
  }

  /** Serialize a completed PPL query and send it to Query Insights; errors are ignored. */
  private void writeQueryInsightsRecord(PPLQueryTask pplQueryTask) {
    if (!shouldReportToQueryInsights(pplQueryTask)) {
      return;
    }
    try {
      String nodeId = clusterServiceRef.localNode().getId();
      String queryText = pplQueryTask.getQueryInsightsQueryText();

      long cpuNanos = pplQueryTask.getCpuNanos();
      long memoryBytes = pplQueryTask.getAllocatedBytes();

      // Taken when the response was sent (see recordCompletion), not now: this runs once the last
      // thread scope closes, which a lookahead search still running on the background pool can
      // hold open well past the response.
      long latencyMillis = pplQueryTask.getQueryInsightsLatencyMillis();
      if (latencyMillis < 0) {
        latencyMillis = elapsedMillis(pplQueryTask);
      }

      // Start time, not report time: Query Insights stamps a search record with the request's
      // start (SearchRequest#getOrCreateAbsoluteStartMillis). Stamping completion here would put a
      // query that crosses a Top N window boundary in a later window than its own child searches,
      // and the marker roll-up would then miss them.
      long timestampMillis = pplQueryTask.getStartTime();

      // The marker stamped on child DSL tasks, so their cpu/memory rolls up into this parent; null
      // when stamping failed, which the reporter sends as an empty marker.
      String parentMarker = pplQueryTask.getQueryInsightsParentMarker();

      String userInfo = pplQueryTask.getQueryInsightsUserInfo();
      boolean failed = pplQueryTask.isQueryInsightsFailed();

      QueryInsightsReporter.report(
          transportServiceRef,
          clusterServiceRef.localNode(),
          "PPL",
          parentMarker,
          nodeId,
          queryText,
          timestampMillis,
          latencyMillis,
          cpuNanos,
          memoryBytes,
          userInfo,
          failed);
    } catch (Exception e) {
      LOG.debug("Failed to write PPL query to Query Insights", e);
    }
  }

  /**
   * {@inheritDoc} Transform the request and call super.doExecute() to support call from other
   * plugins.
   */
  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    if (!pplEnabled.get()) {
      listener.onFailure(
          new IllegalAccessException(
              "Either plugins.ppl.enabled or rest.action.multi.allow_explicit_index setting is"
                  + " false"));
      return;
    }

    TransportPPLQueryRequest transportRequest = TransportPPLQueryRequest.fromActionRequest(request);
    if (transportRequest.isGrammarRequest()) {
      // Authorization is enforced by this transport action before returning grammar metadata in
      // REST.
      listener.onResponse(new TransportPPLQueryResponse("{}"));
      return;
    }

    final PPLQueryTask pplQueryTask = task instanceof PPLQueryTask ? (PPLQueryTask) task : null;
    if (pplQueryTask != null) {
      OpenSearchQueryManager.setCancellableTask(pplQueryTask);
    }
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();

    QueryContext.addRequestId();

    // in order to use PPL service, we need to convert TransportPPLQueryRequest to PPLQueryRequest
    PPLQueryRequest transformedRequest = transportRequest.toPPLQueryRequest();
    QueryContext.setProfile(transformedRequest.profile());

    // Start root span with OTel DB semantic convention attributes
    Span rootSpan =
        tracer.startSpan(
            SpanCreationContext.client()
                .name("opensearch.query")
                .attributes(
                    Attributes.create()
                        .addAttribute("db.system.name", "opensearch")
                        .addAttribute("db.query.type", "ppl")
                        .addAttribute("db.query.id", QueryContext.getRequestId())
                        .addAttribute(
                            "db.operation.name",
                            transformedRequest.isExplainRequest() ? "EXPLAIN" : "EXECUTE")));

    // Put span in scope so ThreadContext propagation captures it
    SpanScope spanScope = tracer.withSpanInScope(rootSpan);

    // Trace wrapper: ends span in async callback, sets error on failure.
    ActionListener<TransportPPLQueryResponse> tracedListener =
        TraceableActionListener.create(listener, rootSpan, tracer);
    ActionListener<TransportPPLQueryResponse> clearingListener =
        wrapWithProfilingClear(tracedListener);
    // Read back later by writeQueryInsightsRecord, which runs after this listener completes.
    clearingListener = recordCompletion(clearingListener, pplQueryTask);

    try {
      // Route to analytics engine for non-Lucene (e.g., Parquet-backed) indices.
      if (unifiedQueryHandler != null
          && unifiedQueryHandler.isAnalyticsIndex(transformedRequest.getRequest(), QueryType.PPL)) {
        LOG.info("[{}] Routing PPL query to analytics engine", QueryContext.getRequestId());
        // Pass this PPL task so the analytics engine links its query task to it for cancellation.
        if (transformedRequest.isExplainRequest()) {
          unifiedQueryHandler.explain(
              transformedRequest.getRequest(),
              QueryType.PPL,
              transformedRequest.mode(),
              task,
              createExplainResponseListener(transformedRequest, clearingListener));
        } else {
          // Analytics route only emits JSON; reject unsupported formats (e.g. csv) with a 4xx.
          try {
            AnalyticsEngineFormatSupport.validateFormat(format(transformedRequest));
          } catch (Exception e) {
            clearingListener.onFailure(e);
            return;
          }
          unifiedQueryHandler.execute(
              transformedRequest.getRequest(),
              QueryType.PPL,
              transformedRequest.profile(),
              transformedRequest.getFetchSize(),
              task,
              clearingListener);
        }
        return;
      }

      Consumer<QueryInsightsMetadata> queryInsightsSink =
          metadata -> {
            rootSpan.addAttribute("db.query.text", metadata.anonymizedQuery());
            if (pplQueryTask != null) {
              // The record keeps the query as written, literals included, as Query Insights does
              // for a DSL search's source; the span above stays anonymized.
              pplQueryTask.setQueryInsightsQueryText(transformedRequest.getRequest());
              pplQueryTask.setQueryInsightsExplain(metadata.explain());
            }
          };
      PPLService pplService = injector.getInstance(PPLService.class);
      if (transformedRequest.isExplainRequest()) {
        pplService.explain(
            transformedRequest,
            createExplainResponseListener(transformedRequest, clearingListener),
            queryInsightsSink);
      } else if (transformedRequest.analyze()) {
        pplService.analyze(
            transformedRequest,
            createAnalyzeResponseListener(transformedRequest, clearingListener),
            queryInsightsSink);
      } else if (transformedRequest.shouldRunAsync()) {
        // Async submission answers RUNNING and the query outlives this task, so the task's
        // completion is not the query's end: a record stamped from it would carry near-zero
        // latency and reference child searches that have not run yet. Left out of Top N until the
        // job path reports for itself.
        submitAsync(task, pplService, transformedRequest, clearingListener, queryInsightsSink);
      } else if (pplQueryTask != null && isQueryInsightsRecordingEnabled()) {
        // Query Insights is set up only on this execute route: explain, analyze, and the analytics
        // engine run no search, so stamping their child searches would orphan them under a parent
        // that never reports, and reporting the query itself would put an empty record in Top N.
        final ThreadContext threadContext = clientRef.threadPool().getThreadContext();
        // Capture the caller's context before stamping, so the callback runs without our header.
        // The worker thread that completes the query carries it, and an in-process caller invoked
        // via NodeClient would otherwise inherit it in onResponse.
        ActionListener<TransportPPLQueryResponse> callerListener =
            ContextPreservingActionListener.wrapPreservingContext(clearingListener, threadContext);
        // Stamp inside a stored context so the header is dropped from this thread when doExecute
        // returns, keeping a caller that issues several PPL queries from tagging later ones (or a
        // plain DSL search) with this query's marker. Child searches still see it: the executor
        // captures the thread context when the work is scheduled, which happens inside this block.
        try (ThreadContext.StoredContext ignored = threadContext.newStoredContext(true)) {
          trackEngineThreads(pplQueryTask);
          stampQueryInsightsParentHeader(pplQueryTask);
          registerQueryInsightsReport(pplQueryTask);
          executePplQuery(pplService, transformedRequest, callerListener, queryInsightsSink);
        }
      } else {
        // Tracked without Query Insights recording too: _tasks and Live Queries read the task.
        trackEngineThreads(pplQueryTask);
        executePplQuery(pplService, transformedRequest, clearingListener, queryInsightsSink);
      }
    } catch (Exception e) {
      clearingListener.onFailure(e);
    } finally {
      spanScope.close();
      // Clear the task in case an early return/throw skipped submit() and left it on this thread.
      OpenSearchQueryManager.clearCancellableTask();
    }
  }

  private void submitAsync(
      Task task,
      PPLService pplService,
      PPLQueryRequest transformedRequest,
      ActionListener<TransportPPLQueryResponse> listener,
      Consumer<QueryInsightsMetadata> queryInsightsSink) {
    if (!(task instanceof PPLQueryTask pplQueryTask)) {
      throw new IllegalArgumentException("Async PPL queries require a cancellable PPLQueryTask");
    }
    Runnable cancelExecution = () -> pplQueryTask.cancel("async PPL query cancelled");
    QueryRunner runner =
        new PPLQueryRunner(
            pplService, transformedRequest, queryInsightsSink, Clock.systemUTC(), cancelExecution);
    ThreadContext threadContext = clientRef.threadPool().getThreadContext();
    ActionListener<TransportPPLQueryResponse> ctxListener =
        ContextPreservingActionListener.wrapPreservingContext(listener, threadContext);
    ResponseListener<ExecutionEngine.QueryResponse> responseListener =
        createListener(transformedRequest, ctxListener);
    CompletionStage<org.opensearch.sql.job.QueryResult> submission;
    try {
      submission =
          queryJobService.submit(
              runner,
              securityAdapter.current(),
              transformedRequest.effectiveWaitForCompletion(),
              transformedRequest.effectiveKeepAlive());
    } catch (RuntimeException e) {
      responseListener.onFailure(e);
      return;
    }
    submission.whenComplete(
        (result, error) -> {
          if (error != null) {
            // whenComplete on a dependent CompletionStage wraps the exception in
            // CompletionException. Unwrap so OpenSearch's status mapper sees the engine's
            // original exception type (preserves 4xx classification and PPL error context).
            Throwable cause = QueryJob.unwrap(error);
            responseListener.onFailure(
                cause instanceof Exception ex ? ex : new RuntimeException(cause));
            return;
          }
          switch (result) {
            case org.opensearch.sql.job.QueryResult.Rows rows -> {
              ExecutionEngine.QueryResponse response =
                  new ExecutionEngine.QueryResponse(rows.schema(), rows.rows(), rows.cursor());
              response.setWarnings(rows.warnings());
              responseListener.onResponse(response);
            }
            case org.opensearch.sql.job.QueryResult.Explain explain -> {
              createExplainResponseListener(transformedRequest, ctxListener)
                  .onResponse(explain.response());
            }
            case org.opensearch.sql.job.QueryResult.Running running ->
                ctxListener.onResponse(new TransportPPLQueryResponse(formatRunning(running)));
          }
        });
  }

  /**
   * Wire shape of a {@link org.opensearch.sql.job.QueryResult.Running}; must match the async-query
   * GET RUNNING body.
   */
  private static String formatRunning(org.opensearch.sql.job.QueryResult.Running running) {
    return new JSONObject()
        .put("id", running.id().encode())
        .put("status", "RUNNING")
        .put("schema", new JSONArray())
        .put("datarows", new JSONArray())
        .put("total", 0)
        .toString();
  }

  private ResponseListener<AnalyzeResponse> createAnalyzeResponseListener(
      PPLQueryRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    return new ResponseListener<AnalyzeResponse>() {
      @Override
      public void onResponse(AnalyzeResponse response) {
        JsonResponseFormatter<AnalyzeResponse> formatter =
            new JsonResponseFormatter<>(PRETTY) {
              @Override
              protected Object buildJsonObject(AnalyzeResponse response) {
                return response;
              }
            };
        listener.onResponse(
            new TransportPPLQueryResponse(formatter.format(response), formatter.contentType()));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  /**
   * TODO: need to extract an interface for both SQL and PPL action handler and move these common
   * methods to the interface. This is not easy to do now because SQL action handler is still in
   * legacy module.
   */
  private ResponseListener<ExecutionEngine.ExplainResponse> createExplainResponseListener(
      PPLQueryRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    return new ResponseListener<ExecutionEngine.ExplainResponse>() {
      @Override
      public void onResponse(ExecutionEngine.ExplainResponse response) {
        Optional<Format> isYamlFormat =
            Format.ofExplain(request.getFormat()).filter(format -> format.equals(Format.YAML));
        ResponseFormatter<ExecutionEngine.ExplainResponse> formatter;
        if (isYamlFormat.isPresent()) {
          formatter =
              new YamlResponseFormatter<>() {
                @Override
                protected Object buildYamlObject(ExecutionEngine.ExplainResponse response) {
                  return normalizeLf(response);
                }
              };
        } else {
          formatter = new ExplainResponseJsonFormatter(PRETTY);
        }
        listener.onResponse(
            new TransportPPLQueryResponse(formatter.format(response), formatter.contentType()));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  private ResponseListener<ExecutionEngine.QueryResponse> createListener(
      PPLQueryRequest pplRequest, ActionListener<TransportPPLQueryResponse> listener) {
    Format format = format(pplRequest);
    ResponseFormatter<QueryResult> formatter;
    if (format.equals(Format.CSV)) {
      formatter = new CsvResponseFormatter(pplRequest.sanitize());
    } else if (format.equals(Format.RAW)) {
      formatter = new RawResponseFormatter();
    } else if (format.equals(Format.VIZ)) {
      formatter = new VisualizationResponseFormatter(pplRequest.style());
    } else {
      formatter = new SimpleJsonResponseFormatter(JsonResponseFormatter.Style.PRETTY);
    }

    return new ResponseListener<ExecutionEngine.QueryResponse>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        String responseContent =
            formatter.format(
                new QueryResult(
                    response.getSchema(),
                    response.getResults(),
                    response.getCursor(),
                    PPL_SPEC,
                    response.getWarnings()));
        listener.onResponse(new TransportPPLQueryResponse(responseContent));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  private Format format(PPLQueryRequest pplRequest) {
    String format = pplRequest.getFormat();
    Optional<Format> optionalFormat = Format.of(format);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT, "response in %s format is not supported.", format));
    }
  }

  private void executePplQuery(
      PPLService pplService,
      PPLQueryRequest request,
      ActionListener<TransportPPLQueryResponse> listener,
      Consumer<QueryInsightsMetadata> queryInsightsSink) {
    pplService.execute(
        request,
        createListener(request, listener),
        createExplainResponseListener(request, listener),
        queryInsightsSink);
  }

  /**
   * Records on the task how the query ended and its latency as the response goes out, for the Query
   * Insights record written later. Returns {@code delegate} unchanged when there is no PPL task.
   */
  private ActionListener<TransportPPLQueryResponse> recordCompletion(
      ActionListener<TransportPPLQueryResponse> delegate, PPLQueryTask pplQueryTask) {
    if (pplQueryTask == null) {
      return delegate;
    }
    return new ActionListener<>() {
      @Override
      public void onResponse(TransportPPLQueryResponse response) {
        pplQueryTask.setQueryInsightsLatencyMillis(elapsedMillis(pplQueryTask));
        delegate.onResponse(response);
      }

      @Override
      public void onFailure(Exception e) {
        pplQueryTask.setQueryInsightsLatencyMillis(elapsedMillis(pplQueryTask));
        pplQueryTask.setQueryInsightsFailed(true);
        delegate.onFailure(e);
      }
    };
  }

  /**
   * Wall-clock (monotonic) time since the task started. The profile total is not used: it collapses
   * to ~0 across the thread that finishes the query.
   */
  private static long elapsedMillis(PPLQueryTask pplQueryTask) {
    return Math.max(0L, (System.nanoTime() - pplQueryTask.getStartTimeNanos()) / 1_000_000L);
  }

  private ActionListener<TransportPPLQueryResponse> wrapWithProfilingClear(
      ActionListener<TransportPPLQueryResponse> delegate) {
    return new ActionListener<>() {
      @Override
      public void onResponse(TransportPPLQueryResponse transportPPLQueryResponse) {
        try {
          delegate.onResponse(transportPPLQueryResponse);
        } finally {
          clearRequestScopedState();
        }
      }

      @Override
      public void onFailure(Exception e) {
        try {
          delegate.onFailure(e);
        } finally {
          clearRequestScopedState();
        }
      }
    };
  }

  /**
   * Clear the per-request state carried in {@link QueryContext}'s thread-locals. Transport threads
   * are pooled, so anything left behind is inherited by the next query to run on this thread.
   */
  private static void clearRequestScopedState() {
    QueryProfiling.clear();
  }
}
