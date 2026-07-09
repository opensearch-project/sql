/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.rest.BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX;
import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse.normalizeLf;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.Locale;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Injector;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.plugin.config.EngineExtensionsHolder;
import org.opensearch.sql.plugin.rest.AnalyticsExecutorHolder;
import org.opensearch.sql.plugin.rest.RestUnifiedQueryAction;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.VisualizationResponseFormatter;
import org.opensearch.sql.protocol.response.format.YamlResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/** Send PPL query transport action. */
public class TransportPPLQueryAction
    extends HandledTransportAction<ActionRequest, TransportPPLQueryResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportPPLQueryAction.class);

  private final Injector injector;

  private final Supplier<Boolean> pplEnabled;

  /** Null when analytics-engine plugin is absent; set via {@link #setQueryPlanExecutor}. */
  private volatile RestUnifiedQueryAction unifiedQueryHandler;

  private final NodeClient clientRef;
  private final ClusterService clusterServiceRef;
  private final org.opensearch.sql.common.setting.Settings pluginSettingsRef;

  @Inject
  public TransportPPLQueryAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      DataSourceServiceImpl dataSourceService,
      org.opensearch.common.settings.Settings clusterSettings,
      EngineExtensionsHolder extensionsHolder) {
    super(PPLQueryAction.NAME, transportService, actionFilters, TransportPPLQueryRequest::new);
    this.clientRef = client;
    this.clusterServiceRef = clusterService;
    this.injector =
        org.opensearch.sql.plugin.config.PplInjectorBuilder.build(
            client, clusterService, dataSourceService, extensionsHolder);
    this.pluginSettingsRef = injector.getInstance(org.opensearch.sql.common.setting.Settings.class);
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
              clientRef, clusterServiceRef, executor, contextProvider, pluginSettingsRef);
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

    if (task instanceof PPLQueryTask pplQueryTask) {
      OpenSearchQueryManager.setCancellableTask(pplQueryTask);
    }
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();

    QueryContext.addRequestId();

    // in order to use PPL service, we need to convert TransportPPLQueryRequest to PPLQueryRequest
    PPLQueryRequest transformedRequest = transportRequest.toPPLQueryRequest();
    QueryContext.setProfile(transformedRequest.profile());
    ActionListener<TransportPPLQueryResponse> clearingListener = wrapWithProfilingClear(listener);

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
        unifiedQueryHandler.execute(
            transformedRequest.getRequest(),
            QueryType.PPL,
            transformedRequest.profile(),
            task,
            clearingListener);
      }
      return;
    }

    PPLService pplService = injector.getInstance(PPLService.class);

    // Async collect: a terminal, non-testmode `| collect index=foo` is detached to a background
    // materialization task (the write runs there). The foreground returns a capped read-only
    // preview of the upstream rows plus the task id; status/cancel via the _tasks API.
    if (!transformedRequest.isExplainRequest()) {
      java.util.Optional<PPLService.CollectRoute> collectRoute =
          pplService.routeTerminalCollect(transformedRequest);
      if (collectRoute.isPresent()) {
        submitAsyncCollect(pplService, transformedRequest, collectRoute.get(), clearingListener);
        return;
      }
    }

    if (transformedRequest.isExplainRequest()) {
      pplService.explain(
          transformedRequest, createExplainResponseListener(transformedRequest, clearingListener));
    } else {
      pplService.execute(
          transformedRequest,
          createListener(transformedRequest, clearingListener),
          createExplainResponseListener(transformedRequest, clearingListener));
    }
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
          formatter =
              new JsonResponseFormatter<>(PRETTY) {
                @Override
                protected Object buildJsonObject(ExecutionEngine.ExplainResponse response) {
                  return response;
                }
              };
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

  private void submitAsyncCollect(
      PPLService pplService,
      PPLQueryRequest request,
      PPLService.CollectRoute route,
      ActionListener<TransportPPLQueryResponse> listener) {
    String destIndex = route.getDestIndex();
    // Plan-time safety refusals, synchronous and before detaching the background write. Shared with
    // the collect operator (CalciteRelNodeVisitor.visitCollect) via CollectValidation, which runs
    // only inside the background task and so cannot surface these to the caller.
    if (destIndex.startsWith(".")) {
      listener.onFailure(org.opensearch.sql.calcite.CollectValidation.dotIndexRefused(destIndex));
      return;
    }
    if (!clusterServiceRef.state().routingTable().hasIndex(destIndex)) {
      listener.onFailure(
          org.opensearch.sql.calcite.CollectValidation.destinationDoesNotExist(destIndex));
      return;
    }
    org.opensearch.tasks.Task task =
        clientRef.executeLocally(
            org.opensearch.sql.plugin.transport.collect.CollectMaterializeAction.INSTANCE,
            new org.opensearch.sql.plugin.transport.collect.CollectMaterializeRequest(
                request.getRequest(), destIndex),
            org.opensearch.tasks.LoggingTaskListener.instance());
    String taskId = clientRef.getLocalNodeId() + ":" + task.getId();
    // Run the AST-built preview (collect stripped, capped) and stamp the background task id onto
    // the
    // response envelope. The original request supplies the response format.
    ResponseListener<ExecutionEngine.QueryResponse> formatting = createListener(request, listener);
    pplService.executeStatement(
        route.getPreviewStatement(),
        new ResponseListener<ExecutionEngine.QueryResponse>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            response.setCollectTaskId(taskId);
            formatting.onResponse(response);
          }

          @Override
          public void onFailure(Exception e) {
            formatting.onFailure(e);
          }
        },
        createExplainResponseListener(request, listener));
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
                    response.getCollectTaskId()));
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

  private ActionListener<TransportPPLQueryResponse> wrapWithProfilingClear(
      ActionListener<TransportPPLQueryResponse> delegate) {
    return new ActionListener<>() {
      @Override
      public void onResponse(TransportPPLQueryResponse transportPPLQueryResponse) {
        try {
          delegate.onResponse(transportPPLQueryResponse);
        } finally {
          QueryProfiling.clear();
        }
      }

      @Override
      public void onFailure(Exception e) {
        try {
          delegate.onFailure(e);
        } finally {
          QueryProfiling.clear();
        }
      }
    };
  }
}
