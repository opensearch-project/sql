/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.commons.ppl.format.JsonResponseFormatter.Style.PRETTY;
import static org.opensearch.rest.BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;

import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.commons.ppl.action.PPLQueryAction;
import org.opensearch.commons.ppl.action.TransportPPLQueryRequest;
import org.opensearch.commons.ppl.action.TransportPPLQueryResponse;
import org.opensearch.commons.ppl.format.Format;
import org.opensearch.commons.ppl.format.JsonResponseFormatter;
import org.opensearch.commons.ppl.format.ResponseFormatter;
import org.opensearch.commons.ppl.util.PPLQueryRequest;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.plugin.config.OpenSearchPluginModule;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.VisualizationResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/** Send PPL query transport action. */
public class TransportPPLQueryAction
    extends HandledTransportAction<ActionRequest, TransportPPLQueryResponse> {

  private final Injector injector;

  private final Supplier<Boolean> pplEnabled;

  /** Constructor of TransportPPLQueryAction. */
  @Inject
  public TransportPPLQueryAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      DataSourceServiceImpl dataSourceService,
      org.opensearch.common.settings.Settings clusterSettings) {
    super(PPLQueryAction.NAME, transportService, actionFilters, TransportPPLQueryRequest::new);

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(new OpenSearchPluginModule());
    modules.add(
        b -> {
          b.bind(NodeClient.class).toInstance(client);
          b.bind(org.opensearch.sql.common.setting.Settings.class)
              .toInstance(new OpenSearchSettings(clusterService.getClusterSettings()));
          b.bind(DataSourceService.class).toInstance(dataSourceService);
        });
    this.injector = modules.createInjector();
    this.pplEnabled =
        () ->
            MULTI_ALLOW_EXPLICIT_INDEX.get(clusterSettings)
                && (Boolean)
                    injector
                        .getInstance(org.opensearch.sql.common.setting.Settings.class)
                        .getSettingValue(Settings.Key.PPL_ENABLED);
  }

  @Override
  protected void doExecute(
          Task task, ActionRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    TransportPPLQueryRequest transformedRequest = TransportPPLQueryRequest.fromActionRequest(request);
    try {
      listener.onResponse(executeRequest(transformedRequest));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  protected TransportPPLQueryResponse executeRequest(TransportPPLQueryRequest request) throws Exception {
    if (!pplEnabled.get()) {
      throw new IllegalAccessException(
              "Either plugins.ppl.enabled or rest.action.multi.allow_explicit_index setting is"
                  + " false");
    }
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();

    QueryContext.addRequestId();

    PPLService pplService =
        SecurityAccess.doPrivileged(() -> injector.getInstance(PPLService.class));
//    TransportPPLQueryRequest transportRequest = TransportPPLQueryRequest.fromActionRequest(request);
    // in order to use PPL service, we need to convert TransportPPLQueryRequest to PPLQueryRequest
    PPLQueryRequest transformedRequest = request.toPPLQueryRequest();

    if (transformedRequest.isExplainRequest()) {
      return null; // dont ever call PPL explain for POC
    }

    Format format = format(transformedRequest);
    ResponseFormatter<QueryResult> formatter;
    if (format.equals(Format.CSV)) {
      formatter = new CsvResponseFormatter(transformedRequest.sanitize());
    } else if (format.equals(Format.RAW)) {
      formatter = new RawResponseFormatter();
    } else if (format.equals(Format.VIZ)) {
      formatter = new VisualizationResponseFormatter(transformedRequest.style());
    } else {
      formatter = new SimpleJsonResponseFormatter(PRETTY);
    }

    try {
      CompletableFuture<TransportPPLQueryResponse> future = new CompletableFuture<>();

      pplService.execute(
        transformedRequest,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            String responseContent = formatter.format(new QueryResult(response.getSchema(), response.getResults(), response.getCursor(), PPL_SPEC));
            future.complete(new TransportPPLQueryResponse(responseContent));
          }

          @Override
          public void onFailure(Exception e) {
            future.completeExceptionally(e);
          }
        },
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.ExplainResponse response) {
            future.complete(new TransportPPLQueryResponse("poc should never reach this point"));
          }

          @Override
          public void onFailure(Exception e) {
            future.completeExceptionally(e);
          }
        }
      );

      return future.get();
    } catch (Exception e) {
      throw new RuntimeException("Failed to execute PPL query", e);
    }
  }

  /**
   * TODO: need to extract an interface for both SQL and PPL action handler and move these common
   * methods to the interface. This is not easy to do now because SQL action handler is still in
   * legacy module.
   */
  private ResponseListener<ExecutionEngine.ExplainResponse> createExplainResponseListener(
      ActionListener<TransportPPLQueryResponse> listener) {
    return new ResponseListener<ExecutionEngine.ExplainResponse>() {
      @Override
      public void onResponse(ExecutionEngine.ExplainResponse response) {
        String responseContent =
            new JsonResponseFormatter<ExecutionEngine.ExplainResponse>(PRETTY) {
              @Override
              protected Object buildJsonObject(ExecutionEngine.ExplainResponse response) {
                return response;
              }
            }.format(response);
        listener.onResponse(new TransportPPLQueryResponse(responseContent));
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
      formatter = new SimpleJsonResponseFormatter(PRETTY);
    }

    return new ResponseListener<ExecutionEngine.QueryResponse>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        String responseContent =
            formatter.format(
                new QueryResult(
                    response.getSchema(), response.getResults(), response.getCursor(), PPL_SPEC));
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
}
