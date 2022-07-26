/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Locale;
import java.util.Optional;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsAction;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.catalog.CatalogService;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.LogUtils;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.plugin.catalog.CatalogServiceImpl;
import org.opensearch.sql.plugin.rest.OpenSearchPluginConfig;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.config.PPLServiceConfig;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.VisualizationResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/** Send PPL query transport action. */
public class TransportPPLQueryAction
    extends HandledTransportAction<ActionRequest, TransportPPLQueryResponse> {
  private final NodeClient client;

  /** Cluster service required by bean initialization. */
  private final ClusterService clusterService;

  /** Settings required by been initialization. */
  private final Settings pluginSettings;


  /** Constructor of TransportPPLQueryAction. */
  @Inject
  public TransportPPLQueryAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      org.opensearch.common.settings.Settings clusterSettings) {
    super(PPLQueryAction.NAME, transportService, actionFilters, TransportPPLQueryRequest::new);
    this.client = client;
    this.clusterService = clusterService;
    this.pluginSettings = new OpenSearchSettings(clusterService.getClusterSettings());
  }

  /**
   * {@inheritDoc} Transform the request and call super.doExecute() to support call from other
   * plugins.
   */
  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();

    LogUtils.addRequestId();

    PPLService pplService = createPPLService(client);
    TransportPPLQueryRequest transportRequest = TransportPPLQueryRequest.fromActionRequest(request);
    // in order to use PPL service, we need to convert TransportPPLQueryRequest to PPLQueryRequest
    PPLQueryRequest transformedRequest = transportRequest.toPPLQueryRequest();

    if (transformedRequest.isExplainRequest()) {
      pplService.explain(transformedRequest, createExplainResponseListener(listener));
    } else {
      pplService.execute(transformedRequest, createListener(transformedRequest, listener));
    }
  }

  private PPLService createPPLService(NodeClient client) {
    return doPrivileged(
        () -> {
          AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
          context.registerBean(ClusterService.class, () -> clusterService);
          context.registerBean(NodeClient.class, () -> client);
          context.registerBean(Settings.class, () -> pluginSettings);
          context.registerBean(CatalogService.class, CatalogServiceImpl::getInstance);
          context.register(OpenSearchPluginConfig.class);
          context.register(PPLServiceConfig.class);

          context.refresh();
          return context.getBean(PPLService.class);
        });
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
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
      formatter = new SimpleJsonResponseFormatter(JsonResponseFormatter.Style.PRETTY);
    }

    return new ResponseListener<ExecutionEngine.QueryResponse>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        String responseContent =
            formatter.format(new QueryResult(response.getSchema(), response.getResults()));
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
