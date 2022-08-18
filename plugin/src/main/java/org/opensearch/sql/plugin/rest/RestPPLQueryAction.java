/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.plugin.rest;

import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.rest.RestStatus.OK;
import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;
import static org.opensearch.sql.opensearch.executor.Scheduler.schedule;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestController;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.LogUtils;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.opensearch.security.SecurityAccess;
import org.opensearch.sql.plugin.request.PPLQueryRequestFactory;
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
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class RestPPLQueryAction extends BaseRestHandler {
  public static final String QUERY_API_ENDPOINT = "/_plugins/_ppl";
  public static final String EXPLAIN_API_ENDPOINT = "/_plugins/_ppl/_explain";
  public static final String LEGACY_QUERY_API_ENDPOINT = "/_opendistro/_ppl";
  public static final String LEGACY_EXPLAIN_API_ENDPOINT = "/_opendistro/_ppl/_explain";

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Cluster service required by bean initialization.
   */
  private final ClusterService clusterService;

  /**
   * Settings required by been initialization.
   */
  private final Settings pluginSettings;

  private final Supplier<Boolean> pplEnabled;

  /**
   * Constructor of RestPPLQueryAction.
   */
  public RestPPLQueryAction(RestController restController, ClusterService clusterService,
                            Settings pluginSettings,
                            org.opensearch.common.settings.Settings clusterSettings) {
    super();
    this.clusterService = clusterService;
    this.pluginSettings = pluginSettings;
    this.pplEnabled =
        () -> MULTI_ALLOW_EXPLICIT_INDEX.get(clusterSettings)
            && (Boolean) pluginSettings.getSettingValue(Settings.Key.PPL_ENABLED);
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of();
  }

  @Override
  public List<ReplacedRoute> replacedRoutes() {
    return Arrays.asList(
        new ReplacedRoute(
            RestRequest.Method.POST, QUERY_API_ENDPOINT,
            RestRequest.Method.POST, LEGACY_QUERY_API_ENDPOINT),
        new ReplacedRoute(
            RestRequest.Method.POST, EXPLAIN_API_ENDPOINT,
            RestRequest.Method.POST, LEGACY_EXPLAIN_API_ENDPOINT));
  }

  @Override
  public String getName() {
    return "ppl_query_action";
  }

  @Override
  protected Set<String> responseParams() {
    Set<String> responseParams = new HashSet<>(super.responseParams());
    responseParams.addAll(Arrays.asList("format", "sanitize"));
    return responseParams;
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient nodeClient) {
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();

    LogUtils.addRequestId();

    if (!pplEnabled.get()) {
      return channel -> reportError(channel, new IllegalAccessException(
          "Either plugins.ppl.enabled or rest.action.multi.allow_explicit_index setting is false"
      ), BAD_REQUEST);
    }

    PPLService pplService = createPPLService(nodeClient);
    PPLQueryRequest pplRequest = PPLQueryRequestFactory.getPPLRequest(request);

    return channel -> schedule(nodeClient, () -> {
      if (pplRequest.isExplainRequest()) {
        pplService.explain(pplRequest, createExplainResponseListener(channel));
      } else {
        pplService.execute(pplRequest, createListener(channel, pplRequest));
      }
    });
  }

  /**
   * Ideally, the AnnotationConfigApplicationContext should be shared across Plugin. By default,
   * spring construct all the bean as singleton. Currently, there are no better solution to
   * create the bean in protocol scope. The limitations are
   * alt-1, add annotation for bean @Scope(value = SCOPE_PROTOTYPE, proxyMode = TARGET_CLASS), it
   * works by add the proxy,
   * but when running in OpenSearch, all the operation need security permission whic is hard
   * to control.
   * alt-2, using ObjectFactory with @Autowired, it also works, but require add to all the
   * configuration.
   * We will revisit the current solution if any major issue found.
   */
  private PPLService createPPLService(NodeClient client) {
    return doPrivileged(() -> {
      AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
      context.registerBean(ClusterService.class, () -> clusterService);
      context.registerBean(NodeClient.class, () -> client);
      context.registerBean(Settings.class, () -> pluginSettings);
      context.register(OpenSearchPluginConfig.class);
      context.register(PPLServiceConfig.class);
      context.refresh();
      return context.getBean(PPLService.class);
    });
  }

  /**
   * TODO: need to extract an interface for both SQL and PPL action handler and move these
   * common methods to the interface. This is not easy to do now because SQL action handler
   * is still in legacy module.
   */
  private ResponseListener<ExplainResponse> createExplainResponseListener(
      RestChannel channel) {
    return new ResponseListener<ExplainResponse>() {
      @Override
      public void onResponse(ExplainResponse response) {
        sendResponse(channel, OK, new JsonResponseFormatter<ExplainResponse>(PRETTY) {
          @Override
          protected Object buildJsonObject(ExplainResponse response) {
            return response;
          }
        }.format(response));
      }

      @Override
      public void onFailure(Exception e) {
        LOG.error("Error happened during explain", e);
        sendResponse(channel, INTERNAL_SERVER_ERROR,
            "Failed to explain the query due to error: " + e.getMessage());
      }
    };
  }

  private ResponseListener<QueryResponse> createListener(RestChannel channel,
                                                         PPLQueryRequest pplRequest) {
    Format format = pplRequest.format();
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
    return new ResponseListener<QueryResponse>() {
      @Override
      public void onResponse(QueryResponse response) {
        sendResponse(channel, OK, formatter.format(new QueryResult(response.getSchema(),
            response.getResults())));
      }

      @Override
      public void onFailure(Exception e) {
        LOG.error("Error happened during query handling", e);
        if (isClientError(e)) {
          Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_CUS).increment();
          reportError(channel, e, BAD_REQUEST);
        } else {
          Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_SYS).increment();
          reportError(channel, e, SERVICE_UNAVAILABLE);
        }
      }
    };
  }

  private <T> T doPrivileged(PrivilegedExceptionAction<T> action) {
    try {
      return SecurityAccess.doPrivileged(action);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to perform privileged action", e);
    }
  }

  private void sendResponse(RestChannel channel, RestStatus status, String content) {
    channel.sendResponse(
        new BytesRestResponse(status, "application/json; charset=UTF-8", content));
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(new BytesRestResponse(status,
        ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
  }

  private static boolean isClientError(Exception e) {
    return e instanceof NullPointerException
        // NPE is hard to differentiate but more likely caused by bad query
        || e instanceof IllegalArgumentException
        || e instanceof IndexNotFoundException
        || e instanceof SemanticCheckException
        || e instanceof ExpressionEvaluationException
        || e instanceof QueryEngineException
        || e instanceof SyntaxCheckException;
  }
}
