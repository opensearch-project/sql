/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.core.rest.RestStatus.OK;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.datasources.exceptions.DataSourceClientException;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.plugin.request.PPLQueryRequestFactory;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;

public class RestPPLQueryAction extends BaseRestHandler {
  public static final String QUERY_API_ENDPOINT = "/_plugins/_ppl";
  public static final String EXPLAIN_API_ENDPOINT = "/_plugins/_ppl/_explain";
  public static final String LEGACY_QUERY_API_ENDPOINT = "/_opendistro/_ppl";
  public static final String LEGACY_EXPLAIN_API_ENDPOINT = "/_opendistro/_ppl/_explain";

  private static final Logger LOG = LogManager.getLogger();

  /** Constructor of RestPPLQueryAction. */
  public RestPPLQueryAction() {
    super();
  }

  private static boolean isClientError(Exception e) {
    return e instanceof NullPointerException
        // NPE is hard to differentiate but more likely caused by bad query
        || e instanceof IllegalArgumentException
        || e instanceof IndexNotFoundException
        || e instanceof SemanticCheckException
        || e instanceof ExpressionEvaluationException
        || e instanceof QueryEngineException
        || e instanceof SyntaxCheckException
        || e instanceof DataSourceClientException
        || e instanceof IllegalAccessException;
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
    TransportPPLQueryRequest transportPPLQueryRequest =
        new TransportPPLQueryRequest(PPLQueryRequestFactory.getPPLRequest(request));

    return channel ->
        nodeClient.execute(
            PPLQueryAction.INSTANCE,
            transportPPLQueryRequest,
            new ActionListener<>() {
              @Override
              public void onResponse(TransportPPLQueryResponse response) {
                sendResponse(channel, OK, response.getResult());
              }

              @Override
              public void onFailure(Exception e) {
                if (transportPPLQueryRequest.isExplainRequest()) {
                  LOG.error("Error happened during explain", e);
                  sendResponse(
                      channel,
                      INTERNAL_SERVER_ERROR,
                      "Failed to explain the query due to error: " + e.getMessage());
                } else if (e instanceof OpenSearchException) {
                  Metrics.getInstance()
                      .getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_CUS)
                      .increment();
                  OpenSearchException exception = (OpenSearchException) e;
                  reportError(channel, exception, exception.status());
                } else {
                  LOG.error("Error happened during query handling", e);
                  if (isClientError(e)) {
                    Metrics.getInstance()
                        .getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_CUS)
                        .increment();
                    reportError(channel, e, BAD_REQUEST);
                  } else {
                    Metrics.getInstance()
                        .getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_SYS)
                        .increment();
                    reportError(channel, e, INTERNAL_SERVER_ERROR);
                  }
                }
              }
            });
  }

  private void sendResponse(RestChannel channel, RestStatus status, String content) {
    channel.sendResponse(new BytesRestResponse(status, "application/json; charset=UTF-8", content));
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(
            status, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
  }
}
