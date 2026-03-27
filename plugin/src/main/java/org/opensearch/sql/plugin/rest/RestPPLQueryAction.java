/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.core.rest.RestStatus.OK;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.datasources.exceptions.DataSourceClientException;
import org.opensearch.sql.exception.QueryEngineException;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.plugin.request.PPLQueryRequestFactory;
import org.opensearch.sql.plugin.transport.PPLQueryAction;
import org.opensearch.sql.plugin.transport.TransportPPLQueryRequest;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.transport.client.node.NodeClient;

public class RestPPLQueryAction extends BaseRestHandler {
  public static final String QUERY_API_ENDPOINT = "/_plugins/_ppl";
  public static final String EXPLAIN_API_ENDPOINT = "/_plugins/_ppl/_explain";

  private static final Logger LOG = LogManager.getLogger();

  /** Constructor of RestPPLQueryAction. */
  public RestPPLQueryAction() {
    super();
  }

  private static boolean isClientError(Exception ex) {
    return ex instanceof IllegalArgumentException
        || ex instanceof IndexNotFoundException
        || ex instanceof QueryEngineException
        || ex instanceof SyntaxCheckException
        || ex instanceof DataSourceClientException
        || ex instanceof IllegalAccessException;
  }

  private static int getRawErrorCode(Exception ex) {
    if (ex instanceof ErrorReport) {
      return getRawErrorCode(((ErrorReport) ex).getCause());
    }
    if (ex instanceof OpenSearchException) {
      return ((OpenSearchException) ex).status().getStatus();
    }
    if (isClientError(ex)) {
      return 400;
    }
    return 500;
  }

  private static RestStatus loggedErrorCode(Exception ex) {
    int code = getRawErrorCode(ex);

    // If we hit neither branch, no-op as false alarm error? I don't believe we can ever hit this
    // scenario.
    if (400 <= code && code < 500) {
      Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_CUS).increment();
    } else if (500 <= code && code < 600) {
      Metrics.getInstance().getNumericalMetric(MetricName.PPL_FAILED_REQ_COUNT_SYS).increment();
    } else {
      LOG.warn("Got an exception returning non-error status {}", RestStatus.fromCode(code), ex);
    }
    return RestStatus.fromCode(code);
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(
        new Route(RestRequest.Method.POST, QUERY_API_ENDPOINT),
        new Route(RestRequest.Method.POST, EXPLAIN_API_ENDPOINT));
  }

  @Override
  public String getName() {
    return "ppl_query_action";
  }

  @Override
  protected Set<String> responseParams() {
    Set<String> responseParams = new HashSet<>(super.responseParams());
    responseParams.addAll(Arrays.asList("format", "mode", "sanitize", "fetch_size"));
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
                sendResponse(channel, OK, response.getContentType(), response.getResult());
              }

              @Override
              public void onFailure(Exception e) {
                RestStatus status = loggedErrorCode(e);
                if (transportPPLQueryRequest.isExplainRequest()) {
                  LOG.error("Error happened during explain (status {})", status, e);
                } else {
                  LOG.error("Error happened during query handling (status {})", status, e);
                }
                reportError(channel, e, status);
              }
            });
  }

  private void sendResponse(
      RestChannel channel, RestStatus status, String contentType, String content) {
    channel.sendResponse(new BytesRestResponse(status, contentType, content));
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(
            status, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
  }
}
