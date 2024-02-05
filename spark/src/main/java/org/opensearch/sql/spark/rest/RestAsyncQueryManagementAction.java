/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.core.rest.RestStatus.TOO_MANY_REQUESTS;
import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.datasources.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.datasources.exceptions.ErrorMessage;
import org.opensearch.sql.datasources.utils.Scheduler;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.utils.MetricUtils;
import org.opensearch.sql.spark.asyncquery.exceptions.AsyncQueryNotFoundException;
import org.opensearch.sql.spark.leasemanager.ConcurrencyLimitExceededException;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;
import org.opensearch.sql.spark.transport.TransportCancelAsyncQueryRequestAction;
import org.opensearch.sql.spark.transport.TransportCreateAsyncQueryRequestAction;
import org.opensearch.sql.spark.transport.TransportGetAsyncQueryResultAction;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionResponse;
import org.opensearch.sql.spark.transport.model.CreateAsyncQueryActionRequest;
import org.opensearch.sql.spark.transport.model.CreateAsyncQueryActionResponse;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionResponse;

public class RestAsyncQueryManagementAction extends BaseRestHandler {

  public static final String ASYNC_QUERY_ACTIONS = "async_query_actions";
  public static final String BASE_ASYNC_QUERY_ACTION_URL = "/_plugins/_async_query";

  private static final Logger LOG = LogManager.getLogger(RestAsyncQueryManagementAction.class);

  @Override
  public String getName() {
    return ASYNC_QUERY_ACTIONS;
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(

        /*
         *
         * Create a new async query using spark execution engine.
         * Request URL: POST
         * Request body:
         * Ref [org.opensearch.sql.spark.transport.model.CreateAsyncQueryActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.spark.transport.model.CreateAsyncQueryActionResponse]
         */
        new Route(POST, BASE_ASYNC_QUERY_ACTION_URL),

        /*
         *
         * GET Async Query result with in spark execution engine.
         * Request URL: GET
         * Request body:
         * Ref [org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.spark.transport.model.GetAsyncQueryResultActionResponse]
         */
        new Route(
            GET, String.format(Locale.ROOT, "%s/{%s}", BASE_ASYNC_QUERY_ACTION_URL, "queryId")),

        /*
         *
         * Cancel a job within spark execution engine.
         * Request URL: DELETE
         * Request body:
         * Ref [org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.spark.transport.model.CancelAsyncQueryActionResponse]
         */
        new Route(
            DELETE, String.format(Locale.ROOT, "%s/{%s}", BASE_ASYNC_QUERY_ACTION_URL, "queryId")));
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient)
      throws IOException {
    switch (restRequest.method()) {
      case POST:
        return executePostRequest(restRequest, nodeClient);
      case GET:
        return executeGetAsyncQueryResultRequest(restRequest, nodeClient);
      case DELETE:
        return executeDeleteRequest(restRequest, nodeClient);
      default:
        return restChannel ->
            restChannel.sendResponse(
                new BytesRestResponse(
                    RestStatus.METHOD_NOT_ALLOWED, String.valueOf(restRequest.method())));
    }
  }

  private RestChannelConsumer executePostRequest(RestRequest restRequest, NodeClient nodeClient) {
    return restChannel -> {
      try {
        MetricUtils.incrementNumericalMetric(MetricName.ASYNC_QUERY_CREATE_API_REQUEST_COUNT);
        CreateAsyncQueryRequest submitJobRequest =
            CreateAsyncQueryRequest.fromXContentParser(restRequest.contentParser());
        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportCreateAsyncQueryRequestAction.ACTION_TYPE,
                    new CreateAsyncQueryActionRequest(submitJobRequest),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(
                          CreateAsyncQueryActionResponse createAsyncQueryActionResponse) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.CREATED,
                                "application/json; charset=UTF-8",
                                createAsyncQueryActionResponse.getResult()));
                      }

                      @Override
                      public void onFailure(Exception e) {
                        handleException(e, restChannel, restRequest.method());
                      }
                    }));
      } catch (Exception e) {
        handleException(e, restChannel, restRequest.method());
      }
    };
  }

  private RestChannelConsumer executeGetAsyncQueryResultRequest(
      RestRequest restRequest, NodeClient nodeClient) {
    MetricUtils.incrementNumericalMetric(MetricName.ASYNC_QUERY_GET_API_REQUEST_COUNT);
    String queryId = restRequest.param("queryId");
    return restChannel ->
        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportGetAsyncQueryResultAction.ACTION_TYPE,
                    new GetAsyncQueryResultActionRequest(queryId),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(
                          GetAsyncQueryResultActionResponse getAsyncQueryResultActionResponse) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.OK,
                                "application/json; charset=UTF-8",
                                getAsyncQueryResultActionResponse.getResult()));
                      }

                      @Override
                      public void onFailure(Exception e) {
                        handleException(e, restChannel, restRequest.method());
                      }
                    }));
  }

  private void handleException(
      Exception e, RestChannel restChannel, RestRequest.Method requestMethod) {
    if (e instanceof OpenSearchException) {
      OpenSearchException exception = (OpenSearchException) e;
      reportError(restChannel, exception, exception.status());
      addCustomerErrorMetric(requestMethod);
    } else if (e instanceof ConcurrencyLimitExceededException) {
      LOG.error("Too many request", e);
      reportError(restChannel, e, TOO_MANY_REQUESTS);
      addCustomerErrorMetric(requestMethod);
    } else {
      LOG.error("Error happened during request handling", e);
      if (isClientError(e)) {
        reportError(restChannel, e, BAD_REQUEST);
        addCustomerErrorMetric(requestMethod);
      } else {
        reportError(restChannel, e, INTERNAL_SERVER_ERROR);
        addSystemErrorMetric(requestMethod);
      }
    }
  }

  private RestChannelConsumer executeDeleteRequest(RestRequest restRequest, NodeClient nodeClient) {
    MetricUtils.incrementNumericalMetric(MetricName.ASYNC_QUERY_CANCEL_API_REQUEST_COUNT);
    String queryId = restRequest.param("queryId");
    return restChannel ->
        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportCancelAsyncQueryRequestAction.ACTION_TYPE,
                    new CancelAsyncQueryActionRequest(queryId),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(
                          CancelAsyncQueryActionResponse cancelAsyncQueryActionResponse) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.NO_CONTENT,
                                "application/json; charset=UTF-8",
                                cancelAsyncQueryActionResponse.getResult()));
                      }

                      @Override
                      public void onFailure(Exception e) {
                        handleException(e, restChannel, restRequest.method());
                      }
                    }));
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(status, new ErrorMessage(e, status.getStatus()).toString()));
  }

  private static boolean isClientError(Exception e) {
    return e instanceof IllegalArgumentException
        || e instanceof IllegalStateException
        || e instanceof DataSourceNotFoundException
        || e instanceof AsyncQueryNotFoundException;
  }

  private void addSystemErrorMetric(RestRequest.Method requestMethod) {
    switch (requestMethod) {
      case POST:
        MetricUtils.incrementNumericalMetric(
            MetricName.ASYNC_QUERY_CREATE_API_FAILED_REQ_COUNT_SYS);
        break;
      case GET:
        MetricUtils.incrementNumericalMetric(MetricName.ASYNC_QUERY_GET_API_FAILED_REQ_COUNT_SYS);
        break;
      case DELETE:
        MetricUtils.incrementNumericalMetric(
            MetricName.ASYNC_QUERY_CANCEL_API_FAILED_REQ_COUNT_SYS);
        break;
    }
  }

  private void addCustomerErrorMetric(RestRequest.Method requestMethod) {
    switch (requestMethod) {
      case POST:
        MetricUtils.incrementNumericalMetric(
            MetricName.ASYNC_QUERY_CREATE_API_FAILED_REQ_COUNT_CUS);
        break;
      case GET:
        MetricUtils.incrementNumericalMetric(MetricName.ASYNC_QUERY_GET_API_FAILED_REQ_COUNT_CUS);
        break;
      case DELETE:
        MetricUtils.incrementNumericalMetric(
            MetricName.ASYNC_QUERY_CANCEL_API_FAILED_REQ_COUNT_CUS);
        break;
    }
  }
}
