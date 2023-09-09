/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.rest;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;
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
import org.opensearch.sql.datasources.exceptions.ErrorMessage;
import org.opensearch.sql.datasources.utils.Scheduler;
import org.opensearch.sql.spark.rest.model.CreateJobRequest;
import org.opensearch.sql.spark.transport.TransportCreateJobRequestAction;
import org.opensearch.sql.spark.transport.TransportDeleteJobRequestAction;
import org.opensearch.sql.spark.transport.TransportGetJobRequestAction;
import org.opensearch.sql.spark.transport.TransportGetQueryResultRequestAction;
import org.opensearch.sql.spark.transport.model.CreateJobActionRequest;
import org.opensearch.sql.spark.transport.model.CreateJobActionResponse;
import org.opensearch.sql.spark.transport.model.DeleteJobActionRequest;
import org.opensearch.sql.spark.transport.model.DeleteJobActionResponse;
import org.opensearch.sql.spark.transport.model.GetJobActionRequest;
import org.opensearch.sql.spark.transport.model.GetJobActionResponse;
import org.opensearch.sql.spark.transport.model.GetJobQueryResultActionRequest;
import org.opensearch.sql.spark.transport.model.GetJobQueryResultActionResponse;

public class RestJobManagementAction extends BaseRestHandler {

  public static final String JOB_ACTIONS = "job_actions";
  public static final String BASE_JOB_ACTION_URL = "/_plugins/_query/_jobs";

  private static final Logger LOG = LogManager.getLogger(RestJobManagementAction.class);

  @Override
  public String getName() {
    return JOB_ACTIONS;
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(

        /*
         *
         * Create a new job with spark execution engine.
         * Request URL: POST
         * Request body:
         * Ref [org.opensearch.sql.spark.transport.model.SubmitJobActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.spark.transport.model.SubmitJobActionResponse]
         */
        new Route(POST, BASE_JOB_ACTION_URL),

        /*
         *
         * GET jobs with in spark execution engine.
         * Request URL: GET
         * Request body:
         * Ref [org.opensearch.sql.spark.transport.model.SubmitJobActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.spark.transport.model.SubmitJobActionResponse]
         */
        new Route(GET, String.format(Locale.ROOT, "%s/{%s}", BASE_JOB_ACTION_URL, "jobId")),
        new Route(GET, BASE_JOB_ACTION_URL),

        /*
         *
         * Cancel a job within spark execution engine.
         * Request URL: DELETE
         * Request body:
         * Ref [org.opensearch.sql.spark.transport.model.SubmitJobActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.spark.transport.model.SubmitJobActionResponse]
         */
        new Route(DELETE, String.format(Locale.ROOT, "%s/{%s}", BASE_JOB_ACTION_URL, "jobId")),

        /*
         * GET query result from job {{jobId}} execution.
         * Request URL: GET
         * Request body:
         * Ref [org.opensearch.sql.spark.transport.model.GetJobQueryResultActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.spark.transport.model.GetJobQueryResultActionResponse]
         */
        new Route(GET, String.format(Locale.ROOT, "%s/{%s}/result", BASE_JOB_ACTION_URL, "jobId")));
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient)
      throws IOException {
    switch (restRequest.method()) {
      case POST:
        return executePostRequest(restRequest, nodeClient);
      case GET:
        return executeGetRequest(restRequest, nodeClient);
      case DELETE:
        return executeDeleteRequest(restRequest, nodeClient);
      default:
        return restChannel ->
            restChannel.sendResponse(
                new BytesRestResponse(
                    RestStatus.METHOD_NOT_ALLOWED, String.valueOf(restRequest.method())));
    }
  }

  private RestChannelConsumer executePostRequest(RestRequest restRequest, NodeClient nodeClient)
      throws IOException {
    CreateJobRequest submitJobRequest =
        CreateJobRequest.fromXContentParser(restRequest.contentParser());
    return restChannel ->
        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportCreateJobRequestAction.ACTION_TYPE,
                    new CreateJobActionRequest(submitJobRequest),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(CreateJobActionResponse createJobActionResponse) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.CREATED,
                                "application/json; charset=UTF-8",
                                submitJobRequest.getQuery()));
                      }

                      @Override
                      public void onFailure(Exception e) {
                        handleException(e, restChannel);
                      }
                    }));
  }

  private RestChannelConsumer executeGetRequest(RestRequest restRequest, NodeClient nodeClient) {
    Boolean isResultRequest = restRequest.rawPath().contains("result");
    if (isResultRequest) {
      return executeGetJobQueryResultRequest(nodeClient, restRequest);
    } else {
      return executeGetJobRequest(nodeClient, restRequest);
    }
  }

  private RestChannelConsumer executeGetJobQueryResultRequest(
      NodeClient nodeClient, RestRequest restRequest) {
    String jobId = restRequest.param("jobId");
    return restChannel ->
        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportGetQueryResultRequestAction.ACTION_TYPE,
                    new GetJobQueryResultActionRequest(jobId),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(
                          GetJobQueryResultActionResponse getJobQueryResultActionResponse) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.OK,
                                "application/json; charset=UTF-8",
                                getJobQueryResultActionResponse.getResult()));
                      }

                      @Override
                      public void onFailure(Exception e) {
                        handleException(e, restChannel);
                      }
                    }));
  }

  private RestChannelConsumer executeGetJobRequest(NodeClient nodeClient, RestRequest restRequest) {
    String jobId = restRequest.param("jobId");
    return restChannel ->
        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportGetJobRequestAction.ACTION_TYPE,
                    new GetJobActionRequest(jobId),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(GetJobActionResponse getJobActionResponse) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.OK,
                                "application/json; charset=UTF-8",
                                getJobActionResponse.getResult()));
                      }

                      @Override
                      public void onFailure(Exception e) {
                        handleException(e, restChannel);
                      }
                    }));
  }

  private void handleException(Exception e, RestChannel restChannel) {
    if (e instanceof OpenSearchException) {
      OpenSearchException exception = (OpenSearchException) e;
      reportError(restChannel, exception, exception.status());
    } else {
      LOG.error("Error happened during request handling", e);
      if (isClientError(e)) {
        reportError(restChannel, e, BAD_REQUEST);
      } else {
        reportError(restChannel, e, SERVICE_UNAVAILABLE);
      }
    }
  }

  private RestChannelConsumer executeDeleteRequest(RestRequest restRequest, NodeClient nodeClient) {
    String jobId = restRequest.param("jobId");
    return restChannel ->
        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportDeleteJobRequestAction.ACTION_TYPE,
                    new DeleteJobActionRequest(jobId),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(DeleteJobActionResponse deleteJobActionResponse) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.OK,
                                "application/json; charset=UTF-8",
                                deleteJobActionResponse.getResult()));
                      }

                      @Override
                      public void onFailure(Exception e) {
                        handleException(e, restChannel);
                      }
                    }));
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(status, new ErrorMessage(e, status.getStatus()).toString()));
  }

  private static boolean isClientError(Exception e) {
    return e instanceof IllegalArgumentException || e instanceof IllegalStateException;
  }
}
