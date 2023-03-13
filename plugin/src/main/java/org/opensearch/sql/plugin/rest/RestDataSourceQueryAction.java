/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;
import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;
import static org.opensearch.sql.plugin.utils.Scheduler.schedule;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.datasource.exceptions.DataSourceNotFoundException;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.plugin.model.CreateDataSourceActionRequest;
import org.opensearch.sql.plugin.model.CreateDataSourceActionResponse;
import org.opensearch.sql.plugin.model.DeleteDataSourceActionRequest;
import org.opensearch.sql.plugin.model.DeleteDataSourceActionResponse;
import org.opensearch.sql.plugin.model.GetDataSourceActionRequest;
import org.opensearch.sql.plugin.model.GetDataSourceActionResponse;
import org.opensearch.sql.plugin.model.UpdateDataSourceActionRequest;
import org.opensearch.sql.plugin.model.UpdateDataSourceActionResponse;
import org.opensearch.sql.plugin.transport.datasource.TransportCreateDataSourceAction;
import org.opensearch.sql.plugin.transport.datasource.TransportDeleteDataSourceAction;
import org.opensearch.sql.plugin.transport.datasource.TransportGetDataSourceAction;
import org.opensearch.sql.plugin.transport.datasource.TransportUpdateDataSourceAction;
import org.opensearch.sql.plugin.utils.XContentParserUtils;

public class RestDataSourceQueryAction extends BaseRestHandler {

  public static final String DATASOURCE_ACTIONS = "datasource_actions";
  public static final String BASE_DATASOURCE_ACTION_URL = "/_plugins/_query/_datasources";

  private static final Logger LOG = LogManager.getLogger(RestDataSourceQueryAction.class);

  @Override
  public String getName() {
    return DATASOURCE_ACTIONS;
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(

        /*
         *
         * Create a new datasource.
         * Request URL: POST
         * Request body:
         * Ref [org.opensearch.sql.plugin.transport.datasource.model.CreateDataSourceActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.plugin.transport.datasource.model.CreateDataSourceActionResponse]
         */
        new Route(POST, BASE_DATASOURCE_ACTION_URL),

        /*
         * GET datasources
         * Request URL: GET
         * Request body:
         * Ref [org.opensearch.sql.plugin.transport.datasource.model.GetDataSourceActionRequest]
         * Response body:
         * Ref [org.opensearch.sql.plugin.transport.datasource.model.GetDataSourceActionResponse]
         */
        new Route(GET, String.format(Locale.ROOT, "%s/{%s}",
            BASE_DATASOURCE_ACTION_URL, "dataSourceName")),
        new Route(GET, BASE_DATASOURCE_ACTION_URL),

        /*
         * GET datasources
         * Request URL: GET
         * Request body:
         * Ref
         * [org.opensearch.sql.plugin.transport.datasource.model.UpdateDataSourceActionRequest]
         * Response body:
         * Ref
         * [org.opensearch.sql.plugin.transport.datasource.model.UpdateDataSourceActionResponse]
         */
        new Route(PUT, BASE_DATASOURCE_ACTION_URL),

        /*
         * GET datasources
         * Request URL: GET
         * Request body: Ref
         * [org.opensearch.sql.plugin.transport.datasource.model.DeleteDataSourceActionRequest]
         * Response body: Ref
         * [org.opensearch.sql.plugin.transport.datasource.model.DeleteDataSourceActionResponse]
         */
        new Route(DELETE, String.format(Locale.ROOT, "%s/{%s}",
            BASE_DATASOURCE_ACTION_URL, "dataSourceName"))
    );
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient)
      throws IOException {
    switch (restRequest.method()) {
      case POST:
        return executePostRequest(restRequest, nodeClient);
      case GET:
        return executeGetRequest(restRequest, nodeClient);
      case PUT:
        return executeUpdateRequest(restRequest, nodeClient);
      case DELETE:
        return executeDeleteRequest(restRequest, nodeClient);
      default:
        return restChannel
            -> restChannel.sendResponse(new BytesRestResponse(RestStatus.METHOD_NOT_ALLOWED,
            String.valueOf(restRequest.method())));
    }
  }

  private RestChannelConsumer executePostRequest(RestRequest restRequest,
                                                 NodeClient nodeClient) throws IOException {

    DataSourceMetadata dataSourceMetadata
        = XContentParserUtils.toDataSourceMetadata(restRequest.contentParser());
    return restChannel -> schedule(nodeClient,
        () -> nodeClient.execute(TransportCreateDataSourceAction.ACTION_TYPE,
            new CreateDataSourceActionRequest(dataSourceMetadata),
            new ActionListener<>() {
              @Override
              public void onResponse(
                  CreateDataSourceActionResponse createDataSourceActionResponse) {
                restChannel.sendResponse(
                    new BytesRestResponse(RestStatus.CREATED, "application/json; charset=UTF-8",
                        createDataSourceActionResponse.getResult()));
              }

              @Override
              public void onFailure(Exception e) {
                handleException(e, restChannel);
              }
            }));
  }

  private RestChannelConsumer executeGetRequest(RestRequest restRequest,
                                                NodeClient nodeClient) {
    String dataSourceName = restRequest.param("dataSourceName");
    return restChannel -> schedule(nodeClient,
        () -> nodeClient.execute(TransportGetDataSourceAction.ACTION_TYPE,
            new GetDataSourceActionRequest(dataSourceName),
            new ActionListener<>() {
              @Override
              public void onResponse(GetDataSourceActionResponse getDataSourceActionResponse) {
                restChannel.sendResponse(
                    new BytesRestResponse(RestStatus.OK, "application/json; charset=UTF-8",
                        getDataSourceActionResponse.getResult()));
              }

              @Override
              public void onFailure(Exception e) {
                handleException(e, restChannel);
              }
            }));
  }

  private RestChannelConsumer executeUpdateRequest(RestRequest restRequest,
                                                   NodeClient nodeClient) throws IOException {
    DataSourceMetadata dataSourceMetadata
        = XContentParserUtils.toDataSourceMetadata(restRequest.contentParser());
    return restChannel -> schedule(nodeClient,
        () -> nodeClient.execute(TransportUpdateDataSourceAction.ACTION_TYPE,
            new UpdateDataSourceActionRequest(dataSourceMetadata),
            new ActionListener<>() {
              @Override
              public void onResponse(
                  UpdateDataSourceActionResponse updateDataSourceActionResponse) {
                restChannel.sendResponse(
                    new BytesRestResponse(RestStatus.OK, "application/json; charset=UTF-8",
                        updateDataSourceActionResponse.getResult()));
              }

              @Override
              public void onFailure(Exception e) {
                handleException(e, restChannel);
              }
            }));
  }

  private RestChannelConsumer executeDeleteRequest(RestRequest restRequest,
                                                   NodeClient nodeClient) {

    String dataSourceName = restRequest.param("dataSourceName");
    return restChannel -> schedule(nodeClient,
        () -> nodeClient.execute(TransportDeleteDataSourceAction.ACTION_TYPE,
            new DeleteDataSourceActionRequest(dataSourceName),
            new ActionListener<>() {
              @Override
              public void onResponse(
                  DeleteDataSourceActionResponse deleteDataSourceActionResponse) {
                restChannel.sendResponse(
                    new BytesRestResponse(RestStatus.NO_CONTENT, "application/json; charset=UTF-8",
                        deleteDataSourceActionResponse.getResult()));
              }

              @Override
              public void onFailure(Exception e) {
                handleException(e, restChannel);
              }
            }));
  }

  private void handleException(Exception e, RestChannel restChannel) {
    if (e instanceof DataSourceNotFoundException) {
      reportError(restChannel, e, NOT_FOUND);
    } else {
      LOG.error("Error happened during request handling", e);
      if (isClientError(e)) {
        Metrics.getInstance()
            .getNumericalMetric(MetricName.DATASOURCE_FAILED_REQ_COUNT_CUS)
            .increment();
        reportError(restChannel, e, BAD_REQUEST);
      } else {
        Metrics.getInstance()
            .getNumericalMetric(MetricName.DATASOURCE_FAILED_REQ_COUNT_SYS)
            .increment();
        reportError(restChannel, e, SERVICE_UNAVAILABLE);
      }
    }
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(
            status, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
  }

  private static boolean isClientError(Exception e) {
    return e instanceof NullPointerException
        // NPE is hard to differentiate but more likely caused by bad query
        || e instanceof IllegalArgumentException;
  }

}