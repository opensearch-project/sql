/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest;

import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR;
import static org.opensearch.rest.RestRequest.Method.GET;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Locale;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import org.opensearch.sql.datasources.exceptions.ErrorMessage;
import org.opensearch.sql.datasources.utils.Scheduler;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.transport.TransportGetDirectQueryResourcesRequestAction;
import org.opensearch.sql.directquery.transport.format.DirectQueryResourcesRequestConverter;
import org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionRequest;
import org.opensearch.sql.directquery.transport.model.GetDirectQueryResourcesActionResponse;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.opensearch.util.RestRequestUtil;
import org.opensearch.transport.client.node.NodeClient;

@RequiredArgsConstructor
public class RestDirectQueryResourcesManagementAction extends BaseRestHandler {

  public static final String DIRECT_QUERY_RESOURCES_ACTIONS = "direct_query_resources_actions";
  public static final String BASE_DIRECT_QUERY_RESOURCES_URL =
      "/_plugins/_directquery/_resources/{dataSource}";

  private static final Logger LOG =
      LogManager.getLogger(RestDirectQueryResourcesManagementAction.class);
  private final OpenSearchSettings settings;

  @Override
  public String getName() {
    return DIRECT_QUERY_RESOURCES_ACTIONS;
  }

  @Override
  public List<Route> routes() {
    return ImmutableList.of(
        new Route(
            GET,
            String.format(
                Locale.ROOT, "%s/api/v1/{resourceType}", BASE_DIRECT_QUERY_RESOURCES_URL)),
        new Route(
            GET,
            String.format(
                Locale.ROOT,
                "%s/api/v1/{resourceType}/{resourceName}/values",
                BASE_DIRECT_QUERY_RESOURCES_URL)),
        new Route(
            GET,
            String.format(
                Locale.ROOT,
                "%s/alertmanager/api/v2/alerts/groups",
                BASE_DIRECT_QUERY_RESOURCES_URL)),
        new Route(
            GET,
            String.format(
                Locale.ROOT,
                "%s/alertmanager/api/v2/{resourceType}",
                BASE_DIRECT_QUERY_RESOURCES_URL)));
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient) {
    if (!dataSourcesEnabled()) {
      return dataSourcesDisabledError(restRequest);
    }

    switch (restRequest.method()) {
      case GET:
        return executeGetResourcesRequest(restRequest, nodeClient);
      default:
        return restChannel ->
            restChannel.sendResponse(
                new BytesRestResponse(
                    RestStatus.METHOD_NOT_ALLOWED, String.valueOf(restRequest.method())));
    }
  }

  private RestChannelConsumer executeGetResourcesRequest(
      RestRequest restRequest, NodeClient nodeClient) {
    GetDirectQueryResourcesRequest directQueryRequest =
        DirectQueryResourcesRequestConverter.fromRestRequest(restRequest);

    return restChannel ->
        Scheduler.schedule(
            nodeClient,
            () ->
                nodeClient.execute(
                    TransportGetDirectQueryResourcesRequestAction.ACTION_TYPE,
                    new GetDirectQueryResourcesActionRequest(directQueryRequest),
                    new ActionListener<>() {
                      @Override
                      public void onResponse(GetDirectQueryResourcesActionResponse response) {
                        restChannel.sendResponse(
                            new BytesRestResponse(
                                RestStatus.OK,
                                "application/json; charset=UTF-8",
                                response.getResult()));
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
    } else {
      LOG.error("Error happened during request handling", e);
      if (isClientError(e)) {
        reportError(restChannel, e, BAD_REQUEST);
      } else {
        reportError(restChannel, e, INTERNAL_SERVER_ERROR);
      }
    }
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(status, new ErrorMessage(e, status.getStatus()).toString()));
  }

  private static boolean isClientError(Exception e) {
    return e instanceof IllegalArgumentException
        || e instanceof IllegalStateException
        || e instanceof DataSourceClientException
        || e instanceof IllegalAccessException;
  }

  private boolean dataSourcesEnabled() {
    return settings.getSettingValue(Settings.Key.DATASOURCES_ENABLED);
  }

  private RestChannelConsumer dataSourcesDisabledError(RestRequest request) {
    RestRequestUtil.consumeAllRequestParameters(request);

    return channel -> {
      reportError(
          channel,
          new IllegalAccessException(
              String.format("%s setting is false", Settings.Key.DATASOURCES_ENABLED.getKeyValue())),
          BAD_REQUEST);
    };
  }
}
