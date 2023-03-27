/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.plugin.rest;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.rest.RestStatus.SERVICE_UNAVAILABLE;
import static org.opensearch.sql.plugin.utils.Scheduler.schedule;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionListener;
import org.opensearch.client.node.NodeClient;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.opensearch.response.error.ErrorMessageFactory;
import org.opensearch.sql.plugin.model.CreateDataSourceActionRequest;
import org.opensearch.sql.plugin.model.CreateDataSourceActionResponse;
import org.opensearch.sql.plugin.transport.datasource.TransportCreateDataSourceAction;
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
        new Route(POST, BASE_DATASOURCE_ACTION_URL)
    );
  }

  @Override
  protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient nodeClient)
      throws IOException {
    switch (restRequest.method()) {
      case POST:
        return executePostRequest(restRequest, nodeClient);
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
                    new BytesRestResponse(RestStatus.OK, "application/json; charset=UTF-8",
                        createDataSourceActionResponse.getResult()));
              }

              @Override
              public void onFailure(Exception e) {
                if (e instanceof IllegalAccessException) {
                  reportError(restChannel, e, BAD_REQUEST);
                } else {
                  LOG.error("Error happened during query handling", e);
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
            }));
  }

  private void reportError(final RestChannel channel, final Exception e, final RestStatus status) {
    channel.sendResponse(
        new BytesRestResponse(
            status, ErrorMessageFactory.createErrorMessage(e, status.getStatus()).toString()));
  }

  private static boolean isClientError(Exception e) {
    return e instanceof NullPointerException
        // NPE is hard to differentiate but more likely caused by bad query
        || e instanceof IllegalArgumentException
        || e instanceof IndexNotFoundException;
  }

}