/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery;

import java.io.IOException;
import java.util.UUID;
import org.opensearch.common.inject.Inject;
import org.opensearch.sql.datasource.client.DataSourceClient;
import org.opensearch.sql.datasource.client.DataSourceClientFactory;
import org.opensearch.sql.datasource.client.exceptions.DataSourceClientException;
import org.opensearch.sql.datasource.query.QueryHandlerRegistry;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryResponse;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;

public class DirectQueryExecutorServiceImpl implements DirectQueryExecutorService {

  private final DataSourceClientFactory dataSourceClientFactory;
  private final QueryHandlerRegistry queryHandlerRegistry;

  @Inject
  public DirectQueryExecutorServiceImpl(
      DataSourceClientFactory dataSourceClientFactory, QueryHandlerRegistry queryHandlerRegistry) {
    this.dataSourceClientFactory = dataSourceClientFactory;
    this.queryHandlerRegistry = queryHandlerRegistry;
  }

  @Override
  public ExecuteDirectQueryResponse executeDirectQuery(ExecuteDirectQueryRequest request) {
    // TODO: Replace with the data source query id.
    String queryId = UUID.randomUUID().toString();
    String sessionId = request.getSessionId(); // Session ID is passed as is
    String dataSourceName = request.getDataSources();
    String dataSourceType = null;
    String result;

    try {
      dataSourceType =
          dataSourceClientFactory.getDataSourceType(dataSourceName).name().toLowerCase();

      DataSourceClient client = dataSourceClientFactory.createClient(dataSourceName);

      result =
          queryHandlerRegistry
              .getQueryHandler(client)
              .map(
                  handler -> {
                    try {
                      return handler.executeQuery(client, request);
                    } catch (IOException e) {
                      return "{\"error\": \"Error executing query: " + e.getMessage() + "\"}";
                    }
                  })
              .orElse("{\"error\": \"Unsupported data source type\"}");

    } catch (Exception e) {
      result = "{\"error\": \"" + e.getMessage() + "\"}";
    }

    return new ExecuteDirectQueryResponse(queryId, result, sessionId, dataSourceType);
  }

  @Override
  public GetDirectQueryResourcesResponse<?> getDirectQueryResources(
      GetDirectQueryResourcesRequest request) {
    DataSourceClient client = dataSourceClientFactory.createClient(request.getDataSource());
    return queryHandlerRegistry
        .getQueryHandler(client)
        .map(
            handler -> {
              try {
                return handler.getResources(client, request);
              } catch (IOException e) {
                throw new DataSourceClientException(
                    String.format(
                        "Error retrieving resources for data source type: %s",
                        request.getDataSource()),
                    e);
              }
            })
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    "Unsupported data source type: " + request.getDataSource()));
  }
}
