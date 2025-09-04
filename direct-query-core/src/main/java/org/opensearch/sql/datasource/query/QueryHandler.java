/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasource.query;

import java.io.IOException;
import org.opensearch.sql.datasource.client.DataSourceClient;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;

/**
 * Interface for handling queries for specific data source types.
 *
 * @param <T> The client type this handler works with, extending DataSourceClient
 */
public interface QueryHandler<T extends DataSourceClient> {

  /**
   * Returns the data source type this handler supports.
   *
   * @return The supported data source type
   */
  DataSourceType getSupportedDataSourceType();

  /**
   * Executes a query for the supported data source type.
   *
   * @param client The client instance to use
   * @param request The query request
   * @return JSON string result of the query
   * @throws IOException If query execution fails
   */
  String executeQuery(T client, ExecuteDirectQueryRequest request) throws IOException;

  /**
   * Gets resources from the data source.
   *
   * @param client The client instance to use
   * @param request The resources request
   * @return Response containing the requested resources
   * @throws IOException If resource retrieval fails
   */
  GetDirectQueryResourcesResponse<?> getResources(T client, GetDirectQueryResourcesRequest request)
      throws IOException;

  /**
   * Checks if this handler can handle the given client type.
   *
   * @param client The client to check
   * @return true if this handler can handle the client
   */
  boolean canHandle(DataSourceClient client);

  /**
   * Gets the client class this handler supports.
   *
   * @return The class of client this handler supports
   */
  Class<T> getClientClass();
}
