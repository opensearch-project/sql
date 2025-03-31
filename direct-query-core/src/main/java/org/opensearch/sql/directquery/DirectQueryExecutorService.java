/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery;

import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryResponse;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesResponse;

public interface DirectQueryExecutorService {

  /**
   * Execute a direct query request.
   *
   * @param request The direct query request
   * @return A response containing the result
   */
  ExecuteDirectQueryResponse executeDirectQuery(ExecuteDirectQueryRequest request);

  /**
   * Get resources from a data source.
   *
   * @param request The resources request
   * @return A response containing the requested resources
   */
  GetDirectQueryResourcesResponse<?> getDirectQueryResources(
      GetDirectQueryResourcesRequest request);
}
