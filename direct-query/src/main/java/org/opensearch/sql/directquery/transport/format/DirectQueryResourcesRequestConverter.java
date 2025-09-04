/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.format;

import java.util.HashMap;
import java.util.Map;
import lombok.experimental.UtilityClass;
import org.opensearch.rest.RestRequest;
import org.opensearch.sql.directquery.rest.model.DirectQueryResourceType;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;

@UtilityClass
public class DirectQueryResourcesRequestConverter {

  /**
   * Converts a RestRequest to a GetDirectQueryResourcesRequest.
   *
   * @param restRequest The REST request to convert
   * @return A configured GetDirectQueryResourcesRequest
   */
  public static GetDirectQueryResourcesRequest fromRestRequest(RestRequest restRequest) {
    GetDirectQueryResourcesRequest directQueryRequest = new GetDirectQueryResourcesRequest();
    directQueryRequest.setDataSource(restRequest.param("dataSource"));

    String path = restRequest.path();
    if (path.contains("/alertmanager/api/v2/")) {
      // Handle Alertmanager API endpoints
      if (path.contains("/alerts/groups")) {
        directQueryRequest.setResourceType(DirectQueryResourceType.ALERTMANAGER_ALERT_GROUPS);
      } else {
        directQueryRequest.setResourceType(
            DirectQueryResourceType.fromString(
                "alertmanager_" + restRequest.param("resourceType")));
      }
    } else {
      directQueryRequest.setResourceTypeFromString(restRequest.param("resourceType"));
      if (restRequest.param("resourceName") != null) {
        directQueryRequest.setResourceName(restRequest.param("resourceName"));
      }
    }

    Map<String, String> queryParams = new HashMap<>();
    for (String key : restRequest.params().keySet()) {
      if (!restRequest.consumedParams().contains(key)) {
        queryParams.put(key, restRequest.param(key));
      }
    }
    directQueryRequest.setQueryParams(queryParams);

    return directQueryRequest;
  }
}
