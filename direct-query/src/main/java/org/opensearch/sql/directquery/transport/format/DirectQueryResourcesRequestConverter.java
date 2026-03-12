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
import org.opensearch.sql.directquery.rest.model.WriteDirectQueryResourcesRequest;

/*
 * @opensearch.experimental
 */
@UtilityClass
public class DirectQueryResourcesRequestConverter {

  /**
   * Converts a RestRequest to a GetDirectQueryResourcesRequest.
   *
   * @param restRequest The REST request to convert
   * @return A configured GetDirectQueryResourcesRequest
   */
  public static GetDirectQueryResourcesRequest toGetDirectRestRequest(RestRequest restRequest) {
    GetDirectQueryResourcesRequest directQueryRequest = new GetDirectQueryResourcesRequest();
    directQueryRequest.setDataSource(restRequest.param("dataSource"));

    // TODO: Move prometheus code into prometheus module/classes
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
    } else if (restRequest.param("namespace") != null) {
      // Handle Ruler API - GET /api/v1/rules/{namespace}
      directQueryRequest.setResourceType(DirectQueryResourceType.RULES);
      directQueryRequest.setResourceName(restRequest.param("namespace"));
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

  /**
   * Converts a RestRequest to a WriteDirectQueryResourcesRequest. Handles POST (create/update) and
   * DELETE operations.
   *
   * @param restRequest The REST request to convert
   * @return A configured WriteDirectQueryResourcesRequest
   */
  public static WriteDirectQueryResourcesRequest toWriteDirectRestRequest(
      RestRequest restRequest) {
    WriteDirectQueryResourcesRequest directQueryRequest = new WriteDirectQueryResourcesRequest();

    directQueryRequest.setDataSource(restRequest.param("dataSource"));
    boolean isDelete = RestRequest.Method.DELETE.equals(restRequest.method());
    directQueryRequest.setDelete(isDelete);

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
    } else if (restRequest.param("namespace") != null) {
      // Handle Ruler API - POST/DELETE /api/v1/rules/{namespace}
      directQueryRequest.setResourceType(DirectQueryResourceType.RULES);
      directQueryRequest.setResourceName(restRequest.param("namespace"));
      if (restRequest.param("groupName") != null) {
        directQueryRequest.setGroupName(restRequest.param("groupName"));
      }
    } else {
      directQueryRequest.setResourceTypeFromString(restRequest.param("resourceType"));
      if (restRequest.param("resourceName") != null) {
        directQueryRequest.setResourceName(restRequest.param("resourceName"));
      }
    }

    if (isDelete) {
      // DELETE requests have no body
      return directQueryRequest;
    }

    if (restRequest.hasContent()) {
      directQueryRequest.setRequest(restRequest.content().utf8ToString());
    } else {
      throw new IllegalArgumentException(
          "The write direct resource request must have a request in the body");
    }
    return directQueryRequest;
  }
}
