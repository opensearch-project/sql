/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class GetDirectQueryResourcesRequest {
  private String dataSource;
  private DirectQueryResourceType resourceType;
  private String resourceName;

  // Optional fields
  private Map<String, String> queryParams;

  /**
   * Sets the resource type from a string value.
   *
   * @param resourceTypeStr The resource type as a string
   */
  public void setResourceTypeFromString(String resourceTypeStr) {
    if (resourceTypeStr != null) {
      this.resourceType = DirectQueryResourceType.fromString(resourceTypeStr);
    }
  }
}
