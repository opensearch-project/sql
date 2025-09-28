/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/*
 * @opensearch.experimental
 */
@Data
@NoArgsConstructor
public class WriteDirectQueryResourcesRequest {
  private String dataSource;
  private DirectQueryResourceType resourceType;
  private String resourceName;
  private String request;
  private Map<String, String> RequestOptions;

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
