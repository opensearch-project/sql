/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.rest.model;

import lombok.Data;
import lombok.NoArgsConstructor;

/*
 * @opensearch.experimental
 */
@Data
@NoArgsConstructor
public class DeleteDirectQueryResourcesRequest {
  private String dataSource;
  private DirectQueryResourceType resourceType;
  private String namespace;
  private String groupName;

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
