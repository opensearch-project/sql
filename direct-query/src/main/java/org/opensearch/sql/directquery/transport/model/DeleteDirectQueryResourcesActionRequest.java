/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.directquery.rest.model.DeleteDirectQueryResourcesRequest;

/*
 * @opensearch.experimental
 */
@Getter
public class DeleteDirectQueryResourcesActionRequest extends ActionRequest {
  private final DeleteDirectQueryResourcesRequest directQueryRequest;

  public DeleteDirectQueryResourcesActionRequest(
      DeleteDirectQueryResourcesRequest directQueryRequest) {
    super();
    this.directQueryRequest = directQueryRequest;
  }

  public DeleteDirectQueryResourcesActionRequest(StreamInput in) throws IOException {
    DeleteDirectQueryResourcesRequest request = new DeleteDirectQueryResourcesRequest();
    request.setDataSource(in.readOptionalString());
    String resourceTypeStr = in.readOptionalString();
    if (resourceTypeStr != null) {
      request.setResourceTypeFromString(resourceTypeStr);
    }
    request.setNamespace(in.readOptionalString());
    request.setGroupName(in.readOptionalString());
    this.directQueryRequest = request;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeOptionalString(directQueryRequest.getDataSource());
    out.writeOptionalString(
        directQueryRequest.getResourceType() != null
            ? directQueryRequest.getResourceType().name()
            : null);
    out.writeOptionalString(directQueryRequest.getNamespace());
    out.writeOptionalString(directQueryRequest.getGroupName());
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
