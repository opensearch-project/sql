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
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;

@Getter
public class GetDirectQueryResourcesActionRequest extends ActionRequest {
  private final GetDirectQueryResourcesRequest directQueryRequest;

  public GetDirectQueryResourcesActionRequest(GetDirectQueryResourcesRequest directQueryRequest) {
    this.directQueryRequest = directQueryRequest;
  }

  public GetDirectQueryResourcesActionRequest(StreamInput in) throws IOException {
    super(in);
    GetDirectQueryResourcesRequest request = new GetDirectQueryResourcesRequest();
    request.setDataSource(in.readOptionalString());
    String resourceTypeStr = in.readOptionalString();
    if (resourceTypeStr != null) {
      request.setResourceTypeFromString(resourceTypeStr);
    }
    request.setResourceName(in.readOptionalString());
    request.setQueryParams(in.readMap(StreamInput::readString, StreamInput::readString));
    this.directQueryRequest = request;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeOptionalString(directQueryRequest.getDataSource());
    out.writeOptionalString(
        directQueryRequest.getResourceType() != null
            ? directQueryRequest.getResourceType().name()
            : null);
    out.writeOptionalString(directQueryRequest.getResourceName());
    out.writeMap(
        directQueryRequest.getQueryParams(), StreamOutput::writeString, StreamOutput::writeString);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
