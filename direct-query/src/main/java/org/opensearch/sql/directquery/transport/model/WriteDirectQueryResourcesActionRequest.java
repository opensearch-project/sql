/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.directquery.rest.model.GetDirectQueryResourcesRequest;
import org.opensearch.sql.directquery.rest.model.WriteDirectQueryResourcesRequest;

import java.io.IOException;
import java.util.Collections;

/*
 * @opensearch.experimental
 */
@Getter
public class WriteDirectQueryResourcesActionRequest extends ActionRequest {
  private final WriteDirectQueryResourcesRequest directQueryRequest;

  public WriteDirectQueryResourcesActionRequest(WriteDirectQueryResourcesRequest directQueryRequest) {
    super();
    this.directQueryRequest = directQueryRequest;
  }

  public WriteDirectQueryResourcesActionRequest(StreamInput in) throws IOException {
    WriteDirectQueryResourcesRequest request = new WriteDirectQueryResourcesRequest();
    request.setDataSource(in.readOptionalString());
    String resourceTypeStr = in.readOptionalString();
    if (resourceTypeStr != null) {
      request.setResourceTypeFromString(resourceTypeStr);
    }
    request.setResourceName(in.readOptionalString());
    request.setRequest(in.readOptionalString());
    request.setRequestOptions(in.readMap(StreamInput::readString, StreamInput::readString));
    this.directQueryRequest = request;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeOptionalString(directQueryRequest.getDataSource());
    out.writeOptionalString(
        directQueryRequest.getResourceType() != null
            ? directQueryRequest.getResourceType().name()
            : null);
    out.writeOptionalString(directQueryRequest.getResourceName());
    out.writeOptionalString(directQueryRequest.getRequest());
    if (directQueryRequest.getRequestOptions() != null) {
      out.writeMap(
          directQueryRequest.getRequestOptions(), StreamOutput::writeString, StreamOutput::writeString);
    } else {
      out.writeMap(
          Collections.emptyMap(), StreamOutput::writeString, StreamOutput::writeString);
    }
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
