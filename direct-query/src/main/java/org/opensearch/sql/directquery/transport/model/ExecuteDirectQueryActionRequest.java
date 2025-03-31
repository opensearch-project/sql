/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import java.io.IOException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.directquery.rest.model.ExecuteDirectQueryRequest;

public class ExecuteDirectQueryActionRequest extends ActionRequest {
  private final ExecuteDirectQueryRequest directQueryRequest;

  public ExecuteDirectQueryActionRequest(ExecuteDirectQueryRequest directQueryRequest) {
    this.directQueryRequest = directQueryRequest;
  }

  public ExecuteDirectQueryActionRequest(StreamInput in) throws IOException {
    super(in);
    // In a real implementation, deserialize the request
    // This is just a placeholder since we don't have the full serialization code
    this.directQueryRequest = new ExecuteDirectQueryRequest();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    // Add serialization logic if needed
  }

  public ExecuteDirectQueryRequest getDirectQueryRequest() {
    return directQueryRequest;
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
