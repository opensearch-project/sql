/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.model;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.sql.spark.rest.model.CreateAsyncQueryRequest;

public class CreateAsyncQueryActionRequest extends ActionRequest {

  @Getter private CreateAsyncQueryRequest createAsyncQueryRequest;

  /** Constructor of CreateJobActionRequest from StreamInput. */
  public CreateAsyncQueryActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public CreateAsyncQueryActionRequest(CreateAsyncQueryRequest createAsyncQueryRequest) {
    this.createAsyncQueryRequest = createAsyncQueryRequest;
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
