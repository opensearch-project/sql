/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.transport.model;

import java.io.IOException;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.sql.spark.rest.model.CreateJobRequest;

public class CreateJobActionRequest extends ActionRequest {

  @Getter private CreateJobRequest createJobRequest;

  /** Constructor of CreateJobActionRequest from StreamInput. */
  public CreateJobActionRequest(StreamInput in) throws IOException {
    super(in);
  }

  public CreateJobActionRequest(CreateJobRequest createJobRequest) {
    this.createJobRequest = createJobRequest;
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
