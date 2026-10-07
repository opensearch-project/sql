/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.model;

import java.io.IOException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

@AllArgsConstructor
@Getter
public class CancelAsyncQueryActionRequest extends ActionRequest {

  private String queryId;

  /** Constructor of SubmitJobActionRequest from StreamInput. */
  public CancelAsyncQueryActionRequest(StreamInput in) throws IOException {
    super(in);
    this.queryId = in.readOptionalString();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeOptionalString(queryId);
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }
}
