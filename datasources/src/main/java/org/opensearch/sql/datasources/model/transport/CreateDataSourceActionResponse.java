/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.datasources.model.transport;

import java.io.IOException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

@RequiredArgsConstructor
public class CreateDataSourceActionResponse
    extends ActionResponse {

  @Getter
  private final String result;

  public CreateDataSourceActionResponse(StreamInput in) throws IOException {
    super(in);
    result = in.readString();
  }

  @Override
  public void writeTo(StreamOutput streamOutput) throws IOException {
    streamOutput.writeString(result);
  }
}
