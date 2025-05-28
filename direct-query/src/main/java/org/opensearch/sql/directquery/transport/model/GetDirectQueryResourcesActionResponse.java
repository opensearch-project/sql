/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import java.io.IOException;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

@Getter
@RequiredArgsConstructor
public class GetDirectQueryResourcesActionResponse extends ActionResponse {

  private final String result;

  public GetDirectQueryResourcesActionResponse(StreamInput in) throws IOException {
    super(in);
    result = in.readString();
  }

  @Override
  public void writeTo(StreamOutput streamOutput) throws IOException {
    streamOutput.writeString(result);
  }
}
