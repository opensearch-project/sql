/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.directquery.transport.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/*
 * @opensearch.experimental
 */
@Getter
@RequiredArgsConstructor
public class WriteDirectQueryResourcesActionResponse extends ActionResponse {

  private final String result;

  public WriteDirectQueryResourcesActionResponse(StreamInput in) throws IOException {
    super(in);
    result = in.readString();
  }

  @Override
  public void writeTo(StreamOutput streamOutput) throws IOException {
    streamOutput.writeString(result);
  }
}
