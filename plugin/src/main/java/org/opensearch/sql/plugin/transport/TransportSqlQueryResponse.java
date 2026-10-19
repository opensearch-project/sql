/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Completion signal for {@link SqlQueryAction}. The actual SQL response is written directly to the
 * REST channel by the execution path; this response only marks that the coordinator task can be
 * unregistered, so it carries no payload.
 */
public class TransportSqlQueryResponse extends ActionResponse {

  public TransportSqlQueryResponse() {}

  public TransportSqlQueryResponse(StreamInput in) throws IOException {
    super(in);
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    // No payload: the SQL result is delivered via the REST channel, not this transport response.
  }
}
