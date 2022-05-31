/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import org.opensearch.action.ActionResponse;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;

public class TransportPPLQueryResponse extends ActionResponse implements ToXContentObject {
  public static final String PPL_QUERY_RESPONSE = "result";
  private final String result;

  public TransportPPLQueryResponse(String result) {
    this.result = result;
  }

  public TransportPPLQueryResponse(StreamInput in) throws IOException {
    super(in);
    result = in.readString();
  }

  public String result() {
    return result;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeString(result);
  }

  @Override
  public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
    builder.startObject();
    builder.field(PPL_QUERY_RESPONSE, result);
    builder.endObject();
    return builder;
  }
}
