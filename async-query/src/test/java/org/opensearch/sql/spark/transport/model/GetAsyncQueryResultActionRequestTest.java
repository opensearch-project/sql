/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.transport.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BytesStreamInput;

public class GetAsyncQueryResultActionRequestTest {

  @Test
  public void streamRoundTrip_preservesQueryId() throws IOException {
    GetAsyncQueryResultActionRequest original = new GetAsyncQueryResultActionRequest("job-xyz");

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    out.flush();

    try (BytesStreamInput in = new BytesStreamInput(out.bytes().toBytesRef().bytes)) {
      GetAsyncQueryResultActionRequest roundTripped = new GetAsyncQueryResultActionRequest(in);
      assertEquals("job-xyz", roundTripped.getQueryId());
    }
  }

  @Test
  public void streamRoundTrip_preservesNullQueryId() throws IOException {
    GetAsyncQueryResultActionRequest original = new GetAsyncQueryResultActionRequest((String) null);

    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    out.flush();

    try (BytesStreamInput in = new BytesStreamInput(out.bytes().toBytesRef().bytes)) {
      GetAsyncQueryResultActionRequest roundTripped = new GetAsyncQueryResultActionRequest(in);
      assertNull(roundTripped.getQueryId());
    }
  }
}
