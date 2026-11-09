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

public class CancelAsyncQueryActionRequestTest {

  @Test
  public void streamRoundTrip_preservesQueryId() throws IOException {
    assertEquals("job-xyz", roundTrip(new CancelAsyncQueryActionRequest("job-xyz")).getQueryId());
  }

  @Test
  public void streamRoundTrip_preservesNullQueryId() throws IOException {
    assertNull(roundTrip(new CancelAsyncQueryActionRequest((String) null)).getQueryId());
  }

  private static CancelAsyncQueryActionRequest roundTrip(CancelAsyncQueryActionRequest original)
      throws IOException {
    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);
    out.flush();
    try (BytesStreamInput in = new BytesStreamInput(out.bytes().toBytesRef().bytes)) {
      return new CancelAsyncQueryActionRequest(in);
    }
  }
}
