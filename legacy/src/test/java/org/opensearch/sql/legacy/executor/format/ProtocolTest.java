/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import org.junit.Test;
import org.opensearch.OpenSearchSecurityException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.core.rest.RestStatus;

public class ProtocolTest {

  @Test
  public void joinFailureWrappingClientErrorKeepsItsStatus() {
    Protocol protocol =
        new Protocol(
            new IllegalStateException(
                "Error occurred during join query run",
                new RuntimeException(
                    "Error occurred while creating PIT.",
                    new ExecutionException(
                        new OpenSearchSecurityException(
                            "no permissions for [indices:data/read/search]",
                            RestStatus.FORBIDDEN)))));

    assertEquals(403, protocol.getStatus());
  }

  @Test
  public void wrappedTooManyRequestsStaysInternalServerError() {
    Protocol protocol =
        new Protocol(
            new IllegalStateException(
                new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS)));

    assertEquals(500, protocol.getStatus());
  }
}
