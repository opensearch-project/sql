/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.format;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ExecutionException;
import org.json.JSONObject;
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
  public void wrappedTooManyRequestsKeepsItsStatusInResponseAndBody() {
    Protocol protocol =
        new Protocol(
            new IllegalStateException(
                new OpenSearchStatusException("busy", RestStatus.TOO_MANY_REQUESTS)));

    assertEquals(429, protocol.getStatus());
    assertEquals(429, new JSONObject(protocol.format()).getInt("status"));
  }

  @Test
  public void wrappedServiceUnavailableKeepsItsStatusInResponseAndBody() {
    Protocol protocol =
        new Protocol(
            new RuntimeException(
                "Error occurred while creating PIT.",
                new ExecutionException(
                    new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE))));

    assertEquals(503, protocol.getStatus());
    assertEquals(503, new JSONObject(protocol.format()).getInt("status"));
  }

  @Test
  public void illegalArgumentWrappingOpenSearchErrorUsesItsStatus() {
    // The body is built from the OpenSearch exception, so the 400 rule must not apply.
    Protocol protocol =
        new Protocol(
            new IllegalArgumentException(
                new OpenSearchStatusException("unavailable", RestStatus.SERVICE_UNAVAILABLE)));

    assertEquals(503, protocol.getStatus());
    assertEquals(503, new JSONObject(protocol.format()).getInt("status"));
  }

  @Test
  public void illegalArgumentWithoutOpenSearchErrorIsBadRequest() {
    Protocol protocol = new Protocol(new IllegalArgumentException("bad argument"));

    assertEquals(400, protocol.getStatus());
    assertEquals(400, new JSONObject(protocol.format()).getInt("status"));
  }

  @Test
  public void otherFailureWithoutOpenSearchErrorIsInternalServerError() {
    Protocol protocol = new Protocol(new IllegalStateException("broken"));

    assertEquals(500, protocol.getStatus());
  }
}
