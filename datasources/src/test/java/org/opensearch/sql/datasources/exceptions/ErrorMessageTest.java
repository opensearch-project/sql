/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.datasources.exceptions;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.opensearch.core.rest.RestStatus;

class ErrorMessageTest {

  @Test
  void toString_returnPrettyPrintedJson() {
    ErrorMessage errorMessage =
        new ErrorMessage(new RuntimeException(), RestStatus.TOO_MANY_REQUESTS.getStatus());

    assertEquals(
        "{\n"
            + "  \"status\": 429,\n"
            + "  \"error\": {\n"
            + "    \"type\": \"RuntimeException\",\n"
            + "    \"reason\": \"Too Many Requests\",\n"
            + "    \"details\": \"\"\n"
            + "  }\n"
            + "}",
        errorMessage.toString());
  }

  @Test
  void getReason() {
    testGetReason(RestStatus.TOO_MANY_REQUESTS, "Too Many Requests");
    testGetReason(RestStatus.BAD_REQUEST, "Invalid Request");
    // other status
    testGetReason(RestStatus.BAD_GATEWAY, "There was internal problem at backend");
  }

  void testGetReason(RestStatus status, String expectedReason) {
    ErrorMessage errorMessage = new ErrorMessage(new RuntimeException(), status.getStatus());

    assertEquals(expectedReason, errorMessage.getReason());
  }
}
