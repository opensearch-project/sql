/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.response.error;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.core.rest.RestStatus.BAD_REQUEST;
import static org.opensearch.core.rest.RestStatus.SERVICE_UNAVAILABLE;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ErrorMessageTest {

  @Test
  public void testToString() {
    ErrorMessage errorMessage =
        new ErrorMessage(new IllegalStateException("illegal state"),
            SERVICE_UNAVAILABLE.getStatus());
    assertEquals("{\n"
        + "  \"error\": {\n"
        + "    \"reason\": \"There was internal problem at backend\",\n"
        + "    \"details\": \"illegal state\",\n"
        + "    \"type\": \"IllegalStateException\"\n"
        + "  },\n"
        + "  \"status\": 503\n"
        + "}", errorMessage.toString());
  }

  @Test
  public void testBadRequestToString() {
    ErrorMessage errorMessage =
        new ErrorMessage(new IllegalStateException(),
            BAD_REQUEST.getStatus());
    assertEquals("{\n"
        + "  \"error\": {\n"
        + "    \"reason\": \"Invalid Query\",\n"
        + "    \"details\": \"\",\n"
        + "    \"type\": \"IllegalStateException\"\n"
        + "  },\n"
        + "  \"status\": 400\n"
        + "}", errorMessage.toString());
  }

  @Test
  public void testToStringWithEmptyErrorMessage() {
    ErrorMessage errorMessage =
        new ErrorMessage(new IllegalStateException(),
            SERVICE_UNAVAILABLE.getStatus());
    assertEquals("{\n"
        + "  \"error\": {\n"
        + "    \"reason\": \"There was internal problem at backend\",\n"
        + "    \"details\": \"\",\n"
        + "    \"type\": \"IllegalStateException\"\n"
        + "  },\n"
        + "  \"status\": 503\n"
        + "}", errorMessage.toString());
  }
}
