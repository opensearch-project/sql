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
  void fetchReason() {
    ErrorMessage errorMessage =
        new ErrorMessage(new RuntimeException(), RestStatus.TOO_MANY_REQUESTS.getStatus());
    assertEquals("Too Many Requests", errorMessage.getReason());
  }
}
