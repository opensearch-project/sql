/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import org.junit.jupiter.api.Test;

public class OpenSearchRequestTest {

  @Test
  void toCursor() {
    var request = mock(OpenSearchRequest.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    assertEquals("", request.toCursor());
  }
}
