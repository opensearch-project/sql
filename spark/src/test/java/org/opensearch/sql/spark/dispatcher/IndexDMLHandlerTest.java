/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.dispatcher;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class IndexDMLHandlerTest {
  @Test
  public void getResponseFromExecutor() {
    assertThrows(
        IllegalStateException.class,
        () ->
            new IndexDMLHandler(null, null, null, null, null, null, null)
                .getResponseFromExecutor(null));
  }
}
