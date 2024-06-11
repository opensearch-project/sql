/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.jupiter.api.Test;

class SessionStateTest {
  @Test
  public void invalidSessionType() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> SessionState.fromString("invalid"));
    assertEquals("Invalid session state: invalid", exception.getMessage());
  }
}
