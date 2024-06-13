/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.jupiter.api.Test;

class SessionTypeTest {
  @Test
  public void invalidSessionType() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> SessionType.fromString("invalid"));
    assertEquals("Invalid session type: invalid", exception.getMessage());
  }
}
