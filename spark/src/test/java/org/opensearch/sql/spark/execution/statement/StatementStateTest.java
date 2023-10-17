/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import static org.junit.Assert.assertThrows;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StatementStateTest {
  @Test
  public void invalidStatementState() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> StatementState.fromString("invalid"));
    Assertions.assertEquals("Invalid statement state: invalid", exception.getMessage());
  }
}
