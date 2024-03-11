/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.execution.statement;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class StatementStateTest {
  @Test
  public void invalidStatementState() {
    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> StatementState.fromString("invalid"));
    Assertions.assertEquals("Invalid statement state: invalid", exception.getMessage());
  }

  @Test
  public void notCancellableStatementState() {
    assertTrue(StatementState.NOT_CANCELLABLE_STATE.contains(StatementState.CANCELLED));
    assertTrue(StatementState.NOT_CANCELLABLE_STATE.contains(StatementState.SUCCESS));
    assertTrue(StatementState.NOT_CANCELLABLE_STATE.contains(StatementState.FAILED));
  }
}
