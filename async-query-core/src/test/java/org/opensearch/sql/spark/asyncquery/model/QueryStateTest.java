/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.asyncquery.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

class QueryStateTest {
  @Test
  public void testFromString() {
    assertEquals(QueryState.WAITING, QueryState.fromString("waiting"));
    assertEquals(QueryState.RUNNING, QueryState.fromString("running"));
    assertEquals(QueryState.SUCCESS, QueryState.fromString("success"));
    assertEquals(QueryState.FAILED, QueryState.fromString("failed"));
    assertEquals(QueryState.CANCELLED, QueryState.fromString("cancelled"));
    assertEquals(QueryState.TIMEOUT, QueryState.fromString("timeout"));
  }

  @Test
  public void testFromStringWithUnknownState() {
    assertThrows(IllegalArgumentException.class, () -> QueryState.fromString("UNKNOWN_STATE"));
  }
}
