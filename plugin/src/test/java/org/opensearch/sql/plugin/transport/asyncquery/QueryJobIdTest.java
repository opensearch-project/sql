/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class QueryJobIdTest {

  @Test
  public void roundTripsOwnerAndContext() {
    QueryJobId id = QueryJobId.create("node-a");

    assertEquals(id, QueryJobId.parse(id.encode()));
  }

  @Test
  public void rejectsMalformedIds() {
    assertThrows(IllegalArgumentException.class, () -> QueryJobId.parse(""));
    assertThrows(IllegalArgumentException.class, () -> QueryJobId.parse("not-an-id"));
    assertThrows(IllegalArgumentException.class, () -> new QueryJobId("", "context-id"));
  }
}
