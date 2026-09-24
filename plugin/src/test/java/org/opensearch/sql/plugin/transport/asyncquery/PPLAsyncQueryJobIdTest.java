/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport.asyncquery;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class PPLAsyncQueryJobIdTest {

  @Test
  public void roundTripsOwnerAndContext() {
    PPLAsyncQueryJobId id = PPLAsyncQueryJobId.create("node-a");

    assertEquals(id, PPLAsyncQueryJobId.parse(id.encode()));
  }

  @Test
  public void rejectsMalformedIds() {
    assertThrows(IllegalArgumentException.class, () -> PPLAsyncQueryJobId.parse(""));
    assertThrows(IllegalArgumentException.class, () -> PPLAsyncQueryJobId.parse("not-an-id"));
    assertThrows(IllegalArgumentException.class, () -> new PPLAsyncQueryJobId("", "context-id"));
  }
}
