/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.asyncquery;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class DummyTest {
  @Test
  public void test() {
    Dummy dummy = new Dummy();
    assertEquals("Hello!", dummy.hello());
  }
}
