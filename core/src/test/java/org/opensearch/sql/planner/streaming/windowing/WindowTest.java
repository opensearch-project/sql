/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.streaming.windowing;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class WindowTest {

  @Test
  void test() {
    Window window = new Window(1000, 2000);
    assertEquals(1000, window.getStartTime());
    assertEquals(2000, window.getEndTime());
    assertEquals(1999, window.maxTimestamp());
  }
}
