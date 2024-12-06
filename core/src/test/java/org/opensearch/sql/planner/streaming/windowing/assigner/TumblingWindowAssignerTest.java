/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.streaming.windowing.Window;

class TumblingWindowAssignerTest {

  @Test
  void testAssignWindow() {
    long windowSize = 1000;
    TumblingWindowAssigner assigner = new TumblingWindowAssigner(windowSize);

    assertEquals(singletonList(new Window(0, 1000)), assigner.assign(500));
    assertEquals(singletonList(new Window(1000, 2000)), assigner.assign(1999));
    assertEquals(singletonList(new Window(2000, 3000)), assigner.assign(2000));
  }

  @Test
  void testConstructWithIllegalWindowSize() {
    IllegalArgumentException error =
        assertThrows(IllegalArgumentException.class, () -> new TumblingWindowAssigner(-1));
    assertEquals("Window size [-1] must be positive number", error.getMessage());
  }
}
