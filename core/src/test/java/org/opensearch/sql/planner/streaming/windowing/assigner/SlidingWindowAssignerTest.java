/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.streaming.windowing.Window;

class SlidingWindowAssignerTest {

  @Test
  void testAssignWindows() {
    long windowSize = 1000;
    long slideSize = 500;
    SlidingWindowAssigner assigner = new SlidingWindowAssigner(windowSize, slideSize);

    assertEquals(List.of(new Window(0, 1000), new Window(500, 1500)), assigner.assign(500));

    assertEquals(List.of(new Window(0, 1000), new Window(500, 1500)), assigner.assign(999));

    assertEquals(List.of(new Window(500, 1500), new Window(1000, 2000)), assigner.assign(1000));
  }

  @Test
  void testConstructWithIllegalArguments() {
    IllegalArgumentException error1 =
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowAssigner(-1, 100));
    assertEquals("Window size [-1] must be positive number", error1.getMessage());

    IllegalArgumentException error2 =
        assertThrows(IllegalArgumentException.class, () -> new SlidingWindowAssigner(1000, 0));
    assertEquals("Slide size [0] must be positive number", error2.getMessage());
  }
}
