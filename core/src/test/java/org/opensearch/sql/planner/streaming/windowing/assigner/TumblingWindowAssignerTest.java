/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.streaming.windowing.Window;

class TumblingWindowAssignerTest {

  @Test
  void testAssign() {
    long windowSize = 1000;
    TumblingWindowAssigner assigner = new TumblingWindowAssigner(windowSize);

    assertEquals(
        Collections.singletonList(new Window(0, windowSize)),
        assigner.assign(500));
    assertEquals(
        Collections.singletonList(new Window(1000, 1000 + windowSize)),
        assigner.assign(1999));
    assertEquals(
        Collections.singletonList(new Window(2000, 2000 + windowSize)),
        assigner.assign(2000));
  }
}