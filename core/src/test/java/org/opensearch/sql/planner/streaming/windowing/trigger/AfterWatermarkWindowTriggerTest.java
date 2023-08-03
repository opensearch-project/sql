/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.trigger;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.streaming.StreamContext;
import org.opensearch.sql.planner.streaming.windowing.Window;

class AfterWatermarkWindowTriggerTest {

  private final StreamContext context = new StreamContext();

  private final AfterWatermarkWindowTrigger trigger = new AfterWatermarkWindowTrigger(context);

  @Test
  void shouldNotFireWindowAboveWatermark() {
    context.setWatermark(999);
    assertEquals(TriggerResult.CONTINUE, trigger.trigger(new Window(500, 1500)));
    assertEquals(TriggerResult.CONTINUE, trigger.trigger(new Window(500, 1001)));
    assertEquals(TriggerResult.CONTINUE, trigger.trigger(new Window(1000, 1500)));
  }

  @Test
  void shouldFireWindowBelowWatermark() {
    context.setWatermark(999);
    assertEquals(TriggerResult.FIRE, trigger.trigger(new Window(500, 800)));
    assertEquals(TriggerResult.FIRE, trigger.trigger(new Window(500, 1000)));
  }
}
