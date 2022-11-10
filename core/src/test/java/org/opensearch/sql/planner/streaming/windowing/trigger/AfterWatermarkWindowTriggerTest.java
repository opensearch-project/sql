/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.trigger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.data.model.ExprValueUtils.integerValue;
import static org.opensearch.sql.expression.DSL.window;
import static org.opensearch.sql.planner.streaming.windowing.trigger.TriggerResult.CONTINUE;
import static org.opensearch.sql.planner.streaming.windowing.trigger.TriggerResult.FIRE;

import org.junit.jupiter.api.Test;
import org.opensearch.sql.planner.streaming.StreamContext;

class AfterWatermarkWindowTriggerTest {

  private final StreamContext context = new StreamContext();

  private final AfterWatermarkWindowTrigger trigger = new AfterWatermarkWindowTrigger(context);

  @Test
  void shouldNotFireWindowAboveWatermark() {
    context.setWatermark(integerValue(999));
    assertEquals(CONTINUE, trigger.trigger(window(500, 1500)));
    assertEquals(CONTINUE, trigger.trigger(window(500, 1000)));
    assertEquals(CONTINUE, trigger.trigger(window(1000, 1500)));
  }

  @Test
  void shouldFireWindowBelowWatermark() {
    context.setWatermark(integerValue(999));
    assertEquals(FIRE, trigger.trigger(window(500, 800)));
    assertEquals(FIRE, trigger.trigger(window(500, 999)));
  }
}