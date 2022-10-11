/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.trigger;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.streaming.event.RecordEvent;
import org.opensearch.sql.planner.streaming.event.StreamContext;
import org.opensearch.sql.planner.streaming.windowing.window.Window;

/**
 * A window trigger that determines trigger result based on event time.
 */
@RequiredArgsConstructor
public class EventTimeWindowTrigger implements WindowTrigger {

  private final StreamContext context;

  @Override
  public TriggerResult onWindow(Window window) {
    if (window.maxTimestamp() <= context.getCurrentWatermark()) {
      return TriggerResult.FIRE;
    } else {
      return TriggerResult.CONTINUE;
    }
  }
}
