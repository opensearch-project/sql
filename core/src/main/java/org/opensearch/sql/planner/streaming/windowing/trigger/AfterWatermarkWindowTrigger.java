/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.trigger;

import static org.opensearch.sql.planner.streaming.windowing.Window.UNBOUND;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.streaming.StreamContext;
import org.opensearch.sql.planner.streaming.windowing.Window;

/**
 * After watermark window trigger fires window state output once a window is below watermark.
 * Precisely speaking, after watermark means the window boundary (max timestamp) is equal to
 * or less than the current watermark timestamp.
 */
@RequiredArgsConstructor
public class AfterWatermarkWindowTrigger implements WindowTrigger {

  /** Stream context that contains the current watermark. */
  private final StreamContext context;

  @Override
  public TriggerResult trigger(Window window) {
    // Fire if window (exclusive upper bound) <= watermark
    if (isWindowBelowWatermark(window)) {
      return TriggerResult.FIRE;
    }
    return TriggerResult.CONTINUE;
  }

  private boolean isWindowBelowWatermark(Window window) {
    return window.compareTo(new Window(UNBOUND, context.getWatermark())) <= 0;
  }
}
