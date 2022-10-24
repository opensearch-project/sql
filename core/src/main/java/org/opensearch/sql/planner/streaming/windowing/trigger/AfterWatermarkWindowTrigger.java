/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.trigger;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.streaming.StreamContext;
import org.opensearch.sql.planner.streaming.windowing.Window;

/**
 * After watermark window trigger fires window state output once watermark.
 */
@RequiredArgsConstructor
public class AfterWatermarkWindowTrigger implements WindowTrigger {

  /** Stream context. */
  private final StreamContext context;

  @Override
  public TriggerResult trigger(Window window) {
    if (window.maxTimestamp() <= context.getWatermark()) {
      return TriggerResult.FIRE;
    }
    return TriggerResult.CONTINUE;
  }
}
