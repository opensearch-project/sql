/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.streaming.event.RecordEvent;
import org.opensearch.sql.planner.streaming.windowing.window.TimeWindow;
import org.opensearch.sql.planner.streaming.windowing.window.Window;

/**
 * A tumbling event time window assigner assigns element based on element's
 * event timestamp to window without overlap.
 */
@RequiredArgsConstructor
public class TumblingEventTimeWindowAssigner implements WindowAssigner {

  private final long windowSize;

  @Override
  public Window assign(RecordEvent event) {
    long start = event.getTimestamp() - event.getTimestamp() % windowSize;
    return new TimeWindow(start, start + windowSize);
  }
}
