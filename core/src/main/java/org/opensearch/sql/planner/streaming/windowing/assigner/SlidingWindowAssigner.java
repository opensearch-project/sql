/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import com.google.common.base.Preconditions;
import java.util.LinkedList;
import java.util.List;
import org.opensearch.sql.planner.streaming.windowing.Window;
import org.opensearch.sql.utils.DateTimeUtils;

/**
 * A sliding window assigner assigns multiple overlapped window per event timestamp. The overlap
 * size is determined by the given slide interval.
 */
public class SlidingWindowAssigner implements WindowAssigner {

  /** Window size in millisecond. */
  private final long windowSize;

  /** Slide size in millisecond. */
  private final long slideSize;

  /**
   * Create sliding window assigner with the given window and slide size in millisecond.
   *
   * @param windowSize window size in millisecond
   * @param slideSize slide size in millisecond
   */
  public SlidingWindowAssigner(long windowSize, long slideSize) {
    Preconditions.checkArgument(
        windowSize > 0, "Window size [%s] must be positive number", windowSize);
    Preconditions.checkArgument(
        slideSize > 0, "Slide size [%s] must be positive number", slideSize);
    this.windowSize = windowSize;
    this.slideSize = slideSize;
  }

  @Override
  public List<Window> assign(long timestamp) {
    LinkedList<Window> windows = new LinkedList<>();

    // Assign window from the last start time to the first until timestamp outside current window
    long startTime = DateTimeUtils.getWindowStartTime(timestamp, slideSize);
    for (Window win = window(startTime); win.maxTimestamp() >= timestamp; win = window(startTime)) {
      windows.addFirst(win);
      startTime -= slideSize;
    }
    return windows;
  }

  private Window window(long startTime) {
    return new Window(startTime, startTime + windowSize);
  }
}
