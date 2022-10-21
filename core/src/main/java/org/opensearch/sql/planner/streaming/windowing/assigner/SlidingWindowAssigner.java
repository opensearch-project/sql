/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.opensearch.sql.planner.streaming.windowing.Window;

/**
 * A sliding window assigner assigns multiple overlapped window per event timestamp.
 * The overlap size is determined by the given slide interval.
 */
public class SlidingWindowAssigner implements WindowAssigner {

  /** Window size in millisecond. */
  private final long windowSize;

  /** Slide size in millisecond. */
  private final long slideSize;

  /**
   * Create sliding window assigner with the given window and slide size.
   */
  public SlidingWindowAssigner(long windowSize, long slideSize) {
    Preconditions.checkArgument(windowSize > 0,
        "Window size [%s] must be positive number", windowSize);
    Preconditions.checkArgument(slideSize > 0,
        "Slide size [%s] must be positive number", slideSize);
    this.windowSize = windowSize;
    this.slideSize = slideSize;
  }

  @Override
  public List<Window> assign(long timestamp) {
    List<Window> windows = new ArrayList<>();

    // Assign window from the last start time to first until given timestamp outside current window
    long startTime = timestamp - timestamp % slideSize;
    for (Window win = window(startTime); win.maxTimestamp() >= timestamp; win = window(startTime)) {
      windows.add(win);
      startTime -= slideSize;
    }

    // Reverse the window list for easy read and test
    Collections.reverse(windows);
    return windows;
  }

  private Window window(long startTime) {
    return new Window(startTime, startTime + windowSize);
  }
}
