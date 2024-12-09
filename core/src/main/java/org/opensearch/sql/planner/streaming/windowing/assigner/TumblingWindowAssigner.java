/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import com.google.common.base.Preconditions;
import java.util.List;
import org.opensearch.sql.planner.streaming.windowing.Window;
import org.opensearch.sql.utils.DateTimeUtils;

/** A tumbling window assigner assigns a single window per event timestamp without overlap. */
public class TumblingWindowAssigner implements WindowAssigner {

  /** Window size in millisecond. */
  private final long windowSize;

  /**
   * Create tumbling window assigner with the given window size.
   *
   * @param windowSize window size in millisecond
   */
  public TumblingWindowAssigner(long windowSize) {
    Preconditions.checkArgument(
        windowSize > 0, "Window size [%s] must be positive number", windowSize);
    this.windowSize = windowSize;
  }

  @Override
  public List<Window> assign(long timestamp) {
    long startTime = DateTimeUtils.getWindowStartTime(timestamp, windowSize);
    return List.of(new Window(startTime, startTime + windowSize));
  }
}
