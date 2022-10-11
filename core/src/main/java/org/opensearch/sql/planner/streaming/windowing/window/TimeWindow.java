/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.window;

import java.util.Date;
import lombok.Data;

/**
 * A time window is a window of time interval with inclusive start time and exclusive end time.
 */
@Data
public class TimeWindow implements Window {

  private final long startTime;

  private final long endTime;

  @Override
  public long startTimestamp() {
    return startTime;
  }

  @Override
  public long maxTimestamp() {
    return endTime - 1;
  }

  @Override
  public String toString() {
    return "TimeWindow{"
        + "startTime=" + new Date(startTime)
        + ", endTime=" + new Date(endTime)
        + '}';
  }
}
