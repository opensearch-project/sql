/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.streaming.windowing;

import lombok.Data;

/**
 * A time window is a window of time interval with inclusive start time and exclusive end time.
 */
@Data
public class Window {

  /** Start timestamp (inclusive) of the time window. */
  private final long startTime;

  /** End timestamp (exclusive) of the time window. */
  private final long endTime;

  /**
   * Return the maximum timestamp (inclusive) of the window.
   */
  public long maxTimestamp() {
    return endTime - 1;
  }
}
