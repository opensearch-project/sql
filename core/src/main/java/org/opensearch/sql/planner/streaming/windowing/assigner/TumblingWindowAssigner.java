/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import java.util.Collections;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.streaming.windowing.Window;

/**
 * A tumbling window assigner assigns a single window per event timestamp
 * without overlap.
 */
@RequiredArgsConstructor
public class TumblingWindowAssigner implements WindowAssigner {

  /** Window size in millisecond. */
  private final long windowSize;

  @Override
  public List<Window> assign(long timestamp) {
    long startTime = timestamp - timestamp % windowSize;
    return Collections.singletonList(new Window(startTime, startTime + windowSize));
  }
}
