/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.event;

import lombok.Data;
import org.opensearch.sql.planner.physical.collector.Collector;

/**
 * Event context.
 */
@Data
public class StreamContext {

  /**
   * Current watermark.
   */
  private long currentWatermark;

  /**
   * Collector with accumulated state.
   */
  private Collector collector;

  /**
   * Copy from another stream context.
   *
   * @param context stream context
   */
  public void copyFrom(StreamContext context) {
    this.currentWatermark = context.currentWatermark;
    this.collector = context.collector;
  }
}
