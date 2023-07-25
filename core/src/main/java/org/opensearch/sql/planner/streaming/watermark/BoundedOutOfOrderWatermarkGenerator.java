/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.watermark;

import lombok.RequiredArgsConstructor;

/** Watermark generator that generates watermark with bounded out-of-order delay. */
@RequiredArgsConstructor
public class BoundedOutOfOrderWatermarkGenerator implements WatermarkGenerator {

  /** The maximum out-of-order allowed in millisecond. */
  private final long maxOutOfOrderAllowed;

  /** The maximum timestamp seen so far in millisecond. */
  private long maxTimestamp;

  @Override
  public long generate(long timestamp) {
    maxTimestamp = Math.max(maxTimestamp, timestamp);
    return (maxTimestamp - maxOutOfOrderAllowed - 1);
  }
}
