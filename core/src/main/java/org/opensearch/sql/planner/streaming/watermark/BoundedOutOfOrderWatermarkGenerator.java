/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.watermark;

import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.streaming.event.Event;
import org.opensearch.sql.planner.streaming.time.TimestampAssigner;

/**
 * Watermark generator for bounded out-of-order data.
 */
public class BoundedOutOfOrderWatermarkGenerator extends WatermarkGenerator {

  /** The maximum timestamp seen so far. */
  private long maxTimestamp;

  /** The maximum out-of-order allowed. */
  private final long maxOutOfOrderAllowed = 2000 * 60; // TODO: hardcoding minute allowed

  public BoundedOutOfOrderWatermarkGenerator(PhysicalPlan input,
                                             TimestampAssigner timestampAssigner) {
    super(input, timestampAssigner);
  }

  @Override
  protected long onEvent(Event event) {
    maxTimestamp = Math.max(maxTimestamp, event.getTimestamp());
    return (maxTimestamp - maxOutOfOrderAllowed - 1); //TODO: emit watermark per record for now
  }
}
