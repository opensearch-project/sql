/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.watermark;

import java.util.Date;
import org.opensearch.sql.data.model.ExprLongValue;

/**
 * Watermark control event.
 */
public class WatermarkEvent extends ExprLongValue {

  public WatermarkEvent(long timestamp) {
    super(timestamp);
  }

  @Override
  public String toString() {
    return "Watermark={" + new Date(longValue()) + "}";
  }
}
