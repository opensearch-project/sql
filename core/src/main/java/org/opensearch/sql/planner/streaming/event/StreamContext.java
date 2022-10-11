/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.event;

import lombok.Data;

/**
 * Event context.
 */
@Data
public class StreamContext {

  /**
   * Current watermark.
   */
  private long currentWatermark;
}
