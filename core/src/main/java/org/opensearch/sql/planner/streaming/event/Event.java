/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.event;

import org.opensearch.sql.data.model.ExprValue;

/**
 * Event propagates through a pipeline.
 */
public interface Event {

  long getTimestamp();

  /**
   * Get data payload.
   */
  ExprValue getData();
}
