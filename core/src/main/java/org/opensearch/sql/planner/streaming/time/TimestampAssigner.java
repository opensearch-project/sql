/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.time;

import org.opensearch.sql.data.model.ExprValue;

/**
 * Timestamp extractor.
 */
public interface TimestampAssigner {

  /**
   * Assign timestamp.
   */
  long assign(ExprValue value);
}
