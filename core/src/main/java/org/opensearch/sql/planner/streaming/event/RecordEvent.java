/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.event;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;

/**
 * An event ...
 */
@Getter
@RequiredArgsConstructor
public class RecordEvent implements Event {

  private final long timestamp;

  /**
   * Original data record.
   */
  private final ExprValue data;

  private final StreamContext context;
}
