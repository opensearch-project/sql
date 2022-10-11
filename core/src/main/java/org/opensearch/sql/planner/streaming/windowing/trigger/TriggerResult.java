/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.trigger;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Trigger result.
 */
@Getter
@RequiredArgsConstructor
public enum TriggerResult {
  CONTINUE(false),
  FIRE(true);

  private final boolean fire;
}
