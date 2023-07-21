/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.planner.streaming.windowing.trigger;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * Result determined by a trigger for what should happen to the window.
 */
@Getter
@RequiredArgsConstructor
public enum TriggerResult {

  /** Continue without any operation. */
  CONTINUE(false, false),

  /** Fire and purge window state by default. */
  FIRE(true, true);

  /** If window should be fired to output. */
  private final boolean fire;

  /** If the window state should be discarded. */
  private final boolean purge;
}
