/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import org.opensearch.sql.planner.streaming.event.RecordEvent;
import org.opensearch.sql.planner.streaming.windowing.window.Window;

/**
 * Assign an incoming element to a window.
 */
public interface WindowAssigner {

  /**
   * Return the window that should be assigned to the event.
   *
   * @param event an event
   * @return window
   */
  Window assign(RecordEvent event);
}
