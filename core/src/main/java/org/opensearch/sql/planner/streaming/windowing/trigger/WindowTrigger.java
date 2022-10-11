/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.trigger;

import org.opensearch.sql.planner.streaming.windowing.window.Window;

/**
 * A window trigger determines if the current window pane should be evaluated to emit results.
 */
public interface WindowTrigger {

  /**
   * Determine the trigger result of an event.
   *
   * @param event an event
   * @return trigger result
   */
  TriggerResult onWindow(Window window);

}
