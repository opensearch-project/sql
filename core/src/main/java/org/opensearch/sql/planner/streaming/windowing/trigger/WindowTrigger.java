/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.trigger;

import org.opensearch.sql.planner.streaming.windowing.Window;

/**
 * A window trigger determines if the current window state should be evaluated to emit output.
 */
public interface WindowTrigger {

  /**
   * Return trigger result for a window.
   *
   * @param window given window
   * @return trigger result
   */
  TriggerResult trigger(Window window);
}
