/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.window;

/**
 * A window is ...
 */
public interface Window {

  /**
   * Get lower bound of current window.
   *
   * @return the start timestamp of the window
   */
  long startTimestamp();

  /**
   * Get the upper bound of current window.
   *
   * @return the largest timestamp within the window
   */
  long maxTimestamp();
}
