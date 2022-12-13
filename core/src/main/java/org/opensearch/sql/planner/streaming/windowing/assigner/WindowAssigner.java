/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import java.util.List;
import org.opensearch.sql.planner.streaming.windowing.Window;

/**
 * A window assigner assigns zero or more window to an event timestamp
 * based on different windowing approach.
 */
public interface WindowAssigner {

  /**
   * Return window(s) assigned to the timestamp.
   * @param timestamp given event timestamp
   * @return windows assigned
   */
  List<Window> assign(long timestamp);

}
