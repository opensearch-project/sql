/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.streaming.windowing.assigner;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.streaming.windowing.Window;

/**
 * A window assigner assigns zero or more window to a value
 * based on different windowing approach.
 */
public interface WindowAssigner {

  /**
   * Return window(s) assigned to the given value.
   *
   * @param value field value extracted out of each tuple
   * @return windows assigned
   */
  List<Window> assign(ExprValue value);

}
