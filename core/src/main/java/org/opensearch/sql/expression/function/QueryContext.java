/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.expression.function;

import java.time.Clock;

/**
 * Class to capture values that may be necessary to implement some functions.
 * An example would be query execution start time to implement now().
 */
public interface QueryContext {
  /**
   * Method to get time when query began execution.
   * @return a clock provided at the start of query execution.
   */
  Clock getQueryStartTime();
}
