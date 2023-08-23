/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.resource.monitor;

/** Interface for different monitor component */
public interface Monitor {

  /**
   * Is resource being monitored exhausted.
   *
   * @return true if yes
   */
  boolean isHealthy();
}
