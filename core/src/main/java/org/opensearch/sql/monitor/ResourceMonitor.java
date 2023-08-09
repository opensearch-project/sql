/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor;

/**
 * The abstract interface of ResourceMonitor. When an fault is detected, the circuit breaker is
 * open.
 */
public abstract class ResourceMonitor {
  /**
   * Is the resource healthy.
   *
   * @return true for healthy, otherwise false.
   */
  public abstract boolean isHealthy();
}
