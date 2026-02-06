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
  public boolean isHealthy() {
    return getStatus().isHealthy();
  }

  /**
   * Get detailed resource status including context for error messages. Subclasses should override
   * this method to provide rich status information.
   *
   * @return ResourceStatus with health state and detailed context
   */
  public ResourceStatus getStatus() {
    // Default implementation for backwards compatibility
    // Subclasses should override this to provide detailed status
    boolean healthy = isHealthyImpl();
    return healthy
        ? ResourceStatus.healthy(ResourceStatus.ResourceType.OTHER)
        : ResourceStatus.builder()
            .healthy(false)
            .type(ResourceStatus.ResourceType.OTHER)
            .description("Resource monitor reports unhealthy status")
            .build();
  }

  /**
   * Internal implementation for health check. Subclasses that don't override getStatus() should
   * override this instead.
   *
   * @return true for healthy, otherwise false.
   */
  protected boolean isHealthyImpl() {
    // Default: delegate to old isHealthy() behavior
    // This prevents infinite recursion for old implementations
    throw new UnsupportedOperationException(
        "Subclass must override either getStatus() or isHealthyImpl()");
  }
}
