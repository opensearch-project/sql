/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor;

import lombok.Builder;
import lombok.Getter;

/**
 * Represents the health status of a resource with detailed context information. This wrapper allows
 * error messages to include actionable details about resource exhaustion instead of just boolean
 * health checks.
 */
@Getter
@Builder
public class ResourceStatus {
  /** Type of resource being monitored. */
  public enum ResourceType {
    MEMORY,
    CPU,
    DISK,
    OTHER
  }

  /** Whether the resource is healthy (within limits). */
  private final boolean healthy;

  /** Type of resource (memory, CPU, etc.). */
  private final ResourceType type;

  /** Human-readable description of resource state. */
  private final String description;

  /** Current resource usage value (optional, for metrics). */
  private final Long currentUsage;

  /** Maximum allowed resource value (optional, for metrics). */
  private final Long maxLimit;

  /** Additional contextual information (optional). */
  private final String additionalContext;

  /**
   * Creates a healthy status with minimal information.
   *
   * @param type Resource type
   * @return Healthy ResourceStatus
   */
  public static ResourceStatus healthy(ResourceType type) {
    return ResourceStatus.builder()
        .healthy(true)
        .type(type)
        .description(type + " resources are healthy")
        .build();
  }

  /**
   * Creates an unhealthy status with detailed context.
   *
   * @param type Resource type
   * @param currentUsage Current usage value
   * @param maxLimit Maximum allowed limit
   * @param description Human-readable description
   * @return Unhealthy ResourceStatus with context
   */
  public static ResourceStatus unhealthy(
      ResourceType type, long currentUsage, long maxLimit, String description) {
    return ResourceStatus.builder()
        .healthy(false)
        .type(type)
        .currentUsage(currentUsage)
        .maxLimit(maxLimit)
        .description(description)
        .build();
  }

  /**
   * Gets a formatted description including usage metrics if available.
   *
   * @return Formatted description string
   */
  public String getFormattedDescription() {
    if (currentUsage != null && maxLimit != null) {
      double percentage = (double) currentUsage / maxLimit * 100;
      return String.format(
          "%s (current: %s, limit: %s, usage: %.1f%%)",
          description, formatBytes(currentUsage), formatBytes(maxLimit), percentage);
    }
    return description;
  }

  /**
   * Formats byte values into human-readable format (KB, MB, GB).
   *
   * @param bytes Byte value
   * @return Formatted string
   */
  private String formatBytes(long bytes) {
    if (bytes < 1024) {
      return bytes + "B";
    } else if (bytes < 1024 * 1024) {
      return String.format("%.1fKB", bytes / 1024.0);
    } else if (bytes < 1024 * 1024 * 1024) {
      return String.format("%.1fMB", bytes / (1024.0 * 1024));
    } else {
      return String.format("%.1fGB", bytes / (1024.0 * 1024 * 1024));
    }
  }
}
