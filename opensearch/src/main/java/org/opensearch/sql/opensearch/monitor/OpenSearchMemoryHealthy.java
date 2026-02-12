/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ThreadLocalRandom;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.monitor.ResourceStatus;

/** OpenSearch Memory Monitor. */
@Log4j2
public class OpenSearchMemoryHealthy {
  private final RandomFail randomFail;
  private final MemoryUsage memoryUsage;

  public OpenSearchMemoryHealthy(Settings settings) {
    randomFail = new RandomFail();
    memoryUsage = buildMemoryUsage(settings);
  }

  @VisibleForTesting
  public OpenSearchMemoryHealthy(RandomFail randomFail, MemoryUsage memoryUsage) {
    this.randomFail = randomFail;
    this.memoryUsage = memoryUsage;
  }

  private MemoryUsage buildMemoryUsage(Settings settings) {
    try {
      return isCalciteEnabled(settings)
          ? GCedMemoryUsage.getInstance()
          : RuntimeMemoryUsage.getInstance();
    } catch (Throwable e) {
      return RuntimeMemoryUsage.getInstance();
    }
  }

  private boolean isCalciteEnabled(Settings settings) {
    if (settings != null) {
      return settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED);
    } else {
      return false;
    }
  }

  /** Is Memory Healthy. Calculate based on the current heap memory usage. */
  public boolean isMemoryHealthy(long limitBytes) {
    final long memoryUsage = this.memoryUsage.usage();
    log.debug("Memory usage:{}, limit:{}", memoryUsage, limitBytes);
    if (memoryUsage < limitBytes) {
      return true;
    } else {
      log.warn("Memory usage:{} exceed limit:{}", memoryUsage, limitBytes);
      if (randomFail.shouldFail()) {
        log.warn("Fast failing the current request");
        throw new MemoryUsageExceedFastFailureException();
      } else {
        throw new MemoryUsageExceedException();
      }
    }
  }

  /**
   * Get detailed memory health status with usage metrics.
   *
   * @param limitBytes Memory limit in bytes
   * @return ResourceStatus with detailed memory information
   */
  public ResourceStatus getMemoryStatus(long limitBytes) {
    final long currentMemoryUsage = this.memoryUsage.usage();
    log.debug("Memory usage:{}, limit:{}", currentMemoryUsage, limitBytes);

    if (currentMemoryUsage < limitBytes) {
      return ResourceStatus.builder()
          .healthy(true)
          .type(ResourceStatus.ResourceType.MEMORY)
          .currentUsage(currentMemoryUsage)
          .maxLimit(limitBytes)
          .description("Memory usage is within limits")
          .build();
    } else {
      log.warn("Memory usage:{} exceed limit:{}", currentMemoryUsage, limitBytes);
      String description =
          String.format(
              "Memory usage exceeds limit: %d bytes used, %d bytes limit",
              currentMemoryUsage, limitBytes);

      return ResourceStatus.builder()
          .healthy(false)
          .type(ResourceStatus.ResourceType.MEMORY)
          .currentUsage(currentMemoryUsage)
          .maxLimit(limitBytes)
          .description(description)
          .build();
    }
  }

  static class RandomFail {
    public boolean shouldFail() {
      return ThreadLocalRandom.current().nextBoolean();
    }
  }

  @NoArgsConstructor
  public static class MemoryUsageExceedFastFailureException extends MemoryUsageException {}

  @NoArgsConstructor
  public static class MemoryUsageExceedException extends MemoryUsageException {}

  @NoArgsConstructor
  public static class MemoryUsageException extends RuntimeException {}
}
