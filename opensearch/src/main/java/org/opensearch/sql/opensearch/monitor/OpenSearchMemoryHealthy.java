/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

import com.google.common.annotations.VisibleForTesting;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

/** OpenSearch Memory Monitor. */
@Log4j2
public class OpenSearchMemoryHealthy {
  private final FastFail fastFail;
  private final MemoryUsage memoryUsage;

  public OpenSearchMemoryHealthy() {
    fastFail = new FastFail();
    memoryUsage = new MemoryUsage();
  }

  @VisibleForTesting
  public OpenSearchMemoryHealthy(FastFail fastFail, MemoryUsage memoryUsage) {
    this.fastFail = fastFail;
    this.memoryUsage = memoryUsage;
  }

  /** Is Memory Healthy. Calculate based on the current heap memory usage. */
  public boolean isMemoryHealthy(long limitBytes) {
    final long memoryUsage = this.memoryUsage.usage();
    log.debug("Memory usage:{}, limit:{}", memoryUsage, limitBytes);
    if (memoryUsage < limitBytes) {
      return true;
    } else {
      log.warn("Memory usage:{} exceed limit:{}", memoryUsage, limitBytes);
      // always return false in prod, only can be true in testing
      if (fastFail.shouldFail()) {
        log.warn("Fast failing the current request");
        throw new MemoryUsageExceedFastFailureException();
      } else {
        throw new MemoryUsageExceedException();
      }
    }
  }

  static class FastFail {
    public boolean shouldFail() {
      return false;
    }
  }

  static class MemoryUsage {
    public long usage() {
      final long freeMemory = Runtime.getRuntime().freeMemory();
      final long totalMemory = Runtime.getRuntime().totalMemory();
      return totalMemory - freeMemory;
    }
  }

  @NoArgsConstructor
  public static class MemoryUsageExceedFastFailureException extends RuntimeException {}

  @NoArgsConstructor
  public static class MemoryUsageExceedException extends RuntimeException {}
}
