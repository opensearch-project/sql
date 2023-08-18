/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ThreadLocalRandom;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

/** OpenSearch Memory Monitor. */
@Log4j2
public class OpenSearchMemoryHealthy {
  private final RandomFail randomFail;
  private final MemoryUsage memoryUsage;

  public OpenSearchMemoryHealthy() {
    randomFail = new RandomFail();
    memoryUsage = new MemoryUsage();
  }

  @VisibleForTesting
  public OpenSearchMemoryHealthy(RandomFail randomFail, MemoryUsage memoryUsage) {
    this.randomFail = randomFail;
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
      if (randomFail.shouldFail()) {
        log.warn("Fast failure the current request");
        throw new MemoryUsageExceedFastFailureException();
      } else {
        throw new MemoryUsageExceedException();
      }
    }
  }

  static class RandomFail {
    public boolean shouldFail() {
      return ThreadLocalRandom.current().nextBoolean();
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
