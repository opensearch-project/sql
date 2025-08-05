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

/** OpenSearch Memory Monitor. */
@Log4j2
public class OpenSearchMemoryHealthy {
  private final RandomFail randomFail;
  private final MemoryUsage memoryUsage;

  public OpenSearchMemoryHealthy(Settings settings) {
    randomFail = new RandomFail();
    memoryUsage =
        isCalciteEnabled(settings)
            ? GCedMemoryUsage.getInstance()
            : RuntimeMemoryUsage.getInstance();
  }

  @VisibleForTesting
  public OpenSearchMemoryHealthy(RandomFail randomFail, MemoryUsage memoryUsage) {
    this.randomFail = randomFail;
    this.memoryUsage = memoryUsage;
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

  static class RandomFail {
    public boolean shouldFail() {
      return ThreadLocalRandom.current().nextBoolean();
    }
  }

  @NoArgsConstructor
  public static class MemoryUsageExceedFastFailureException extends RuntimeException {}

  @NoArgsConstructor
  public static class MemoryUsageExceedException extends RuntimeException {}
}
