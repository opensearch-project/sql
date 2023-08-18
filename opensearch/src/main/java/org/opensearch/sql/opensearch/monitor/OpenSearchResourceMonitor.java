/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.monitor;

import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import java.util.function.Supplier;
import lombok.extern.log4j.Log4j2;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.monitor.ResourceMonitor;

/**
 * {@link ResourceMonitor} implementation on Elasticsearch. When the heap memory usage exceeds
 * certain threshold, the monitor is not healthy.<br>
 * Todo, add metrics.
 */
@Log4j2
public class OpenSearchResourceMonitor extends ResourceMonitor {
  private final Settings settings;
  private final Retry retry;
  private final OpenSearchMemoryHealthy memoryMonitor;

  /** Constructor. */
  public OpenSearchResourceMonitor(Settings settings, OpenSearchMemoryHealthy memoryMonitor) {
    this.settings = settings;
    RetryConfig config =
        RetryConfig.custom()
            .maxAttempts(3)
            .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(1000))
            .retryExceptions(OpenSearchMemoryHealthy.MemoryUsageExceedException.class)
            .ignoreExceptions(OpenSearchMemoryHealthy.MemoryUsageExceedFastFailureException.class)
            .build();
    retry = Retry.of("mem", config);
    this.memoryMonitor = memoryMonitor;
  }

  /**
   * Is Healthy.
   *
   * @return true if healthy, otherwise return false.
   */
  @Override
  public boolean isHealthy() {
    try {
      ByteSizeValue limit = settings.getSettingValue(Settings.Key.QUERY_MEMORY_LIMIT);
      Supplier<Boolean> booleanSupplier =
          Retry.decorateSupplier(retry, () -> memoryMonitor.isMemoryHealthy(limit.getBytes()));
      return booleanSupplier.get();
    } catch (Exception e) {
      return false;
    }
  }
}
