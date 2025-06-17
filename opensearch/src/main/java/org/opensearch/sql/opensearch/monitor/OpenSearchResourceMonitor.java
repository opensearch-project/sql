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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
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
  private final ClusterService clusterService;

  /** Constructor. */
  public OpenSearchResourceMonitor(
      Settings settings, OpenSearchMemoryHealthy memoryMonitor, ClusterService clusterService) {
    this.settings = settings;
    RetryConfig config =
        RetryConfig.custom()
            .maxAttempts(3)
            .intervalFunction(IntervalFunction.ofExponentialRandomBackoff(1000))
            .retryExceptions(OpenSearchMemoryHealthy.MemoryUsageExceedException.class)
            .ignoreExceptions(OpenSearchMemoryHealthy.MemoryUsageExceedFastFailureException.class)
            .build();
    this.retry = Retry.of("mem", config);
    this.retry
        .getEventPublisher()
        .onRetry(
            event -> {
              if (event.getNumberOfRetryAttempts() == 2 && canTriggerGC()) {
                System.gc();
                log.warn("isMemoryHealthy() failed in first retry, triggered System.gc()");
              }
            });
    this.memoryMonitor = memoryMonitor;
    this.clusterService = clusterService;
  }

  private boolean canTriggerGC() {
    return clusterService.isStateInitialised()
        && isDedicatedCoordinator(clusterService.localNode());
  }

  /** Keep the same behaviour with {@link DiscoveryNodes#getCoordinatingOnlyNodes()} */
  private boolean isDedicatedCoordinator(DiscoveryNode local) {
    return !local.isDataNode() // Not a data role
        && !local.isWarmNode() // Not a warm data role
        && !local.isIngestNode() // Not an ingest role
        && !local.isClusterManagerNode(); // Not a manager role
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
      if (limit == null) {
        // undefined, be always healthy, this is useful in Calcite standalone ITs
        // since AlwaysHealthyMonitor is not work within Calcite tests.
        return true;
      }
      Supplier<Boolean> booleanSupplier =
          Retry.decorateSupplier(retry, () -> memoryMonitor.isMemoryHealthy(limit.getBytes()));
      return booleanSupplier.get();
    } catch (Exception e) {
      return false;
    }
  }
}
