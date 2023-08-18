/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.query.planner.resource;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.legacy.executor.join.MetaSearchResult;
import org.opensearch.sql.legacy.query.join.BackOffRetryStrategy;
import org.opensearch.sql.legacy.query.planner.core.Config;
import org.opensearch.sql.legacy.query.planner.resource.monitor.Monitor;
import org.opensearch.sql.legacy.query.planner.resource.monitor.TotalMemoryMonitor;

/** Aggregated resource monitor */
public class ResourceManager {

  private static final Logger LOG = LogManager.getLogger();

  /** Actual resource monitor list */
  private final List<Monitor> monitors = new ArrayList<>();

  /** Time out for the execution */
  private final int timeout;

  private final Instant startTime;

  /** Meta result of the execution */
  private final MetaSearchResult metaResult;

  public ResourceManager(Stats stats, Config config) {
    this.monitors.add(new TotalMemoryMonitor(stats, config));
    this.timeout = config.timeout();
    this.startTime = Instant.now();
    this.metaResult = new MetaSearchResult();
  }

  /**
   * Is all resource monitor healthy with strategy.
   *
   * @return true for yes
   */
  public boolean isHealthy() {
    return BackOffRetryStrategy.isHealthy();
  }

  /**
   * Is current execution time out?
   *
   * @return true for yes
   */
  public boolean isTimeout() {
    return Duration.between(startTime, Instant.now()).getSeconds() >= timeout;
  }

  public MetaSearchResult getMetaResult() {
    return metaResult;
  }
}
