/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor.cursor;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.legacy.query.join.BackOffRetryStrategy;
import org.opensearch.threadpool.ThreadPool;

public class CursorAsyncRestExecutor {
  /** Custom thread pool name managed by OpenSearch */
  public static final String SQL_WORKER_THREAD_POOL_NAME = "sql-worker";

  private static final Logger LOG = LogManager.getLogger(CursorAsyncRestExecutor.class);

  /** Delegated rest executor to async */
  private final CursorRestExecutor executor;

  CursorAsyncRestExecutor(CursorRestExecutor executor) {
    this.executor = executor;
  }

  public void execute(Client client, Map<String, String> params, RestChannel channel) {
    async(client, params, channel);
  }

  /** Run given task in thread pool asynchronously */
  private void async(Client client, Map<String, String> params, RestChannel channel) {

    ThreadPool threadPool = client.threadPool();
    Runnable runnable =
        () -> {
          try {
            doExecuteWithTimeMeasured(client, params, channel);
          } catch (IOException e) {
            Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
            LOG.warn(
                "[{}] [MCB] async task got an IO/SQL exception: {}",
                QueryContext.getRequestId(),
                e.getMessage());
            e.printStackTrace();
            channel.sendResponse(
                new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
          } catch (IllegalStateException e) {
            Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
            LOG.warn(
                "[{}] [MCB] async task got a runtime exception: {}",
                QueryContext.getRequestId(),
                e.getMessage());
            e.printStackTrace();
            channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.INSUFFICIENT_STORAGE, "Memory circuit is broken."));
          } catch (Throwable t) {
            Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
            LOG.warn(
                "[{}] [MCB] async task got an unknown throwable: {}",
                QueryContext.getRequestId(),
                t.getMessage());
            t.printStackTrace();
            channel.sendResponse(
                new BytesRestResponse(
                    RestStatus.INTERNAL_SERVER_ERROR, String.valueOf(t.getMessage())));
          } finally {
            BackOffRetryStrategy.releaseMem(executor);
          }
        };

    // Preserve context of calling thread to ensure headers of requests are forwarded when running
    // blocking actions
    threadPool.schedule(
        QueryContext.withCurrentContext(runnable), new TimeValue(0L), SQL_WORKER_THREAD_POOL_NAME);
  }

  /** Time the real execution of Executor and log slow query for troubleshooting */
  private void doExecuteWithTimeMeasured(
      Client client, Map<String, String> params, RestChannel channel) throws Exception {
    long startTime = System.nanoTime();
    try {
      executor.execute(client, params, channel);
    } finally {
      Duration elapsed = Duration.ofNanos(System.nanoTime() - startTime);
      int slowLogThreshold = LocalClusterState.state().getSettingValue(Settings.Key.SQL_SLOWLOG);
      if (elapsed.getSeconds() >= slowLogThreshold) {
        LOG.warn(
            "[{}] Slow query: elapsed={} (ms)", QueryContext.getRequestId(), elapsed.toMillis());
      }
    }
  }
}
