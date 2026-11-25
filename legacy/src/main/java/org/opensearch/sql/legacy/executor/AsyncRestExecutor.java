/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.executor.format.ErrorMessageFactory;
import org.opensearch.sql.legacy.query.QueryAction;
import org.opensearch.sql.legacy.query.join.BackOffRetryStrategy;
import org.opensearch.sql.legacy.utils.OpenSearchFailures;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transports;
import org.opensearch.transport.client.Client;

/** A RestExecutor wrapper to execute request asynchronously to avoid blocking transport thread. */
public class AsyncRestExecutor implements RestExecutor {
  private static final Logger LOG = LogManager.getLogger(AsyncRestExecutor.class);

  /**
   * Treat all actions as blocking which means async all actions, ex. execute() in csv executor or
   * pretty format executor
   */
  private static final Predicate<QueryAction> ALL_ACTION_IS_BLOCKING = anyAction -> true;

  /** Delegated rest executor to async */
  private final RestExecutor executor;

  /** Request type that expect to async to avoid blocking */
  private final Predicate<QueryAction> isBlocking;

  AsyncRestExecutor(RestExecutor executor) {
    this(executor, ALL_ACTION_IS_BLOCKING);
  }

  AsyncRestExecutor(RestExecutor executor, Predicate<QueryAction> isBlocking) {
    this.executor = executor;
    this.isBlocking = isBlocking;
  }

  @Override
  public void execute(
      Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel)
      throws Exception {
    if (isBlockingAction(queryAction) && isRunningInTransportThread()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "[{}] Async blocking query action [{}] for executor [{}] in current thread [{}]",
            QueryContext.getRequestId(),
            name(executor),
            name(queryAction),
            Thread.currentThread().getName());
      }
      async(client, params, queryAction, channel);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(
            "[{}] Continue running query action [{}] for executor [{}] in current thread [{}]",
            QueryContext.getRequestId(),
            name(executor),
            name(queryAction),
            Thread.currentThread().getName());
      }
      doExecuteWithTimeMeasured(client, params, queryAction, channel);
    }
  }

  @Override
  public String execute(Client client, Map<String, String> params, QueryAction queryAction)
      throws Exception {
    // Result is always required and no easy way to async it here.
    return executor.execute(client, params, queryAction);
  }

  private boolean isBlockingAction(QueryAction queryAction) {
    return isBlocking.test(queryAction);
  }

  private boolean isRunningInTransportThread() {
    return Transports.isTransportThread(Thread.currentThread());
  }

  /** Run given task in thread pool asynchronously */
  private void async(
      Client client, Map<String, String> params, QueryAction queryAction, RestChannel channel) {

    ThreadPool threadPool = client.threadPool();
    Runnable runnable =
        () -> {
          try {
            doExecuteWithTimeMeasured(client, params, queryAction, channel);
          } catch (IOException | SqlParseException | OpenSearchException e) {
            sendFailure(
                channel,
                e,
                "async task got an IO/SQL exception",
                () -> new BytesRestResponse(RestStatus.INTERNAL_SERVER_ERROR, e.getMessage()));
          } catch (IllegalStateException e) {
            sendFailure(
                channel,
                e,
                "async task got a runtime exception",
                () ->
                    new BytesRestResponse(
                        RestStatus.INSUFFICIENT_STORAGE, "Memory circuit is broken."));
          } catch (Throwable t) {
            sendFailure(
                channel,
                t,
                "async task got an unknown throwable",
                () ->
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

  /**
   * Sends a failed request and counts it by the status of the response. If OpenSearch reported the
   * failure, the response uses the status OpenSearch reported, such as 403, 429 or 503, so that it
   * matches the status in the body. Otherwise the response is the given fallback.
   */
  private static void sendFailure(
      RestChannel channel,
      Throwable failure,
      String message,
      Supplier<BytesRestResponse> fallback) {
    if (OpenSearchFailures.findClientErrorCause(failure).isPresent()) {
      OpenSearchFailures.logFailure(
          LOG,
          Level.WARN,
          "[" + QueryContext.getRequestId() + "] async task got a client error",
          failure);
    } else {
      LOG.warn("[{}] [MCB] {}: {}", QueryContext.getRequestId(), message, failure.getMessage());
    }
    BytesRestResponse response =
        OpenSearchFailures.findOpenSearchCause(failure)
            .map(AsyncRestExecutor::openSearchFailureResponse)
            .orElseGet(fallback);
    OpenSearchFailures.countFailure(response.status().getStatus());
    channel.sendResponse(response);
  }

  private static BytesRestResponse openSearchFailureResponse(OpenSearchException failure) {
    RestStatus status = failure.status();
    return new BytesRestResponse(
        status, ErrorMessageFactory.createErrorMessage(failure, status.getStatus()).toString());
  }

  /** Time the real execution of Executor and log slow query for troubleshooting */
  private void doExecuteWithTimeMeasured(
      Client client, Map<String, String> params, QueryAction action, RestChannel channel)
      throws Exception {
    long startTime = System.nanoTime();
    try {
      executor.execute(client, params, action, channel);
    } finally {
      Duration elapsed = Duration.ofNanos(System.nanoTime() - startTime);
      int slowLogThreshold = LocalClusterState.state().getSettingValue(Settings.Key.SQL_SLOWLOG);
      if (elapsed.getSeconds() >= slowLogThreshold) {
        LOG.warn(
            "[{}] Slow query: elapsed={} (ms)", QueryContext.getRequestId(), elapsed.toMillis());
      }
    }
  }

  private String name(Object object) {
    return object.getClass().getSimpleName();
  }
}
