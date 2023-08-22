/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.opensearch.sql.executor.execution.AbstractPlan;

/** Default QueryManager implementation which execute {@link AbstractPlan} on caller thread. */
public class DefaultQueryManager implements QueryManager {

  private final ExecutorService executorService;

  private final Map<QueryId, Future<?>> map = new HashMap<>();

  public DefaultQueryManager(ExecutorService executorService) {
    this.executorService = executorService;
  }

  public static DefaultQueryManager defaultQueryManager() {
    return new DefaultQueryManager(Executors.newSingleThreadExecutor());
  }

  @Override
  public synchronized QueryId submit(AbstractPlan queryExecution) {
    Future<?> future = executorService.submit(queryExecution::execute);
    QueryId queryId = queryExecution.getQueryId();

    map.put(queryId, future);
    return queryId;
  }

  @Override
  public synchronized boolean cancel(QueryId queryId) {
    if (map.containsKey(queryId)) {
      Future<?> future = map.get(queryId);
      map.remove(queryId);
      return future.cancel(true);
    } else {
      return false;
    }
  }

  public void awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
    executorService.shutdown();
    executorService.awaitTermination(timeout, timeUnit);
  }
}
