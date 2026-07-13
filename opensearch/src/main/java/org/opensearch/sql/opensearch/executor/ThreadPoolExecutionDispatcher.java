/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_SLOW_WORKER_THREAD_POOL_NAME;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionDispatcher;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.ThreadPool;

/**
 * Dispatches query execution to either the fast or slow worker thread pool based on whether the
 * plan contains scripts. Plans with scripts require in-memory evaluation and are routed to the slow
 * pool so they don't block fast pushdown-only queries.
 */
@RequiredArgsConstructor
public class ThreadPoolExecutionDispatcher implements ExecutionDispatcher {

  private static final Logger LOG = LogManager.getLogger(ThreadPoolExecutionDispatcher.class);

  private final ThreadPool threadPool;
  private final Settings settings;

  @Override
  public void dispatch(
      RelNode plan,
      CalcitePlanContext context,
      ResponseListener<ExecutionEngine.QueryResponse> listener,
      ExecutionEngine engine) {
    if (isSlowPoolEnabled() && ScriptDetector.hasScripts(plan)) {
      LOG.debug("Query plan contains scripts, dispatching to slow worker pool");
      Map<String, String> ctx = ThreadContext.getImmutableContext();
      CancellableTask task = OpenSearchQueryManager.getCancellableTask();
      threadPool.schedule(
          () -> {
            ThreadContext.putAll(ctx);
            OpenSearchQueryManager.setCancellableTask(task);
            try {
              engine.execute(plan, context, listener);
            } finally {
              OpenSearchQueryManager.clearCancellableTask();
            }
          },
          new TimeValue(0),
          SQL_SLOW_WORKER_THREAD_POOL_NAME);
    } else {
      engine.execute(plan, context, listener);
    }
  }

  private boolean isSlowPoolEnabled() {
    return settings.getSettingValue(Settings.Key.SQL_SLOW_WORKER_POOL_ENABLED);
  }
}
