/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_SLOW_WORKER_THREAD_POOL_NAME;

import java.util.Map;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.runtime.Hook;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.checkerframework.checker.nullness.qual.Nullable;
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
      @Nullable JaninoRelMetadataProvider metadataProvider =
          RelMetadataQueryBase.THREAD_PROVIDERS.get();
      long currentTime = Hook.CURRENT_TIME.get(-1L);
      threadPool.schedule(
          () -> {
            ThreadContext.putAll(ctx);
            OpenSearchQueryManager.setCancellableTask(task);
            if (metadataProvider != null) {
              RelMetadataQueryBase.THREAD_PROVIDERS.set(metadataProvider);
            }
            Hook.Closeable hookHandle = null;
            if (currentTime >= 0) {
              hookHandle =
                  Hook.CURRENT_TIME.addThread(
                      (Consumer<org.apache.calcite.util.Holder<Long>>) h -> h.set(currentTime));
            }
            try {
              engine.execute(plan, context, listener);
            } finally {
              if (hookHandle != null) {
                hookHandle.close();
              }
              OpenSearchQueryManager.clearCancellableTask();
              RelMetadataQueryBase.THREAD_PROVIDERS.remove();
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
