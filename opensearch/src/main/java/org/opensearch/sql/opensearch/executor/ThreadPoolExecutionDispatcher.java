/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_COMPLEX_WORKER_THREAD_POOL_NAME;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;

import java.util.Map;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.Holder;
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
import org.opensearch.sql.monitor.profile.ProfileContext;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.ThreadPool;

/**
 * Dispatches query execution to either the fast or complex worker thread pool based on whether the
 * plan contains scripts. Plans with scripts require in-memory evaluation and are routed to the
 * complex pool so they don't block fast pushdown-only queries.
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
    dispatchInternal(plan, context, () -> engine.execute(plan, context, listener), listener);
  }

  @Override
  public void dispatchTask(RelNode plan, CalcitePlanContext context, Runnable task) {
    dispatchInternal(plan, context, task, null);
  }

  private void dispatchInternal(
      RelNode optimizedPlan,
      CalcitePlanContext context,
      Runnable task,
      @Nullable ResponseListener<?> failureListener) {
    if (isComplexPoolEnabled() && ScriptDetector.hasScripts(optimizedPlan)) {
      LOG.debug("Query plan contains scripts, dispatching to complex worker pool");
      // Capture thread-local state to propagate across thread boundary
      Map<String, String> ctx = ThreadContext.getImmutableContext();
      CancellableTask cancellableTask = OpenSearchQueryManager.getCancellableTask();
      ProfileContext profileContext = QueryProfiling.current();
      CalcitePlanContext.ThreadLocalSnapshot snapshot = CalcitePlanContext.snapshotThreadLocals();
      @Nullable JaninoRelMetadataProvider metadataProvider =
          RelMetadataQueryBase.THREAD_PROVIDERS.get();
      long currentTime = Hook.CURRENT_TIME.get(-1L);

      threadPool.schedule(
          () -> {
            final Thread executionThread = Thread.currentThread();
            Cancellable cancelPoller = scheduleCancellationPoller(cancellableTask, executionThread);
            Hook.Closeable hookHandle = null;
            try {
              // Restore state from caller thread
              ThreadContext.putAll(ctx);
              OpenSearchQueryManager.setCancellableTask(cancellableTask);
              QueryProfiling.set(profileContext);
              CalcitePlanContext.restoreThreadLocals(snapshot);
              // Override execution pool to indicate complex pool
              CalcitePlanContext.executionPool.set(SQL_COMPLEX_WORKER_THREAD_POOL_NAME);
              if (metadataProvider != null) {
                RelMetadataQueryBase.THREAD_PROVIDERS.set(metadataProvider);
              }
              if (currentTime >= 0) {
                hookHandle =
                    Hook.CURRENT_TIME.addThread((Consumer<Holder<Long>>) h -> h.set(currentTime));
              }
              task.run();
            } catch (Exception e) {
              LOG.error("Exception during task execution on complex pool", e);
              if (failureListener != null) {
                failureListener.onFailure(e);
              }
            } finally {
              cancelPoller.cancel();
              Thread.interrupted();
              if (hookHandle != null) {
                hookHandle.close();
              }
              OpenSearchQueryManager.clearCancellableTask();
              RelMetadataQueryBase.THREAD_PROVIDERS.remove();
              CalcitePlanContext.clearTimewrapSignals();
              QueryProfiling.clear();
            }
          },
          new TimeValue(0),
          SQL_COMPLEX_WORKER_THREAD_POOL_NAME);
    } else {
      CalcitePlanContext.executionPool.set(SQL_WORKER_THREAD_POOL_NAME);
      task.run();
    }
  }

  private static final TimeValue CANCEL_POLL_INTERVAL = new TimeValue(500);
  private static final Cancellable NOOP_CANCELLABLE =
      new Cancellable() {
        @Override
        public boolean cancel() {
          return false;
        }

        @Override
        public boolean isCancelled() {
          return false;
        }
      };

  /**
   * Polls the cancellable task and interrupts the execution thread when cancelled. This bridges the
   * gap where OpenSearchQueryManager's timeout interrupt targets the sql-worker thread but
   * execution has moved to the complex-worker thread.
   */
  private Cancellable scheduleCancellationPoller(
      @Nullable CancellableTask cancellableTask, Thread executionThread) {
    if (cancellableTask == null) {
      return NOOP_CANCELLABLE;
    }
    return threadPool.scheduleWithFixedDelay(
        () -> {
          if (cancellableTask.isCancelled()) {
            LOG.debug("Task cancelled, interrupting complex pool execution thread");
            executionThread.interrupt();
          }
        },
        CANCEL_POLL_INTERVAL,
        ThreadPool.Names.GENERIC);
  }

  private boolean isComplexPoolEnabled() {
    return settings.getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED);
  }
}
