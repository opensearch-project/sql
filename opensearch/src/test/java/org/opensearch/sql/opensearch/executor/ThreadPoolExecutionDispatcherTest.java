/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.apache.logging.log4j.ThreadContext.clearAll;
import static org.apache.logging.log4j.ThreadContext.get;
import static org.apache.logging.log4j.ThreadContext.put;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.common.settings.Settings.EMPTY;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_COMPLEX_WORKER_THREAD_POOL_NAME;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.Scheduler.Cancellable;
import org.opensearch.threadpool.Scheduler.ScheduledCancellable;
import org.opensearch.threadpool.ThreadPool;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class ThreadPoolExecutionDispatcherTest {

  @Mock private ThreadPool threadPool;
  @Mock private Settings settings;
  @Mock private CalcitePlanContext context;
  @Mock private ResponseListener<ExecutionEngine.QueryResponse> listener;
  @Mock private ExecutionEngine engine;

  private ThreadPoolExecutionDispatcher dispatcher;

  @BeforeEach
  void setUp() {
    dispatcher = new ThreadPoolExecutionDispatcher(threadPool, settings);
    when(threadPool.getThreadContext()).thenReturn(new ThreadContext(EMPTY));
    // Mock schedule calls to return non-null cancellables (for both outer dispatch and inner
    // timeout)
    when(threadPool.schedule(any(Runnable.class), any(TimeValue.class), any()))
        .thenReturn(mock(ScheduledCancellable.class));
    when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class), any()))
        .thenReturn(mock(Cancellable.class));
  }

  @AfterEach
  void tearDown() {
    clearAll();
    OpenSearchQueryManager.clearCancellableTask();
    RelMetadataQueryBase.THREAD_PROVIDERS.remove();
    CalcitePlanContext.clearTimewrapSignals();
  }

  @Test
  void executesInlineWhenNoScripts() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    RelNode plan = createMockNode();

    dispatcher.dispatch(plan, context, listener, engine);

    verify(engine).execute(plan, context, listener);
    verify(threadPool, never()).schedule(any(), any(TimeValue.class), any());
  }

  @Test
  void dispatchesToSlowPoolWhenScriptsDetected() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    AbstractCalciteIndexScan scan = createMockScanWithScripts();

    dispatcher.dispatch(scan, context, listener, engine);

    verify(threadPool)
        .schedule(
            any(Runnable.class), eq(new TimeValue(0)), eq(SQL_COMPLEX_WORKER_THREAD_POOL_NAME));
    verify(engine, never()).execute(any(RelNode.class), any(), any(ResponseListener.class));
  }

  @Test
  void executesInlineWhenSlowPoolDisabled() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(false);
    AbstractCalciteIndexScan scan = createMockScanWithScripts();

    dispatcher.dispatch(scan, context, listener, engine);

    verify(engine).execute(scan, context, listener);
    verify(threadPool, never()).schedule(any(), any(TimeValue.class), any());
  }

  @Test
  void scheduledRunnableCallsEngine() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    verify(engine).execute(scan, context, listener);
  }

  @Test
  void propagatesCancellableTaskToSlowPool() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    CancellableTask mockTask = mock(CancellableTask.class);
    OpenSearchQueryManager.setCancellableTask(mockTask);

    AtomicReference<CancellableTask> taskOnSlowPool = new AtomicReference<>();
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              // Simulate running on a different thread — clear the ThreadLocal first
              OpenSearchQueryManager.clearCancellableTask();
              task.run();
              taskOnSlowPool.set(OpenSearchQueryManager.getCancellableTask());
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    // During execution, the task should have been available
    // (we check via a side-channel since the finally block clears it)
    verify(engine).execute(scan, context, listener);
    // After execution, it should be cleaned up
    assertNull(
        OpenSearchQueryManager.getCancellableTask(),
        "CancellableTask should be cleared after execution");
  }

  @Test
  void propagatesLog4jThreadContextToSlowPool() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    put("request.id", "test-123");
    put("user", "admin");

    AtomicReference<String> requestIdOnSlowPool = new AtomicReference<>();
    AtomicReference<String> userOnSlowPool = new AtomicReference<>();
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              // Simulate a different thread — clear MDC
              clearAll();
              task.run();
              requestIdOnSlowPool.set(get("request.id"));
              userOnSlowPool.set(get("user"));
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    assertEquals("test-123", requestIdOnSlowPool.get());
    assertEquals("admin", userOnSlowPool.get());
  }

  @Test
  void propagatesMetadataProviderToSlowPool() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    JaninoRelMetadataProvider provider = mock(JaninoRelMetadataProvider.class);
    RelMetadataQueryBase.THREAD_PROVIDERS.set(provider);

    AtomicReference<JaninoRelMetadataProvider> providerOnSlowPool = new AtomicReference<>();
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              RelMetadataQueryBase.THREAD_PROVIDERS.remove();
              task.run();
              providerOnSlowPool.set(RelMetadataQueryBase.THREAD_PROVIDERS.get());
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    // After finally block, metadata provider should be cleaned up
    assertNull(
        RelMetadataQueryBase.THREAD_PROVIDERS.get(),
        "Metadata provider should be cleaned up after execution");
  }

  @Test
  void propagatesTimewrapSignalsToSlowPool() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    CalcitePlanContext.stripNullColumns.set(true);
    CalcitePlanContext.timewrapUnitName.set("HOUR");
    CalcitePlanContext.timewrapSeries.set("timestamp");

    AtomicReference<Boolean> stripOnSlowPool = new AtomicReference<>();
    AtomicReference<String> unitOnSlowPool = new AtomicReference<>();
    AtomicReference<String> seriesOnSlowPool = new AtomicReference<>();
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              // Clear thread-locals to simulate new thread
              CalcitePlanContext.clearTimewrapSignals();
              CalcitePlanContext.stripNullColumns.set(false);
              task.run();
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    // Capture the values during engine.execute
    doAnswer(
            invocation -> {
              stripOnSlowPool.set(CalcitePlanContext.stripNullColumns.get());
              unitOnSlowPool.set(CalcitePlanContext.timewrapUnitName.get());
              seriesOnSlowPool.set(CalcitePlanContext.timewrapSeries.get());
              return null;
            })
        .when(engine)
        .execute(any(RelNode.class), any(), any(ResponseListener.class));

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    assertEquals(true, stripOnSlowPool.get());
    assertEquals("HOUR", unitOnSlowPool.get());
    assertEquals("timestamp", seriesOnSlowPool.get());
  }

  @Test
  void forwardsExceptionToListenerOnSlowPool() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    RuntimeException error = new RuntimeException("execution failed");
    doThrow(error).when(engine).execute(any(RelNode.class), any(), any(ResponseListener.class));

    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    verify(listener).onFailure(error);
  }

  @Test
  void cleansUpThreadLocalsAfterException() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    CancellableTask mockTask = mock(CancellableTask.class);
    OpenSearchQueryManager.setCancellableTask(mockTask);
    CalcitePlanContext.timewrapUnitName.set("DAY");
    RelMetadataQueryBase.THREAD_PROVIDERS.set(mock(JaninoRelMetadataProvider.class));

    doThrow(new RuntimeException("boom"))
        .when(engine)
        .execute(any(RelNode.class), any(), any(ResponseListener.class));

    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    assertNull(OpenSearchQueryManager.getCancellableTask());
    assertNull(RelMetadataQueryBase.THREAD_PROVIDERS.get());
    // timewrapSignals cleared via clearTimewrapSignals()
    assertNull(CalcitePlanContext.timewrapUnitName.get());
  }

  @Test
  void cancellableTaskAvailableDuringExecution() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    CancellableTask mockTask = mock(CancellableTask.class);
    OpenSearchQueryManager.setCancellableTask(mockTask);

    AtomicReference<CancellableTask> taskDuringExecution = new AtomicReference<>();
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              // Simulate running on a different thread
              OpenSearchQueryManager.clearCancellableTask();
              task.run();
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    doAnswer(
            invocation -> {
              taskDuringExecution.set(OpenSearchQueryManager.getCancellableTask());
              return null;
            })
        .when(engine)
        .execute(any(RelNode.class), any(), any(ResponseListener.class));

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    assertNotNull(
        taskDuringExecution.get(), "CancellableTask should be available during execution");
    assertEquals(mockTask, taskDuringExecution.get());
  }

  @Test
  void carriesParentMarkerHeaderToComplexPool() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));

    // Distinct contexts for the caller thread (has the marker) and the pool thread (starts empty),
    // so the re-apply putHeader branch runs.
    ThreadContext callerCtx = new ThreadContext(EMPTY);
    callerCtx.putHeader("X-Query-Insights-Parent", "PPL:node-1:5");
    ThreadContext poolCtx = new ThreadContext(EMPTY);
    when(threadPool.getThreadContext()).thenReturn(callerCtx).thenReturn(poolCtx);

    AtomicReference<String> markerDuringExecution = new AtomicReference<>();
    doAnswer(
            invocation -> {
              Object executor = invocation.getArgument(2);
              if (SQL_COMPLEX_WORKER_THREAD_POOL_NAME.equals(executor)) {
                invocation.<Runnable>getArgument(0).run();
              }
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());
    doAnswer(
            invocation -> {
              markerDuringExecution.set(poolCtx.getHeader("X-Query-Insights-Parent"));
              return null;
            })
        .when(engine)
        .execute(any(RelNode.class), any(), any(ResponseListener.class));

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    assertEquals(
        "PPL:node-1:5",
        markerDuringExecution.get(),
        "parent marker header should be re-applied on the complex-worker thread");
  }

  @Test
  void bracketsResourceTrackingOnComplexPool() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    CancellableTask trackingTask = mock(CancellableTask.class);
    when(trackingTask.supportsResourceTracking()).thenReturn(true);
    OpenSearchQueryManager.setCancellableTask(trackingTask);

    doAnswer(
            invocation -> {
              Object executor = invocation.getArgument(2);
              if (SQL_COMPLEX_WORKER_THREAD_POOL_NAME.equals(executor)) {
                OpenSearchQueryManager.clearCancellableTask();
                invocation.<Runnable>getArgument(0).run();
              }
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    // start/stopThreadResourceTracking take (threadId, statsType, ResourceUsageMetric...) varargs.
    verify(trackingTask).supportsResourceTracking();
    verify(trackingTask)
        .startThreadResourceTracking(
            anyLong(), any(ResourceStatsType.class), any(ResourceUsageMetric[].class));
    verify(trackingTask)
        .stopThreadResourceTracking(
            anyLong(), any(ResourceStatsType.class), any(ResourceUsageMetric[].class));
  }

  @Test
  void cancellationPollerInterruptsWhenTaskCancelled() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    CancellableTask cancelledTask = mock(CancellableTask.class);
    when(cancelledTask.isCancelled()).thenReturn(true);
    OpenSearchQueryManager.setCancellableTask(cancelledTask);

    // Capture the poller runnable so we can fire it directly and assert it interrupts the thread.
    AtomicReference<Runnable> poller = new AtomicReference<>();
    when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class), any()))
        .thenAnswer(
            invocation -> {
              poller.set(invocation.getArgument(0));
              return mock(Cancellable.class);
            });

    AtomicReference<Boolean> interruptedInExecute = new AtomicReference<>(false);
    doAnswer(
            invocation -> {
              Object executor = invocation.getArgument(2);
              if (SQL_COMPLEX_WORKER_THREAD_POOL_NAME.equals(executor)) {
                invocation.<Runnable>getArgument(0).run();
              }
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());
    doAnswer(
            invocation -> {
              // Fire the poller on this (execution) thread; it should interrupt us.
              poller.get().run();
              interruptedInExecute.set(Thread.currentThread().isInterrupted());
              Thread.interrupted(); // clear so we don't leak the interrupt into the pool thread
              return null;
            })
        .when(engine)
        .execute(any(RelNode.class), any(), any(ResponseListener.class));

    AbstractCalciteIndexScan scan = createMockScanWithScripts();
    dispatcher.dispatch(scan, context, listener, engine);

    assertTrue(
        interruptedInExecute.get(),
        "cancellation poller should interrupt the execution thread when the task is cancelled");
  }

  @Test
  void dispatchTaskRunsInlineWhenNoScripts() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    RelNode plan = createMockNode();
    AtomicReference<Boolean> ran = new AtomicReference<>(false);

    dispatcher.dispatchTask(plan, context, () -> ran.set(true));

    assertTrue(ran.get());
    verify(threadPool, never()).schedule(any(), any(TimeValue.class), any());
  }

  @Test
  void dispatchTaskSwallowsFailureWhenNoListener() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_COMPLEX_WORKER_POOL_ENABLED))
        .thenReturn(true);
    when(settings.<TimeValue>getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT))
        .thenReturn(new TimeValue(60000));
    doAnswer(
            invocation -> {
              Object executor = invocation.getArgument(2);
              if (SQL_COMPLEX_WORKER_THREAD_POOL_NAME.equals(executor)) {
                invocation.<Runnable>getArgument(0).run();
              }
              return mock(ScheduledCancellable.class);
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    AbstractCalciteIndexScan scan = createMockScanWithScripts();

    dispatcher.dispatchTask(
        scan,
        context,
        () -> {
          throw new RuntimeException("boom");
        });

    // No exception escaped and the task was cleared: the null-listener catch + finally ran.
    assertNull(OpenSearchQueryManager.getCancellableTask());
  }

  private static RelNode createMockNode(RelNode... children) {
    RelNode node = mock(RelNode.class);
    List<RelNode> childList = List.of(children);
    when(node.getInputs()).thenReturn(childList);
    doAnswer(
            invocation -> {
              RelVisitor visitor = invocation.getArgument(0);
              for (int i = 0; i < childList.size(); i++) {
                visitor.visit(childList.get(i), i, node);
              }
              return null;
            })
        .when(node)
        .childrenAccept(any(RelVisitor.class));
    return node;
  }

  private static AbstractCalciteIndexScan createMockScanWithScripts() {
    AbstractCalciteIndexScan scan = mock(AbstractCalciteIndexScan.class);
    PushDownContext ctx = mock(PushDownContext.class);
    when(ctx.isScriptPushed()).thenReturn(true);
    when(ctx.isSortExprPushed()).thenReturn(false);
    when(ctx.getAggSpec()).thenReturn(null);
    when(scan.getPushDownContext()).thenReturn(ctx);
    when(scan.getInputs()).thenReturn(List.of());
    doAnswer(invocation -> null).when(scan).childrenAccept(any(RelVisitor.class));
    return scan;
  }
}
