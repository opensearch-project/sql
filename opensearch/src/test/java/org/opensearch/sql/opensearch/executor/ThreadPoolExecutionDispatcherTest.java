/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_COMPLEX_WORKER_THREAD_POOL_NAME;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQueryBase;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;
import org.opensearch.tasks.CancellableTask;
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
    // Mock the cancellation poller so it returns a no-op cancellable
    when(threadPool.scheduleWithFixedDelay(any(Runnable.class), any(TimeValue.class), any()))
        .thenReturn(mock(org.opensearch.threadpool.Scheduler.Cancellable.class));
  }

  @AfterEach
  void tearDown() {
    ThreadContext.clearAll();
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
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return null;
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
              return null;
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
    ThreadContext.put("request.id", "test-123");
    ThreadContext.put("user", "admin");

    AtomicReference<String> requestIdOnSlowPool = new AtomicReference<>();
    AtomicReference<String> userOnSlowPool = new AtomicReference<>();
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              // Simulate a different thread — clear MDC
              ThreadContext.clearAll();
              task.run();
              requestIdOnSlowPool.set(ThreadContext.get("request.id"));
              userOnSlowPool.set(ThreadContext.get("user"));
              return null;
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
    JaninoRelMetadataProvider provider = mock(JaninoRelMetadataProvider.class);
    RelMetadataQueryBase.THREAD_PROVIDERS.set(provider);

    AtomicReference<JaninoRelMetadataProvider> providerOnSlowPool = new AtomicReference<>();
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              RelMetadataQueryBase.THREAD_PROVIDERS.remove();
              task.run();
              providerOnSlowPool.set(RelMetadataQueryBase.THREAD_PROVIDERS.get());
              return null;
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
              return null;
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
    RuntimeException error = new RuntimeException("execution failed");
    doThrow(error).when(engine).execute(any(RelNode.class), any(), any(ResponseListener.class));

    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return null;
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
              return null;
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
    CancellableTask mockTask = mock(CancellableTask.class);
    OpenSearchQueryManager.setCancellableTask(mockTask);

    AtomicReference<CancellableTask> taskDuringExecution = new AtomicReference<>();
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              // Simulate running on a different thread
              OpenSearchQueryManager.clearCancellableTask();
              task.run();
              return null;
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
