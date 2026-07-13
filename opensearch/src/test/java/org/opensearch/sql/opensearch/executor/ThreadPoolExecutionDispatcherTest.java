/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_SLOW_WORKER_THREAD_POOL_NAME;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
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
  }

  @Test
  void executesInlineWhenNoScripts() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_SLOW_WORKER_POOL_ENABLED))
        .thenReturn(true);
    RelNode plan = createMockNode();

    dispatcher.dispatch(plan, context, listener, engine);

    verify(engine).execute(plan, context, listener);
    verify(threadPool, never()).schedule(any(), any(TimeValue.class), any());
  }

  @Test
  void dispatchesToSlowPoolWhenScriptsDetected() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_SLOW_WORKER_POOL_ENABLED))
        .thenReturn(true);
    AbstractCalciteIndexScan scan = createMockScan(true);

    dispatcher.dispatch(scan, context, listener, engine);

    verify(threadPool)
        .schedule(any(Runnable.class), eq(new TimeValue(0)), eq(SQL_SLOW_WORKER_THREAD_POOL_NAME));
    verify(engine, never()).execute(any(RelNode.class), any(), any(ResponseListener.class));
  }

  @Test
  void executesInlineWhenSlowPoolDisabled() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_SLOW_WORKER_POOL_ENABLED))
        .thenReturn(false);
    AbstractCalciteIndexScan scan = createMockScan(true);

    dispatcher.dispatch(scan, context, listener, engine);

    verify(engine).execute(scan, context, listener);
    verify(threadPool, never()).schedule(any(), any(TimeValue.class), any());
  }

  @Test
  void scheduledRunnableCallsEngine() {
    when(settings.<Boolean>getSettingValue(Settings.Key.SQL_SLOW_WORKER_POOL_ENABLED))
        .thenReturn(true);
    doAnswer(
            invocation -> {
              Runnable task = invocation.getArgument(0);
              task.run();
              return null;
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), any());

    AbstractCalciteIndexScan scan = createMockScan(true);
    dispatcher.dispatch(scan, context, listener, engine);

    verify(engine).execute(scan, context, listener);
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

  private static AbstractCalciteIndexScan createMockScan(boolean scriptPushed) {
    AbstractCalciteIndexScan scan = mock(AbstractCalciteIndexScan.class);
    when(scan.isScriptPushed()).thenReturn(scriptPushed);
    when(scan.getInputs()).thenReturn(List.of());
    doAnswer(invocation -> null).when(scan).childrenAccept(any(RelVisitor.class));
    return scan;
  }
}
