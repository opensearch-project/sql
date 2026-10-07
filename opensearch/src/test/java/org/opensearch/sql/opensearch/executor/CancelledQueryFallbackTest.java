/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.executor.ExecutionContext;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.monitor.ResourceMonitor;
import org.opensearch.sql.monitor.ResourceStatus;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.storage.scan.OpenSearchIndexEnumerator;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.tasks.CancellableTask;

/** A cancelled Calcite scan must fail the query rather than restart it on the V2 engine. */
class CancelledQueryFallbackTest {

  private final Settings settings = mock(Settings.class);
  private final ExecutionEngine engine = mock(ExecutionEngine.class);
  private final Analyzer analyzer = mock(Analyzer.class);
  private final Planner planner = mock(Planner.class);
  private final AtomicInteger legacyExecutions = new AtomicInteger();

  @SuppressWarnings("unchecked")
  private final ResponseListener<ExecutionEngine.QueryResponse> listener =
      mock(ResponseListener.class);

  @BeforeEach
  void setUp() {
    when(settings.getSettingValue(Settings.Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);
    when(settings.getSettingValue(Settings.Key.CALCITE_FALLBACK_ALLOWED)).thenReturn(true);
    when(settings.getSettingValue(Settings.Key.QUERY_SIZE_LIMIT)).thenReturn(10000);
    when(settings.getSettingValue(Settings.Key.PPL_SYNTAX_LEGACY_PREFERRED)).thenReturn(true);
    when(analyzer.analyze(any(), any())).thenReturn(mock(LogicalPlan.class));
    when(planner.plan(any())).thenReturn(mock(PhysicalPlan.class));
    doAnswer(
            invocation -> {
              legacyExecutions.incrementAndGet();
              return null;
            })
        .when(engine)
        .execute(any(PhysicalPlan.class), any(ExecutionContext.class), any());
  }

  @AfterEach
  void clearTask() {
    OpenSearchQueryManager.clearCancellableTask();
  }

  @Test
  void cancelledCalciteScanDoesNotStartLegacyEngine() {
    CancellableTask task = cancellableTask();
    doAnswer(
            invocation -> {
              OpenSearchQueryManager.setCancellableTask(task);
              OpenSearchIndexEnumerator scan =
                  new OpenSearchIndexEnumerator(
                      client(),
                      List.of("x"),
                      100,
                      100,
                      100,
                      mock(OpenSearchRequest.class),
                      monitor());
              task.cancel("async PPL query cancelled");
              scan.moveNext();
              return null;
            })
        .when(engine)
        .execute(any(RelNode.class), any(CalcitePlanContext.class), any());

    queryService().execute(mock(UnresolvedPlan.class), QueryType.PPL, listener);

    assertEquals(0, legacyExecutions.get());
    verify(analyzer, never()).analyze(any(), any());
    ArgumentCaptor<Exception> failure = ArgumentCaptor.forClass(Exception.class);
    verify(listener).onFailure(failure.capture());
    assertTrue(hasCause(failure.getValue(), TaskCancelledException.class));
  }

  @Test
  void unsupportedCalciteQueryStillFallsBackToLegacyEngine() {
    doThrow(new CalciteUnsupportedException("unsupported"))
        .when(engine)
        .execute(any(RelNode.class), any(CalcitePlanContext.class), any());

    queryService().execute(mock(UnresolvedPlan.class), QueryType.PPL, listener);

    assertEquals(1, legacyExecutions.get());
    verify(listener, never()).onFailure(any());
  }

  private QueryService queryService() {
    return new QueryService(analyzer, engine, planner, null, settings) {
      @Override
      public RelNode analyze(UnresolvedPlan ast, CalcitePlanContext context) {
        return context.relBuilder.values(new String[] {"x"}, 1).build();
      }
    };
  }

  private static OpenSearchClient client() {
    OpenSearchClient client = mock(OpenSearchClient.class);
    when(client.getNodeClient()).thenReturn(Optional.empty());
    return client;
  }

  private static ResourceMonitor monitor() {
    ResourceStatus healthy = mock(ResourceStatus.class);
    when(healthy.isHealthy()).thenReturn(true);
    ResourceMonitor monitor = mock(ResourceMonitor.class);
    when(monitor.getStatus()).thenReturn(healthy);
    return monitor;
  }

  private static CancellableTask cancellableTask() {
    return new CancellableTask(1, "transport", "ppl", "test", TaskId.EMPTY_TASK_ID, Map.of()) {
      @Override
      public boolean shouldCancelChildrenOnCancellation() {
        return true;
      }
    };
  }

  private static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (type.isInstance(cause)) {
        return true;
      }
      if (cause.getCause() == cause) {
        return false;
      }
    }
    return false;
  }
}
