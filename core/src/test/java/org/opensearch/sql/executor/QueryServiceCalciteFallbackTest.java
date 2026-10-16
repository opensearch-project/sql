/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.planner.Planner;

/**
 * The Calcite-to-V2 fallback decision of {@link QueryService}: an error marked with {@link
 * SemanticCheckException#withoutCalciteFallback()} reaches the client from both execute and
 * explain, even when fallback is allowed; any other error still falls back.
 */
class QueryServiceCalciteFallbackTest {

  private final Analyzer analyzer = mock(Analyzer.class);
  private final UnresolvedPlan plan = mock(UnresolvedPlan.class);
  private final AtomicReference<Exception> failure = new AtomicReference<>();
  private QueryService service;

  @BeforeEach
  void setUp() {
    Settings settings = mock(Settings.class);
    lenient().when(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(true);
    lenient().when(settings.getSettingValue(Key.CALCITE_FALLBACK_ALLOWED)).thenReturn(true);
    lenient().when(settings.getSettingValue(Key.QUERY_SIZE_LIMIT)).thenReturn(10000);
    lenient().when(settings.getSettingValue(Key.PPL_SUBSEARCH_MAXOUT)).thenReturn(0);
    lenient().when(settings.getSettingValue(Key.PPL_JOIN_SUBSEARCH_MAXOUT)).thenReturn(0);
    lenient().when(settings.getSettingValue(Key.PPL_SYNTAX_LEGACY_PREFERRED)).thenReturn(true);
    // The V2 engine fails too, so a fallback is seen as a call to its analyzer.
    lenient().when(analyzer.analyze(any(), any())).thenThrow(new IllegalStateException("v2"));
    service =
        spy(
            new QueryService(
                analyzer, mock(ExecutionEngine.class), mock(Planner.class), null, settings));
  }

  private void calciteFailsWith(RuntimeException error) {
    doThrow(error).when(service).analyze(any(UnresolvedPlan.class), any(CalcitePlanContext.class));
  }

  private <T> ResponseListener<T> listener() {
    return new ResponseListener<>() {
      @Override
      public void onResponse(T response) {
        throw new AssertionError("no response expected");
      }

      @Override
      public void onFailure(Exception e) {
        failure.set(e);
      }
    };
  }

  private void execute() {
    service.execute(plan, QueryType.PPL, listener());
  }

  private void explain() {
    service.explain(plan, QueryType.PPL, listener(), ExplainMode.STANDARD);
  }

  @Test
  void markedErrorIsReportedFromExecute() {
    SemanticCheckException error =
        new SemanticCheckException("vector error").withoutCalciteFallback();
    calciteFailsWith(error);
    execute();
    verify(analyzer, never()).analyze(any(), any());
    assertTrue(SemanticCheckException.preventsCalciteFallback(failure.get()), failure::toString);
    assertSame(error, rootSemanticCheckError(failure.get()));
  }

  @Test
  void markedErrorIsReportedFromExplain() {
    SemanticCheckException error =
        new SemanticCheckException("vector error").withoutCalciteFallback();
    calciteFailsWith(error);
    explain();
    verify(analyzer, never()).analyze(any(), any());
    assertSame(error, failure.get());
  }

  /** The setup does reach the fallback: an unmarked error is retried on the V2 engine. */
  @Test
  void unmarkedErrorFallsBackFromExecuteAndExplain() {
    calciteFailsWith(new SemanticCheckException("other error"));
    execute();
    explain();
    verify(analyzer, times(2)).analyze(any(), any());
    assertFalse(SemanticCheckException.preventsCalciteFallback(failure.get()));
  }

  private static Throwable rootSemanticCheckError(Throwable t) {
    for (Throwable cause = t; cause != null; cause = cause.getCause()) {
      if (cause instanceof SemanticCheckException) {
        return cause;
      }
    }
    return t;
  }
}
