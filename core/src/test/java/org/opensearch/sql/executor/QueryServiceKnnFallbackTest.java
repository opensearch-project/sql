/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.error.ErrorReport;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.exception.CalciteUnsupportedException;
import org.opensearch.sql.planner.Planner;

/**
 * The V2 engine cannot run a k-NN step, so a statement with one is not retried on it: not on a
 * {@link CalciteUnsupportedException}, which otherwise always falls back, and not with {@code
 * plugins.calcite.fallback.allowed}. The client gets the Calcite error either way, because a V2
 * attempt that fails reports the Calcite error (opensearch-project/sql#5060); skipping the attempt
 * saves the work, which these tests see as the V2 analyzer never being called.
 */
class QueryServiceKnnFallbackTest {

  /** {@code source=t | knn ...}; lowering it fails as an unsupported dedup below it would. */
  private static final UnresolvedPlan KNN_STATEMENT =
      AstDSL.knn(
          AstDSL.relation("t"),
          AstDSL.field("embedding"),
          AstDSL.vectorLiteral(1, 2, 3),
          AstDSL.argument("k", AstDSL.intLiteral(5)));

  private static final UnresolvedPlan PLAIN_STATEMENT = AstDSL.relation("t");

  private final Analyzer analyzer = mock(Analyzer.class);
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
    doThrow(new CalciteUnsupportedException("Consecutive deduplication is unsupported in Calcite"))
        .when(service)
        .analyze(any(UnresolvedPlan.class), any(CalcitePlanContext.class));
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

  private static Throwable unwrap(Throwable t) {
    return t instanceof ErrorReport report ? report.getCause() : t;
  }

  @Test
  void executeReportsTheCalciteErrorOfAKnnStatement() {
    service.execute(KNN_STATEMENT, QueryType.PPL, listener());
    verify(analyzer, never()).analyze(any(), any());
    assertInstanceOf(CalciteUnsupportedException.class, unwrap(failure.get()), failure::toString);
  }

  @Test
  void explainReportsTheCalciteErrorOfAKnnStatement() {
    service.explain(KNN_STATEMENT, QueryType.PPL, listener(), ExplainMode.STANDARD);
    verify(analyzer, never()).analyze(any(), any());
    assertInstanceOf(CalciteUnsupportedException.class, unwrap(failure.get()), failure::toString);
  }

  /** The setup does reach the fallback: the same error on a statement without k-NN falls back. */
  @Test
  void aStatementWithoutKnnStillFallsBack() {
    service.execute(PLAIN_STATEMENT, QueryType.PPL, listener());
    service.explain(PLAIN_STATEMENT, QueryType.PPL, listener(), ExplainMode.STANDARD);
    verify(analyzer, times(2)).analyze(any(), any());
  }
}
