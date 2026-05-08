/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@ExtendWith(MockitoExtension.class)
class DelegatingExecutionEngineTest {

  @Mock private ExecutionEngine defaultEngine;

  @Mock private ExecutionEngine extension1;

  @Mock private ExecutionEngine extension2;

  @Mock private RelNode relNode;

  @Mock private CalcitePlanContext calciteContext;

  @Mock private PhysicalPlan physicalPlan;

  @Mock private ExecutionContext executionContext;

  @Mock private ResponseListener<ExecutionEngine.QueryResponse> queryListener;

  @Mock private ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  @Test
  void executeRelNodeRoutesToMatchingExtension() {
    when(extension1.canVectorize(relNode)).thenReturn(true);
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1, extension2));

    engine.execute(relNode, calciteContext, queryListener);

    verify(extension1).execute(relNode, calciteContext, queryListener);
    verify(defaultEngine, never()).execute(any(RelNode.class), any(), eq(queryListener));
  }

  @Test
  void executeRelNodeFallsBackToDefaultWhenNoExtensionMatches() {
    when(extension1.canVectorize(relNode)).thenReturn(false);
    when(extension2.canVectorize(relNode)).thenReturn(false);
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1, extension2));

    engine.execute(relNode, calciteContext, queryListener);

    verify(defaultEngine).execute(relNode, calciteContext, queryListener);
    verify(extension1, never()).execute(any(RelNode.class), any(), eq(queryListener));
    verify(extension2, never()).execute(any(RelNode.class), any(), eq(queryListener));
  }

  @Test
  void executeRelNodeRoutesToFirstMatchingExtension() {
    when(extension1.canVectorize(relNode)).thenReturn(true);
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1, extension2));

    engine.execute(relNode, calciteContext, queryListener);

    verify(extension1).execute(relNode, calciteContext, queryListener);
    verify(extension2, never()).execute(any(RelNode.class), any(), eq(queryListener));
  }

  @Test
  void explainRelNodeRoutesToMatchingExtension() {
    when(extension1.canVectorize(relNode)).thenReturn(true);
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1));

    engine.explain(relNode, ExplainMode.STANDARD, calciteContext, explainListener);

    verify(extension1).explain(relNode, ExplainMode.STANDARD, calciteContext, explainListener);
    verify(defaultEngine, never()).explain(any(RelNode.class), any(), any(), eq(explainListener));
  }

  @Test
  void explainRelNodeFallsBackToDefaultWhenNoExtensionMatches() {
    when(extension1.canVectorize(relNode)).thenReturn(false);
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1));

    engine.explain(relNode, ExplainMode.STANDARD, calciteContext, explainListener);

    verify(defaultEngine).explain(relNode, ExplainMode.STANDARD, calciteContext, explainListener);
  }

  @Test
  void canVectorizeReturnsTrueWhenExtensionMatches() {
    when(extension1.canVectorize(relNode)).thenReturn(false);
    when(extension2.canVectorize(relNode)).thenReturn(true);
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1, extension2));

    assert engine.canVectorize(relNode);
  }

  @Test
  void canVectorizeReturnsFalseWhenNoExtensionMatches() {
    when(extension1.canVectorize(relNode)).thenReturn(false);
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1));

    assert !engine.canVectorize(relNode);
  }

  @Test
  void physicalPlanExecuteDelegatesToDefault() {
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1));

    engine.execute(physicalPlan, queryListener);

    verify(defaultEngine).execute(physicalPlan, queryListener);
  }

  @Test
  void physicalPlanExecuteWithContextDelegatesToDefault() {
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1));

    engine.execute(physicalPlan, executionContext, queryListener);

    verify(defaultEngine).execute(physicalPlan, executionContext, queryListener);
  }

  @Test
  void physicalPlanExplainDelegatesToDefault() {
    DelegatingExecutionEngine engine =
        new DelegatingExecutionEngine(defaultEngine, List.of(extension1));

    engine.explain(physicalPlan, explainListener);

    verify(defaultEngine).explain(physicalPlan, explainListener);
  }

  @Test
  void emptyExtensionsListAlwaysFallsBackToDefault() {
    DelegatingExecutionEngine engine = new DelegatingExecutionEngine(defaultEngine, List.of());

    engine.execute(relNode, calciteContext, queryListener);

    verify(defaultEngine).execute(relNode, calciteContext, queryListener);
  }
}
