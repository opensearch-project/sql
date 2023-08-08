/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.streaming.StreamingSource;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.planner.logical.LogicalPlanDSL;
import org.opensearch.sql.storage.Table;

@ExtendWith(MockitoExtension.class)
class StreamingQueryPlanTest {

  static final String tableName = "mock";

  @Mock private QueryService queryService;

  @Mock private QueryId queryId;

  @Mock private StreamingQueryPlan.ExecutionStrategy executionStrategy;

  @Mock private ResponseListener<ExecutionEngine.QueryResponse> listener;

  @Mock private UnresolvedPlan unresolvedPlan;

  @Mock private Table table;

  @Mock private StreamingSource streamingSource;

  @Test
  void executionSuccess() throws InterruptedException {
    streamingQuery().streamingSource().shouldSuccess();
  }

  @Test
  void failIfNoRelation() throws InterruptedException {
    streamingQuery()
        .withoutSource()
        .shouldFail("Could find relation plan, LogicalValues does not have child node.");
  }

  @Test
  void failIfNoStreamingSource() throws InterruptedException {
    streamingQuery()
        .nonStreamingSource()
        .shouldFail(String.format("table %s could not been used as streaming source.", tableName));
  }

  @Test
  void taskExecutionShouldNotCallListener() throws InterruptedException {
    streamingQuery().streamingSource().taskExecutionShouldNotCallListener();
  }

  Helper streamingQuery() {
    return new Helper();
  }

  class Helper {

    private StreamingQueryPlan queryPlan;

    public Helper() {
      queryPlan =
          new StreamingQueryPlan(
              queryId, unresolvedPlan, queryService, listener, executionStrategy);
    }

    Helper streamingSource() {
      when(table.asStreamingSource()).thenReturn(streamingSource);
      when(queryService.analyze(any()))
          .thenReturn(
              LogicalPlanDSL.project(
                  LogicalPlanDSL.relation(tableName, table),
                  DSL.named("integer_value", DSL.ref("integer_value", INTEGER))));
      return this;
    }

    Helper nonStreamingSource() {
      when(table.asStreamingSource()).thenThrow(UnsupportedOperationException.class);
      when(queryService.analyze(any())).thenReturn(LogicalPlanDSL.relation(tableName, table));

      return this;
    }

    Helper withoutSource() {
      when(queryService.analyze(any())).thenReturn(LogicalPlanDSL.values());

      return this;
    }

    void shouldSuccess() throws InterruptedException {
      queryPlan.execute();
      verify(executionStrategy).execute(any());
      verify(listener, never()).onFailure(any());
      verify(listener, never()).onResponse(any());
    }

    void shouldFail(String expectedException) throws InterruptedException {
      queryPlan.execute();
      verify(executionStrategy, never()).execute(any());
      ArgumentCaptor<Exception> argument = ArgumentCaptor.forClass(Exception.class);
      verify(listener).onFailure(argument.capture());
      assertEquals(expectedException, argument.getValue().getMessage());
    }

    void taskExecutionShouldNotCallListener() throws InterruptedException {
      doThrow(InterruptedException.class).when(executionStrategy).execute(any());
      queryPlan.execute();
      verify(listener, never()).onFailure(any());
      verify(listener, never()).onResponse(any());
    }
  }
}
