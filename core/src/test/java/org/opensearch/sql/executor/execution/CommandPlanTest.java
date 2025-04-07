/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.planner.logical.LogicalPlan;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class CommandPlanTest {

  @Test
  public void execute_without_error() {
    QueryService qs = mock(QueryService.class);
    ResponseListener listener = mock(ResponseListener.class);
    doNothing().when(qs).execute(any(), any(), any());

    new CommandPlan(
            QueryId.queryId(), mock(QueryType.class), mock(UnresolvedPlan.class), qs, listener)
        .execute();

    verify(qs).execute(any(), any(), any());
    verify(listener, never()).onFailure(any());
  }

  @Test
  public void execute_with_error() {
    QueryService qs = mock(QueryService.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
    ResponseListener listener = mock(ResponseListener.class);
    doThrow(new RuntimeException()).when(qs).executePlan(any(LogicalPlan.class), any(), any());

    new CommandPlan(
            QueryId.queryId(), mock(QueryType.class), mock(UnresolvedPlan.class), qs, listener)
        .execute();

    verify(listener).onFailure(any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void explain_not_supported() {
    QueryService qs = mock(QueryService.class);
    ResponseListener listener = mock(ResponseListener.class);
    ResponseListener explainListener = mock(ResponseListener.class);

    var exception =
        assertThrows(
            Throwable.class,
            () ->
                new CommandPlan(
                        QueryId.queryId(),
                        mock(QueryType.class),
                        mock(UnresolvedPlan.class),
                        qs,
                        listener)
                    .explain(explainListener, Explain.ExplainFormat.STANDARD));
    assertEquals("CommandPlan does not support explain", exception.getMessage());

    verify(listener, never()).onResponse(any());
    verify(listener, never()).onFailure(any());
    verify(explainListener, never()).onResponse(any());
    verify(explainListener, never()).onFailure(any());
  }
}
