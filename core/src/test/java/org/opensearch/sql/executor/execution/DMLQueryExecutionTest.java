/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;

@ExtendWith(MockitoExtension.class)
class DMLQueryExecutionTest {

  @Mock
  private QueryId queryId;

  @Mock
  private UnresolvedPlan plan;

  @Mock
  private QueryService queryService;

  @Mock
  private ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  @Mock
  private ResponseListener<ExecutionEngine.QueryResponse> queryResponseListener;

  @Test
  public void startQueryExecution() {
    DMLQueryExecution queryExecution = new DMLQueryExecution(queryId, plan, false, queryService);
    queryExecution.registerListener(queryResponseListener);
    queryExecution.start();

    verify(queryService, times(1)).execute(any(), any());
  }

  @Test
  public void startQueryExecutionWithoutListener() {
    DMLQueryExecution queryExecution = new DMLQueryExecution(queryId, plan, false, queryService);
    queryExecution.start();

    verify(queryService, never()).execute(any(), any());
  }

  @Test
  public void startExplainExecution() {
    DMLQueryExecution queryExecution = new DMLQueryExecution(queryId, plan, true, queryService);
    queryExecution.registerListener(explainListener);
    queryExecution.start();

    verify(queryService, times(1)).explain(any(), any());
  }

  @Test
  public void startExplainExecutionWithoutListener() {
    DMLQueryExecution queryExecution = new DMLQueryExecution(queryId, plan, true, queryService);
    queryExecution.start();

    verify(queryService, never()).explain(any(), any());
  }
}
