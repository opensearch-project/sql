/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.DefaultExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class QueryPlanTest {

  @Mock private QueryId queryId;

  @Mock private QueryType queryType;

  @Mock private UnresolvedPlan plan;

  @Mock private QueryService queryService;

  @Mock private ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  @Mock private ResponseListener<ExecutionEngine.QueryResponse> queryListener;

  @Mock private ExplainMode mode;

  @Test
  public void execute_no_page_size() {
    QueryPlan query = new QueryPlan(queryId, queryType, plan, queryService, queryListener);
    query.execute();

    verify(queryService, times(1)).execute(any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void warnings_supported_flag_reaches_execution_thread() throws InterruptedException {
    QueryPlan query = new QueryPlan(queryId, queryType, plan, queryService, queryListener);
    query.setWarningsSupported(true);

    // Configure the plan here but run execute() on another thread, mirroring the transport->worker
    // handoff. The flag must ride the plan object, not a thread-local the handoff can drop.
    AtomicBoolean defaultedBeforeExecute = new AtomicBoolean(true);
    AtomicBoolean seenOnWorker = new AtomicBoolean(false);
    Thread worker =
        new Thread(
            () -> {
              defaultedBeforeExecute.set(CalcitePlanContext.isWarningsSupported());
              query.execute();
              seenOnWorker.set(CalcitePlanContext.isWarningsSupported());
            });
    worker.start();
    worker.join();

    assertFalse(
        defaultedBeforeExecute.get(), "worker thread should default to no warnings support");
    assertTrue(seenOnWorker.get(), "execute() must carry warningsSupported onto the worker thread");
    verify(queryService, times(1)).execute(any(), any(), any(), anyBoolean(), any());
  }

  @Test
  public void warnings_unsupported_plan_resets_flag_on_reused_worker_thread()
      throws InterruptedException {
    // Plan defaults to warningsSupported=false.
    QueryPlan query = new QueryPlan(queryId, queryType, plan, queryService, queryListener);

    AtomicBoolean seenOnWorker = new AtomicBoolean(true);
    Thread worker =
        new Thread(
            () -> {
              // Simulate a pooled worker left "supported" by a prior query.
              CalcitePlanContext.setWarningsSupported(true);
              query.execute();
              seenOnWorker.set(CalcitePlanContext.isWarningsSupported());
            });
    worker.start();
    worker.join();

    assertFalse(
        seenOnWorker.get(), "a warnings-unsupported plan must reset the flag on a reused thread");
  }

  @Test
  public void explain_no_page_size() {
    QueryPlan query = new QueryPlan(queryId, queryType, plan, queryService, queryListener);
    query.explain(explainListener, mode, null);

    verify(queryService, times(1))
        .explain(
            eq(plan),
            eq(queryType),
            isNull(),
            anyBoolean(),
            eq(explainListener),
            eq(mode),
            isNull());
  }

  @Test
  public void can_execute_paginated_plan() {
    var listener =
        new ResponseListener<ExecutionEngine.QueryResponse>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            assertNotNull(response);
          }

          @Override
          public void onFailure(Exception e) {
            fail();
          }
        };
    var plan =
        new QueryPlan(
            QueryId.queryId(), queryType, mock(UnresolvedPlan.class), 10, queryService, listener);
    plan.execute();
  }

  @Test
  // Same as previous test, but with incomplete QueryService
  public void can_handle_error_while_executing_plan() {
    var listener =
        new ResponseListener<ExecutionEngine.QueryResponse>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            fail();
          }

          @Override
          public void onFailure(Exception e) {
            assertNotNull(e);
          }
        };
    var plan =
        new QueryPlan(
            QueryId.queryId(),
            queryType,
            mock(UnresolvedPlan.class),
            10,
            new QueryService(null, new DefaultExecutionEngine(), null),
            listener);
    plan.execute();
  }

  @Test
  public void explain_is_not_supported_for_pagination() {
    new QueryPlan(null, null, null, 0, null, null)
        .explain(
            new ResponseListener<>() {
              @Override
              public void onResponse(ExecutionEngine.ExplainResponse response) {
                fail();
              }

              @Override
              public void onFailure(Exception e) {
                assertTrue(e instanceof NotImplementedException);
              }
            },
            mode);
  }
}
