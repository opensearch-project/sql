/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
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

  @Mock private Explain.ExplainFormat format;

  @Test
  public void execute_no_page_size() {
    QueryPlan query = new QueryPlan(queryId, queryType, plan, queryService, queryListener);
    query.execute();

    verify(queryService, times(1)).execute(any(), any(), any());
  }

  @Test
  public void explain_no_page_size() {
    QueryPlan query = new QueryPlan(queryId, queryType, plan, queryService, queryListener);
    query.explain(explainListener, format);

    verify(queryService, times(1)).explain(plan, queryType, explainListener, format);
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
            format);
  }
}
