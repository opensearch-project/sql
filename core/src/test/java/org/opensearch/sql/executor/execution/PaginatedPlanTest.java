/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.commons.lang3.NotImplementedException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.DefaultExecutionEngine;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPaginate;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class PaginatedPlanTest {

  private static QueryService queryService;

  /**
   * Initialize the mocks.
   */
  @BeforeAll
  public static void setUp() {
    var analyzer = mock(Analyzer.class);
    when(analyzer.analyze(any(), any())).thenReturn(mock(LogicalPaginate.class));
    var planner = mock(Planner.class);
    when(planner.plan(any())).thenReturn(mock(PhysicalPlan.class));
    queryService = new QueryService(analyzer, new DefaultExecutionEngine(), planner);
  }

  @Test
  public void can_execute_plan() {
    var listener = new ResponseListener<ExecutionEngine.QueryResponse>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        assertNotNull(response);
      }

      @Override
      public void onFailure(Exception e) {
        fail();
      }
    };
    var plan = new PaginatedPlan(QueryId.queryId(), mock(UnresolvedPlan.class), 10,
        queryService, listener);
    plan.execute();
  }

  @Test
  // Same as previous test, but with incomplete PaginatedQueryService
  public void can_handle_error_while_executing_plan() {
    var listener = new ResponseListener<ExecutionEngine.QueryResponse>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        fail();
      }

      @Override
      public void onFailure(Exception e) {
        assertNotNull(e);
      }
    };
    var plan = new PaginatedPlan(QueryId.queryId(), mock(UnresolvedPlan.class), 10,
        new QueryService(null, new DefaultExecutionEngine(), null), listener);
    plan.execute();
  }

  @Test
  public void explain_is_not_supported() {
    new PaginatedPlan(null, null, 0, null, null).explain(new ResponseListener<>() {
        @Override
        public void onResponse(ExecutionEngine.ExplainResponse response) {
          fail();
        }

        @Override
        public void onFailure(Exception e) {
          assertTrue(e instanceof NotImplementedException);
        }
      });
  }
}
