/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@ExtendWith(MockitoExtension.class)
class QueryServiceTest {
  private AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  private QueryService queryService;

  @Mock
  private ExecutionEngine executionEngine;

  @Mock
  private Analyzer analyzer;

  @Mock
  private Planner planner;

  @Mock
  private UnresolvedPlan ast;

  @Mock
  private LogicalPlan logicalPlan;

  @Mock
  private PhysicalPlan plan;

  @Mock
  private ExecutionEngine.Schema schema;

  @Mock
  private PlanContext planContext;

  @BeforeEach
  public void setUp() {
    lenient().when(analyzer.analyze(any(), any())).thenReturn(logicalPlan);
    lenient().when(planner.plan(any())).thenReturn(plan);

    queryService = new QueryService(analyzer, executionEngine, planner);
  }

  @Test
  public void testExecuteShouldPass() {
    doAnswer(
            invocation -> {
              ResponseListener<ExecutionEngine.QueryResponse> listener = invocation.getArgument(1);
              listener.onResponse(
                  new ExecutionEngine.QueryResponse(schema, Collections.emptyList()));
              return null;
            })
        .when(executionEngine)
        .execute(any(), any());

    queryService.execute(
        ast,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse pplQueryResponse) {
            assertNotNull(pplQueryResponse);
          }

          @Override
          public void onFailure(Exception e) {
            fail();
          }
        });
  }

  @Test
  public void testExplainShouldPass() {
    doAnswer(
            invocation -> {
              ResponseListener<ExecutionEngine.ExplainResponse> listener =
                  invocation.getArgument(1);
              listener.onResponse(
                  new ExecutionEngine.ExplainResponse(
                      new ExecutionEngine.ExplainResponseNode("test")));
              return null;
            })
        .when(executionEngine)
        .explain(any(), any());

    queryService.explain(
        ast,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.ExplainResponse pplQueryResponse) {
            assertNotNull(pplQueryResponse);
          }

          @Override
          public void onFailure(Exception e) {
            fail();
          }
        });
  }

  @Test
  public void testExecuteWithExceptionShouldBeCaughtByHandler() {
    doThrow(new IllegalStateException("illegal state exception"))
        .when(executionEngine)
        .execute(any(), any());

    queryService.execute(
        ast,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse pplQueryResponse) {
            fail();
          }

          @Override
          public void onFailure(Exception e) {
            assertTrue(e instanceof IllegalStateException);
          }
        });
  }

  @Test
  public void testExecuteWithIllegalQueryShouldBeCaughtByHandler() {
    doThrow(new IllegalStateException("illegal state exception"))
        .when(executionEngine)
        .explain(any(), any());

    queryService.explain(
        ast,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.ExplainResponse pplQueryResponse) {
            fail();
          }

          @Override
          public void onFailure(Exception e) {
            assertTrue(e instanceof IllegalStateException);
          }
        });
  }

  @Test
  public void testExecutePlanShouldPass() {
    doAnswer(
        invocation -> {
          ResponseListener<ExecutionEngine.QueryResponse> listener = invocation.getArgument(1);
          listener.onResponse(
              new ExecutionEngine.QueryResponse(schema, Collections.emptyList()));
          return null;
        })
        .when(executionEngine)
        .execute(any(), any());

    queryService.executePlan(
        logicalPlan,
        planContext,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse pplQueryResponse) {
            assertNotNull(pplQueryResponse);
          }

          @Override
          public void onFailure(Exception e) {
            fail();
          }
        });
  }

  @Test
  public void analyzeExceptionShouldBeCached() {
    when(analyzer.analyze(any(), any())).thenThrow(IllegalStateException.class);

    queryService.execute(
        ast,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse pplQueryResponse) {
            fail();
          }

          @Override
          public void onFailure(Exception e) {
            assertTrue(e instanceof IllegalStateException);
          }
        });
  }
}
