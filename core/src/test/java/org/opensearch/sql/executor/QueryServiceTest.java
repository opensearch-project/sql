/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
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
import org.opensearch.sql.storage.split.Split;
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

  @Mock
  private Split split;

  @BeforeEach
  public void setUp() {
    lenient().when(analyzer.analyze(any(), any())).thenReturn(logicalPlan);
    when(planner.plan(any())).thenReturn(plan);

    queryService = new QueryService(analyzer, executionEngine, planner);
  }

  @Test
  public void testExecuteShouldPass() {
    doAnswer(
            invocation -> {
              ResponseListener<ExecutionEngine.QueryResponse> listener = invocation.getArgument(2);
              listener.onResponse(
                  new ExecutionEngine.QueryResponse(schema, Collections.emptyList()));
              return null;
            })
        .when(executionEngine)
        .execute(any(), any(), any());

    queryService.execute(
        ast,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse pplQueryResponse) {

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
        .execute(any(), any(), any());

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
  public void testExecutePlan() {
    queryService()
        .executeSuccess();

    queryService()
        .executeSuccessWithSplit();
  }

  Helper queryService() {
    return new Helper();
  }

  class Helper {
    public Helper() {
      doAnswer(
          invocation -> {
            ResponseListener<ExecutionEngine.QueryResponse> listener = invocation.getArgument(2);
            listener.onResponse(
                new ExecutionEngine.QueryResponse(schema, Collections.emptyList()));
            return null;
          })
          .when(executionEngine)
          .execute(any(), any(), any());
    }

    void executeSuccess() {
      when(planContext.getSplit()).thenReturn(Optional.empty());
      executeInternal();
    }

    void executeSuccessWithSplit() {
      when(planContext.getSplit()).thenReturn(Optional.of(split));
      executeInternal();
    }

    private void executeInternal() {
      queryService.executePlan(
          logicalPlan,
          planContext,
          new ResponseListener<>() {
            @Override
            public void onResponse(ExecutionEngine.QueryResponse pplQueryResponse) {

            }

            @Override
            public void onFailure(Exception e) {
              fail();
            }
          });
    }

  }
}
