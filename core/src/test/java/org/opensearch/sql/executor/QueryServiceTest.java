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

import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.analysis.Analyzer;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.planner.PlanContext;
import org.opensearch.sql.planner.Planner;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.split.Split;

@ExtendWith(MockitoExtension.class)
class QueryServiceTest {

  private QueryService queryService;

  @Mock private ExecutionEngine executionEngine;

  @Mock private Settings settings;

  @Mock private Analyzer analyzer;

  @Mock private Planner planner;

  @Mock private UnresolvedPlan ast;

  @Mock private QueryType queryType;

  @Mock private LogicalPlan logicalPlan;

  @Mock private PhysicalPlan plan;

  @Mock private ExecutionEngine.Schema schema;

  @Mock private PlanContext planContext;

  @Mock private Split split;

  private final Explain.ExplainFormat format = Explain.ExplainFormat.STANDARD;

  @Test
  public void executeWithoutContext() {
    queryService().executeSuccess().handledByOnResponse();
  }

  @Test
  public void executeWithContext() {
    queryService().executeSuccess(split).handledByOnResponse();
  }

  @Test
  public void testExplainShouldPass() {
    queryService().explainSuccess().handledByExplainOnResponse();
  }

  @Test
  public void testExecuteWithExceptionShouldBeCaughtByHandler() {
    queryService().executeFail().handledByOnFailure();
  }

  @Test
  public void explainWithIllegalQueryShouldBeCaughtByHandler() {
    queryService().explainFail().handledByExplainOnFailure();
  }

  @Test
  public void analyzeExceptionShouldBeCached() {
    queryService().analyzeFail().handledByOnFailure();
  }

  Helper queryService() {
    return new Helper();
  }

  class Helper {

    Optional<Split> split = Optional.empty();

    public Helper() {
      lenient().when(analyzer.analyze(any(), any())).thenReturn(logicalPlan);
      lenient().when(planner.plan(any())).thenReturn(plan);
      lenient().when(settings.getSettingValue(Key.QUERY_SIZE_LIMIT)).thenReturn(200);
      lenient().when(settings.getSettingValue(Key.CALCITE_ENGINE_ENABLED)).thenReturn(false);

      queryService = new QueryService(analyzer, executionEngine, planner, settings);
    }

    Helper executeSuccess() {
      executeSuccess(null);

      return this;
    }

    Helper executeSuccess(Split split) {
      this.split = Optional.ofNullable(split);
      doAnswer(
              invocation -> {
                ResponseListener<ExecutionEngine.QueryResponse> listener =
                    invocation.getArgument(2);
                listener.onResponse(
                    new ExecutionEngine.QueryResponse(
                        schema, Collections.emptyList(), Cursor.None));
                return null;
              })
          .when(executionEngine)
          .execute(any(PhysicalPlan.class), any(), any());
      lenient().when(planContext.getSplit()).thenReturn(this.split);

      return this;
    }

    Helper analyzeFail() {
      doThrow(new IllegalStateException("analyze exception")).when(analyzer).analyze(any(), any());

      return this;
    }

    Helper executeFail() {
      doThrow(new IllegalStateException("illegal state exception"))
          .when(executionEngine)
          .execute(any(PhysicalPlan.class), any(), any());

      return this;
    }

    Helper explainSuccess() {
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

      return this;
    }

    Helper explainFail() {
      doThrow(new IllegalStateException("illegal state exception"))
          .when(executionEngine)
          .explain(any(), any());

      return this;
    }

    void handledByOnResponse() {
      ResponseListener<ExecutionEngine.QueryResponse> responseListener =
          new ResponseListener<>() {
            @Override
            public void onResponse(ExecutionEngine.QueryResponse pplQueryResponse) {
              assertNotNull(pplQueryResponse);
            }

            @Override
            public void onFailure(Exception e) {
              fail();
            }
          };
      split.ifPresentOrElse(
          split -> queryService.executePlan(logicalPlan, planContext, responseListener),
          () -> queryService.execute(ast, queryType, responseListener));
    }

    void handledByOnFailure() {
      ResponseListener<ExecutionEngine.QueryResponse> responseListener =
          new ResponseListener<>() {
            @Override
            public void onResponse(ExecutionEngine.QueryResponse pplQueryResponse) {
              fail();
            }

            @Override
            public void onFailure(Exception e) {
              assertTrue(e instanceof IllegalStateException);
            }
          };
      split.ifPresentOrElse(
          split -> queryService.executePlan(logicalPlan, planContext, responseListener),
          () -> queryService.execute(ast, queryType, responseListener));
    }

    void handledByExplainOnResponse() {
      queryService.explain(
          ast,
          queryType,
          new ResponseListener<>() {
            @Override
            public void onResponse(ExecutionEngine.ExplainResponse pplQueryResponse) {
              assertNotNull(pplQueryResponse);
            }

            @Override
            public void onFailure(Exception e) {
              fail();
            }
          },
          format);
    }

    void handledByExplainOnFailure() {
      queryService.explain(
          ast,
          queryType,
          new ResponseListener<>() {
            @Override
            public void onResponse(ExecutionEngine.ExplainResponse pplQueryResponse) {
              fail();
            }

            @Override
            public void onFailure(Exception e) {
              assertTrue(e instanceof IllegalStateException);
            }
          },
          format);
    }
  }
}
