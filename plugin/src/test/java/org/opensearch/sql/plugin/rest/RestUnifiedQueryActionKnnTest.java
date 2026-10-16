/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.Frameworks;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.ast.dsl.AstDSL;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.knn.KnnErrors;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.executor.DirectExecutionDispatcher;
import org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.plugin.transport.TransportPPLQueryResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/**
 * A k-NN step is rejected on the analytics-engine route through execute and explain, by the check
 * the planner runs on the AST it parses. No grammar has {@code knn} yet, so the context's parser is
 * replaced by one that returns the AST of {@code source=parquet_logs | knn field=embedding
 * vector=[1, 2, 3] k=5}; everything else is the production path.
 */
public class RestUnifiedQueryActionKnnTest {

  private static final String QUERY =
      "source=parquet_logs | knn field=embedding vector=[1,2,3] k=5";

  private RestUnifiedQueryAction action;
  private final List<String> parsedQueries = new ArrayList<>();

  @Before
  public void setUp() {
    NodeClient client = mock(NodeClient.class);
    ThreadPool threadPool = mock(ThreadPool.class);
    when(client.threadPool()).thenReturn(threadPool);
    // Run the scheduled planning task on the calling thread.
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArgument(0)).run();
              return null;
            })
        .when(threadPool)
        .schedule(any(Runnable.class), any(TimeValue.class), anyString());
    ClusterService clusterService = mock(ClusterService.class);
    when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
    EngineContextProvider contextProvider = mock(EngineContextProvider.class);
    when(contextProvider.getContext())
        .thenReturn(
            new QueryRequestContext(mock(ClusterState.class), Frameworks.createRootSchema(true)));
    @SuppressWarnings("unchecked")
    QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = mock(QueryPlanExecutor.class);

    action =
        new RestUnifiedQueryAction(
            client,
            clusterService,
            executor,
            contextProvider,
            mock(org.opensearch.sql.common.setting.Settings.class),
            new DirectExecutionDispatcher()) {
          @Override
          UnifiedQueryContext buildContext(
              QueryType queryType, boolean profiling, QueryRequestContext queryCtx) {
            UnifiedQueryContext context = super.buildContext(queryType, profiling, queryCtx);
            return new UnifiedQueryContext(
                context.getPlanContext(),
                context.getSettings(),
                query -> {
                  parsedQueries.add(query);
                  return knnStatement();
                },
                context.getLangSpec());
          }
        };
  }

  private static UnresolvedPlan knnStatement() {
    return AstDSL.knn(
        AstDSL.relation("parquet_logs"),
        AstDSL.field("embedding"),
        AstDSL.vectorLiteral(1, 2, 3),
        AstDSL.argument("k", AstDSL.intLiteral(5)));
  }

  @Test
  public void executeRejectsKnnBeforeLowering() {
    AtomicReference<Exception> failure = new AtomicReference<>();
    AtomicReference<TransportPPLQueryResponse> response = new AtomicReference<>();
    action.execute(QUERY, QueryType.PPL, false, ActionListener.wrap(response::set, failure::set));

    assertNull(response.get());
    assertRejected(failure.get());
  }

  @Test
  public void explainRejectsKnnBeforeLowering() {
    AtomicReference<Exception> failure = new AtomicReference<>();
    action.explain(
        QUERY,
        QueryType.PPL,
        ExplainMode.STANDARD,
        new ResponseListener<ExplainResponse>() {
          @Override
          public void onResponse(ExplainResponse explainResponse) {
            throw new AssertionError("explain must fail");
          }

          @Override
          public void onFailure(Exception e) {
            failure.set(e);
          }
        });

    assertRejected(failure.get());
  }

  /**
   * The error comes from the planner's statement check: the query was parsed once, and lowering,
   * which would fail with "table not found" for this empty schema, never ran.
   */
  private void assertRejected(Exception failure) {
    assertNotNull("the query must fail", failure);
    assertTrue(failure.toString(), failure instanceof SemanticCheckException);
    assertEquals(KnnErrors.ANALYTICS_ENGINE_NOT_SUPPORTED_MESSAGE, failure.getMessage());
    assertEquals(List.of(QUERY), parsedQueries);
  }
}
