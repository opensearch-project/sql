/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.opensearch.sql.executor.execution.QueryPlanFactory.NO_CONSUMER_RESPONSE_LISTENER;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.CloseCursor;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.QueryType;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class QueryPlanFactoryTest {

  @Mock private UnresolvedPlan plan;

  @Mock private QueryType queryType;

  @Mock private QueryService queryService;

  @Mock private ResponseListener<ExecutionEngine.QueryResponse> queryListener;

  @Mock private ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  @Mock private ExecutionEngine.QueryResponse queryResponse;

  private QueryPlanFactory factory;

  @BeforeEach
  void init() {
    factory = new QueryPlanFactory(queryService);
  }

  @Test
  public void create_from_query_should_success() {
    Statement query = new Query(plan, 0, queryType);
    AbstractPlan queryExecution = factory.create(query, queryListener, explainListener);
    assertTrue(queryExecution instanceof QueryPlan);
  }

  @Test
  public void create_from_explain_should_success() {
    Statement query = new Explain(new Query(plan, 0, queryType), queryType);
    AbstractPlan queryExecution = factory.create(query, queryListener, explainListener);
    assertTrue(queryExecution instanceof ExplainPlan);
  }

  @Test
  public void create_from_cursor_should_success() {
    AbstractPlan queryExecution =
        factory.create("", false, queryType, null, queryListener, explainListener);
    AbstractPlan explainExecution =
        factory.create("", true, queryType, null, queryListener, explainListener);
    assertAll(
        () -> assertTrue(queryExecution instanceof QueryPlan),
        () -> assertTrue(explainExecution instanceof ExplainPlan));
  }

  @Test
  public void no_consumer_response_channel() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> NO_CONSUMER_RESPONSE_LISTENER.onResponse(queryResponse));
    assertEquals(
        "[BUG] query response should not sent to unexpected channel", exception.getMessage());

    exception =
        assertThrows(
            IllegalStateException.class,
            () -> NO_CONSUMER_RESPONSE_LISTENER.onFailure(new RuntimeException()));
    assertEquals(
        "[BUG] exception response should not sent to unexpected channel", exception.getMessage());
  }

  @Test
  public void create_query_with_fetch_size_should_create_query_plan() {
    factory = new QueryPlanFactory(queryService);
    Statement query = new Query(plan, 10, queryType);
    AbstractPlan queryExecution = factory.create(query, queryListener, explainListener);
    assertTrue(queryExecution instanceof QueryPlan);
  }

  @Test
  public void create_query_with_fetch_size_and_offset_should_create_query_plan() {
    factory = new QueryPlanFactory(queryService);
    Statement query = new Query(plan, 10, queryType, 50);
    AbstractPlan queryExecution = factory.create(query, queryListener, explainListener);
    assertTrue(queryExecution instanceof QueryPlan);
  }

  @Test
  public void create_close_cursor() {
    factory = new QueryPlanFactory(queryService);
    var plan = factory.createCloseCursor("pewpew", queryType, queryListener);
    assertTrue(plan instanceof CommandPlan);
    plan.execute();
    var captor = ArgumentCaptor.forClass(UnresolvedPlan.class);
    verify(queryService).execute(captor.capture(), any(), any());
    assertTrue(captor.getValue() instanceof CloseCursor);
  }
}
