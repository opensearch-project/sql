/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.executor.execution.QueryPlanFactory.NO_CONSUMER_RESPONSE_LISTENER;

import java.util.Optional;
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
import org.opensearch.sql.exception.UnsupportedCursorRequestException;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;
import org.opensearch.sql.executor.pagination.CanPaginateVisitor;

@ExtendWith(MockitoExtension.class)
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class QueryPlanFactoryTest {

  @Mock
  private UnresolvedPlan plan;

  @Mock
  private QueryService queryService;

  @Mock
  private ResponseListener<ExecutionEngine.QueryResponse> queryListener;

  @Mock
  private ResponseListener<ExecutionEngine.ExplainResponse> explainListener;

  @Mock
  private ExecutionEngine.QueryResponse queryResponse;

  private QueryPlanFactory factory;

  @BeforeEach
  void init() {
    factory = new QueryPlanFactory(queryService);
  }

  @Test
  public void create_from_query_should_success() {
    Statement query = new Query(plan, 0);
    AbstractPlan queryExecution =
        factory.create(query, Optional.of(queryListener), Optional.empty());
    assertTrue(queryExecution instanceof QueryPlan);
  }

  @Test
  public void create_from_explain_should_success() {
    Statement query = new Explain(new Query(plan, 0));
    AbstractPlan queryExecution =
        factory.create(query, Optional.empty(), Optional.of(explainListener));
    assertTrue(queryExecution instanceof ExplainPlan);
  }

  @Test
  public void create_from_cursor_should_success() {
    AbstractPlan queryExecution = factory.create("", false,
        queryListener, explainListener);
    AbstractPlan explainExecution = factory.create("", true,
        queryListener, explainListener);
    assertAll(
        () -> assertTrue(queryExecution instanceof QueryPlan),
        () -> assertTrue(explainExecution instanceof ExplainPlan)
    );
  }

  @Test
  public void create_from_query_without_query_listener_should_throw_exception() {
    Statement query = new Query(plan, 0);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(
            query, Optional.empty(), Optional.empty()));
    assertEquals("[BUG] query listener must be not null", exception.getMessage());
  }

  @Test
  public void create_from_explain_without_explain_listener_should_throw_exception() {
    Statement query = new Explain(new Query(plan, 0));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(
            query, Optional.empty(), Optional.empty()));
    assertEquals("[BUG] explain listener must be not null", exception.getMessage());
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
  public void create_query_with_fetch_size_which_can_be_paged() {
    when(plan.accept(any(CanPaginateVisitor.class), any())).thenReturn(Boolean.TRUE);
    factory = new QueryPlanFactory(queryService);
    Statement query = new Query(plan, 10);
    AbstractPlan queryExecution =
        factory.create(query, Optional.of(queryListener), Optional.empty());
    assertTrue(queryExecution instanceof QueryPlan);
  }

  @Test
  public void create_query_with_fetch_size_which_cannot_be_paged() {
    when(plan.accept(any(CanPaginateVisitor.class), any())).thenReturn(Boolean.FALSE);
    factory = new QueryPlanFactory(queryService);
    Statement query = new Query(plan, 10);
    assertThrows(UnsupportedCursorRequestException.class,
        () -> factory.create(query,
            Optional.of(queryListener), Optional.empty()));
  }

  @Test
  public void create_close_cursor() {
    factory = new QueryPlanFactory(queryService);
    var plan = factory.createCloseCursor("pewpew", queryListener);
    assertTrue(plan instanceof CommandPlan);
    plan.execute();
    var captor = ArgumentCaptor.forClass(UnresolvedPlan.class);
    verify(queryService).execute(captor.capture(), any());
    assertTrue(captor.getValue() instanceof CloseCursor);
  }
}
