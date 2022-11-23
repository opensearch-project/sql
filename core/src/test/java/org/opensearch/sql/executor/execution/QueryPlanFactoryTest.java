/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.sql.executor.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.executor.execution.QueryPlanFactory.NO_CONSUMER_RESPONSE_LISTENER;

import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.sql.ast.statement.Explain;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryService;

@ExtendWith(MockitoExtension.class)
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
  public void createFromQueryShouldSuccess() {
    Statement query = new Query(plan);
    AbstractPlan queryExecution =
        factory.create(query, Optional.of(queryListener), Optional.empty());
    assertTrue(queryExecution instanceof QueryPlan);
  }

  @Test
  public void createFromExplainShouldSuccess() {
    Statement query = new Explain(new Query(plan));
    AbstractPlan queryExecution =
        factory.create(query, Optional.empty(), Optional.of(explainListener));
    assertTrue(queryExecution instanceof ExplainPlan);
  }

  @Test
  public void createFromQueryWithoutQueryListenerShouldThrowException() {
    Statement query = new Query(plan);

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(query,
            Optional.empty(), Optional.empty()));
    assertEquals("[BUG] query listener must be not null", exception.getMessage());
  }

  @Test
  public void createFromExplainWithoutExplainListenerShouldThrowException() {
    Statement query = new Explain(new Query(plan));

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> factory.create(query,
            Optional.empty(), Optional.empty()));
    assertEquals("[BUG] explain listener must be not null", exception.getMessage());
  }

  @Test
  public void noConsumerResponseChannel() {
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
}
